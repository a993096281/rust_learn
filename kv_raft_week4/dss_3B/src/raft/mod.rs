//use std::sync::Arc;

use futures::sync::mpsc::UnboundedSender;
use futures::Future;
use labcodec;
use labrpc::RpcFuture;

#[cfg(test)]
pub mod config;
pub mod errors;
pub mod persister;
pub mod service;
#[cfg(test)]
mod tests;

use self::errors::*;
use self::persister::*;
use self::service::*;

use rand::Rng;
use std::cmp;
use std::thread;
use std::time::{Duration, Instant};
use std::sync::{Arc, Mutex};
use std::sync::mpsc;
use std::sync::mpsc::Sender;

use self::config::Entry;  //raft测试的输入日志
//use crate::kvraft::server::OpEntry as Entry;  //kv_raft测试的输入日志

#[macro_export]
macro_rules! my_debug {
    ($($arg: tt)*) => (
        println!("Debug[{}:{}]: {}", file!(), line!(),format_args!($($arg)*));
    )
}
const TIMEOUT_LOW_BOUND: u64 = 250;   //follow超时时间下界，单位ms
const TIMEOUT_HIGH_BOUND: u64 = 400;  //follow超时时间上界，单位ms
const HEARTBEAT_TIME: u64 = 100;     //leader心跳时间，单位ms
 
 const MAX_SEND_ENTRIES: u64 = 100; //一次最大可发送的entries

#[derive(Clone, Debug)]
pub struct ApplyMsg {
    pub command_valid: bool,
    pub command: Vec<u8>,
    pub command_index: u64,
    pub is_snapshot: bool,  //是否是snapshot操作，kv_raft才会用到
    pub snapshot: Vec<u8>,  //是snapshot操作时，snapshot的数据
}

/// State of a raft peer.
#[derive(Default, Clone, Debug)]
pub struct State {
    pub term: u64,
    pub is_leader: bool,
    pub is_candidate: bool,
}



impl State {
    /// The current term of this peer.
    fn new() -> Self {
        State {
            term: 0,
            is_leader: false,
            is_candidate: false,
        }
    }
    pub fn term(&self) -> u64 {
        self.term
    }
    /// Whether this peer believes it is the leader.
    pub fn is_leader(&self) -> bool {
        self.is_leader
    }
    pub fn is_candidate(&self) -> bool {
        self.is_candidate
    }
}
#[derive(Clone, PartialEq, Message)]
pub struct LogEntry {
    #[prost(uint64, tag = "1")]
    pub index: u64, //主要做检验，理论上以Vec的下标为主

    #[prost(uint64, tag = "2")]
    pub term: u64,

    #[prost(bytes, tag = "3")]
    pub entry: Vec<u8>,
}

impl LogEntry {
    fn new() -> Self {
        LogEntry {
            index: 0,
            term: 0,
            entry: vec![],
        }
    }

    fn from_data(index: u64, term: u64, src_entry: &Vec<u8>) -> Self {
        LogEntry {
            index,
            term,
            entry: src_entry.clone(),
        }
    }
}

//保持raft的所有状态
#[derive(Clone, PartialEq, Message)]
pub struct RaftState {
    //voted_for 暂时觉得没必要保存
    #[prost(uint64, tag = "1")]
    pub term: u64,

    #[prost(bool, tag = "2")]
    pub is_leader: bool,

    #[prost(bool, tag = "3")]
    pub is_candidate: bool,

    #[prost(uint64, tag = "4")]
    pub commit_index: u64,

    #[prost(uint64, tag = "5")]
    pub last_applied: u64,

    #[prost(uint64, tag = "6")]
    pub snapshot_index: u64,

    #[prost(uint64, tag = "7")]
    pub snapshot_term: u64,

    #[prost(bytes, repeated, tag = "8")]
    pub logs: Vec<Vec<u8>>,
}

// A single Raft peer.
pub struct Raft {
    // RPC end points of all peers
    pub peers: Vec<RaftClient>,
    // Object to hold this peer's persisted state
    pub persister: Box<dyn Persister>,
    // this peer's index into peers[]
    pub me: usize,
    pub state: State,
    pub apply_ch: UnboundedSender<ApplyMsg>,
    pub voted_for: Option<usize>,
    pub log: Vec<LogEntry>,           //日志条目，1开始，并且Vec下标为索引
    pub commit_index: u64,            //已提交的日志条目，增加Option主要是开始为None，值为log下标索引
    pub last_applied: u64, //最后被应用到状态机的日志条目索引值，增加Option主要是开始为None，值为log下标索引
    pub next_index: Option<Vec<u64>>, //对于每一个服务器，需要发送给他的下一个日志条目的索引值
    pub match_index: Option<Vec<u64>>, //对于每一个服务器，已经复制给他的日志的最高索引值

    pub snapshot_index: u64,    //kv_raft用到
    pub snapshot_term: u64,     //kv_raft用到

    // Your data here (2A, 2B, 2C).
    // Look at the paper's Figure 2 for a description of what
    // state a Raft server must maintain.
}

impl Raft {
    /// the service or tester wants to create a Raft server. the ports
    /// of all the Raft servers (including this one) are in peers. this
    /// server's port is peers[me]. all the servers' peers arrays
    /// have the same order. persister is a place for this server to
    /// save its persistent state, and also initially holds the most
    /// recent saved state, if any. apply_ch is a channel on which the
    /// tester or service expects Raft to send ApplyMsg messages.
    /// This method must return quickly.
    pub fn new(
        peers: Vec<RaftClient>,
        me: usize,
        persister: Box<dyn Persister>,
        apply_ch: UnboundedSender<ApplyMsg>,
    ) -> Raft {
        let raft_state = persister.raft_state();

        // Your initialization code here (2A, 2B, 2C).
        let mut rf = Raft {
            peers,
            persister,
            me,
            state: State::new(),
            apply_ch,
            voted_for: None,
            log: vec![LogEntry::new()],
            commit_index: 0,
            last_applied: 0,
            next_index: None,
            match_index: None,
            snapshot_index: 0,
            snapshot_term: 0,
        };

        // initialize from state persisted before a crash
        rf.restore(&raft_state);

        rf
    }
    pub fn term(&self) -> u64 {
        self.state.term()
    }

    pub fn is_leader(&self) -> bool {
        self.state.is_leader()
    }
    pub fn is_candidate(&self) -> bool {
        self.state.is_candidate()
    }

    pub fn set_state(&mut self, term: u64, is_leader: bool, is_candidate: bool) {
        let state = State {
            term,
            is_leader,
            is_candidate,
        };
        self.state = state;
        my_debug!(
            "set id:{} term:{} islead:{} iscondi:{}",
            self.me,
            self.state.term(),
            self.state.is_leader(),
            self.state.is_candidate()
        );
        if self.state.is_leader() {       //leader
            self.next_index = Some(vec![1; self.peers.len()]);
            self.match_index = Some(vec![0; self.peers.len()]);
        } else if self.is_candidate() {   //candidate
            self.next_index = None;
            self.match_index = None;
            self.voted_for = Some(self.me);
        } else {                          //follower
            self.next_index = None;
            self.match_index = None;
            self.voted_for = None;
        }
        self.persist();
    }
    pub fn get_last_log_index_and_term(&self) -> (u64, u64) {
        let index = self.log.len() - 1;
        let term = self.log[index].term;
        (index as u64, term)
    }

    pub fn get_peers_num(&self) -> usize {
        self.peers.len()
    }

    pub fn get_log(&self, index: u64) -> Option<LogEntry> {  //index是日志的index
        let index = index - self.snapshot_index;
        if ((self.log.len() - 1) as u64) < index {   //log的index不够
            None
        } else {
            Some(self.log[index as usize].clone())
        }
    }

    pub fn handle_append_reply(&mut self, id: usize, result: i32, for_next_index: u64) {
        //1:sucess 0:fail -1:error
        if !self.is_leader() || self.match_index == None || self.next_index == None {
            return;
        }
        my_debug!("leader:{} handle_append_reply id:{} result:{} for_next_index:{}",
            self.me, id, result, for_next_index);
        if for_next_index < 1 || for_next_index > self.log.len() as u64 {
            return;
        }
        let mut match_index = self.match_index.clone().unwrap();
        let mut next_index = self.next_index.clone().unwrap();
        let _old_match = match_index[id];
        let _old_next = next_index[id];
        //my_debug!("id:{} before match_index:{:?}", self.me, match_index);
        //my_debug!("id:{} before next_index:{:?}", self.me, next_index);
        if result == 1 {
            //收到success
            if for_next_index <= self.log.len() as u64 {
                match_index[id] = for_next_index - 1;
                next_index[id] = for_next_index;
            }
            else {
                my_debug!("error:leader:{} handle_append_reply id:{} for_next_index:{} log:{}",
                    self.me, id, for_next_index, self.log.len());
                return;
            }
        } else if result == 0 {
            //收到fail
            if next_index[id] < (MAX_SEND_ENTRIES + 1) {
                next_index[id] = 1;
            } else {
                next_index[id] -= MAX_SEND_ENTRIES;
            }
        }
        //收到error，不变
        self.next_index = Some(next_index.clone());
        self.match_index = Some(match_index.clone());
        my_debug!("id:{} handle_append_reply id:{} next_index:[{}->{}] match_index:[{}->{}]", 
            self.me, id, _old_next, next_index[id], _old_match, match_index[id]);
        //my_debug!("id:{} after match_index:{:?}", self.me, match_index);
        //my_debug!("id:{} after next_index:{:?}", self.me, next_index);
        if result != 1 && _old_match == match_index[id] {   //不成功，match_index没有更改，无需更新commit
            return;
        }
        //查看是否更新commit
        let mut new_commit_index: u64 = 0;
        for index in ((self.commit_index + 1)..(match_index[id] + 1)).rev() {
            //rev()逆序，从大到小开始检测
            let mut pass: usize = 0;
            for i in 0..self.get_peers_num() {
                if i == self.me {
                    continue;
                }
                if match_index[i] >= index {
                    pass += 1;
                }
            }
            if (pass + 1) > self.get_peers_num() / 2 {
                //说明通过超过半数，可commit
                new_commit_index = index;
                break;
            }
        }
        if new_commit_index != 0 {
            let log = self.get_log(new_commit_index).unwrap();
            if log.term != self.term() {
                return;
            }
            //my_debug!("id:{} new_commit_index:{:?}", self.me, new_commit_index);
            self.set_commit_index(new_commit_index);
        }
    }

    pub fn set_commit_index(&mut self, new_commit_index: u64) {
        if new_commit_index < self.commit_index {
            my_debug!(
                "error:id:{} set_commit_index fail:[{}-{}]",
                self.me,
                self.commit_index,
                new_commit_index
            );
            return;
        }
        my_debug!(
            "id:{} set commit_index:[{}->{}]",
            self.me,
            self.commit_index,
            new_commit_index
        );
        self.commit_index = new_commit_index;
        if self.commit_index > self.last_applied {
            //更新状态机
            let last = self.last_applied;
            for i in last..self.commit_index {
                self.last_applied += 1; //并发送
                let mesg = ApplyMsg {
                    command_valid: true,
                    command: self.log[self.last_applied as usize].entry.clone(),
                    command_index: self.last_applied,
                    is_snapshot: false,
                    snapshot: vec![],
                };
                let _ret = self.apply_ch.unbounded_send(mesg);
                my_debug!("id:{} apply_ch:[{}]", self.me, self.last_applied);
                self.persist();
            }
        }
    }

    pub fn push_log(&mut self, index: u64, term: u64, entry: &Vec<u8>) {
        if self.log.len() as u64 != index {
            my_debug!(
                "error:id:{} push index:{} error log:[{}:{}]",
                self.me,
                self.log.len(),
                index,
                term
            );
            return;
        }
        self.log.push(LogEntry::from_data(index, term, entry));
        self.persist();
        let mut _entr: Option<Entry> = None;
        match labcodec::decode(entry) {
            Ok(en) => {
                _entr = Some(en);
            }
            Err(e) => {}
        }
        my_debug!(
            "id:{} push log:[{}:{}:{:?}] ",
            self.me,
            index,
            term,
            _entr.unwrap()
        );
    }

    pub fn delete_log(&mut self, save_index: u64) {  //save_index是日志的index
        //删除save_index后面的log，save_index不删除
        let save_index = save_index - self.snapshot_index;  //vector数组的index
        if ((self.log.len() - 1) as u64) < save_index {
            return;
        }
        let _delete: Vec<LogEntry> = self.log.drain((save_index as usize + 1)..).collect();
        self.persist();
        for de in &_delete {
            let entry = de.entry.clone();
            let mut _entr: Option<Entry> = None;
            match labcodec::decode(&entry) {
                Ok(en) => {
                    _entr = Some(en);
                }
                Err(e) => {}
            }
            my_debug!(
                "id:{} delete log:[{}:{}:{:?}]",
                self.me,
                de.index,
                de.term,
                _entr
            );
        }
    }

    pub fn delete_prev_log(&mut self, save_index: u64) {  //save_index是日志的index
        //删除save_index前面的log，save_index不删除
        let save_index = save_index - self.snapshot_index;  //vector数组的index
        if ((self.log.len() - 1) as u64) < save_index {  //log不够，
            my_debug!("error:id:{} delete prev log save_index:{} snapshot_index:{} log:{}",
                self.me, save_index, self.snapshot_index, self.log.len());
            return;
        }
        let _delete: Vec<LogEntry> = self.log.drain(..(save_index as usize)).collect();
        for de in &_delete {
            let entry = de.entry.clone();
            let mut _entr: Option<Entry> = None;
            match labcodec::decode(&entry) {
                Ok(en) => {
                    _entr = Some(en);
                }
                Err(e) => {}
            }
            my_debug!(
                "id:{} delete prev log:[{}:{}:{:?}]",
                self.me,
                de.index,
                de.term,
                _entr
            );
        }
    }

    pub fn check_and_do_compress_log(&mut self, maxraftstate: usize, index: u64) {
        if maxraftstate > self.persister.raft_state().len() {  //未超过，无需压缩日志
            return;
        }
        if index > self.commit_index || index <= self.snapshot_index {
            my_debug!("error:id:{} compress_log error index:{} commit:{} snapshot:{}",
                self.me, self.commit_index, self.snapshot_index);
            return;
        }
        self.delete_prev_log(index);  //删除index之前的日志
        self.snapshot_index = index;
        self.snapshot_term = self.log[0].term;
        self.persist();  //无需保存snapshot，因为kv_server前面保存了

    }

    /// save Raft's persistent state to stable storage,
    /// where it can later be retrieved after a crash and restart.
    /// see paper's Figure 2 for a description of what should be persistent.
    fn persist(&mut self) {
        // Your code here (2C).
        // Example:
        // labcodec::encode(&self.xxx, &mut data).unwrap();
        // labcodec::encode(&self.yyy, &mut data).unwrap();
        // self.persister.save_raft_state(data);
        let mut data = vec![];
        let mut raft_state = RaftState {
            term: self.term(),
            is_leader: self.is_leader(),
            is_candidate: self.is_candidate(),
            commit_index: self.commit_index,
            last_applied: self.last_applied,
            snapshot_index: self.snapshot_index,
            snapshot_term: self.snapshot_term,
            logs: vec![],
        };
        for i in 1..self.log.len() {
            //从1开始，不保存index为0的空log
            let mut dat = vec![];
            let log = self.log[i].clone();
            let _ret = labcodec::encode(&log, &mut dat).map_err(Error::Encode);
            raft_state.logs.push(dat);
        }
        let _ret = labcodec::encode(&raft_state, &mut data).map_err(Error::Encode);
        self.persister.save_raft_state(data);
    }
    pub fn save_state_and_snapshot(&self, data2: Vec<u8>) {
        let mut data = vec![];
        let mut raft_state = RaftState {
            term: self.term(),
            is_leader: self.is_leader(),
            is_candidate: self.is_candidate(),
            commit_index: self.commit_index,
            last_applied: self.last_applied,
            snapshot_index: self.snapshot_index,
            snapshot_term: self.snapshot_term,
            logs: vec![],
        };
        for i in 1..self.log.len() {
            //从1开始，不保存index为0的空log
            let mut dat = vec![];
            let log = self.log[i].clone();
            let _ret = labcodec::encode(&log, &mut dat).map_err(Error::Encode);
            raft_state.logs.push(dat);
        }
        let _ret = labcodec::encode(&raft_state, &mut data).map_err(Error::Encode);
        self.persister.save_state_and_snapshot(data, data2);
        //my_debug!("id:{} save_state_and_snapshot",self.me);
    }

    /// restore previously persisted state.
    fn restore(&mut self, data: &[u8]) {
        if data.is_empty() {
            // bootstrap without any state?
            return;
        }
        // Your code here (2C).
        // Example:
        // match labcodec::decode(data) {
        //     Ok(o) => {
        //         self.xxx = o.xxx;
        //         self.yyy = o.yyy;
        //     }
        //     Err(e) => {
        //         panic!("{:?}", e);
        //     }
        // }
        match labcodec::decode(data) {
            Ok(state) => {
                let state: RaftState = state;
                let restate = State {
                    term: state.term,
                    is_leader: false,
                    is_candidate: false,
                };

                self.state = restate;
                self.commit_index = state.commit_index;
                self.last_applied = state.last_applied;
                self.snapshot_index = state.snapshot_index;
                self.snapshot_term = state.snapshot_term;

                my_debug!("id:{} restore state:{:?} num:{}", self.me, state, state.logs.len());
                for i in 0..state.logs.len() {
                    let log_encode = state.logs[i].clone();
                    match labcodec::decode(&log_encode) {
                        Ok(log) => {
                            let log: LogEntry = log;
                            self.log.push(log.clone());
                            my_debug!("id:{} restore log :{:?}", self.me, log);
                        }
                        Err(e) => {
                            panic!("{:?}", e);
                        }
                    }
                }
                //my_debug!("id:{} restore set state:{:?}", self.me, self.state);
            }
            Err(e) => {
                panic!("{:?}", e);
            }
        }
    }

    /// example code to send a RequestVote RPC to a server.
    /// server is the index of the target server in peers.
    /// expects RPC arguments in args.
    ///
    /// The labrpc package simulates a lossy network, in which servers
    /// may be unreachable, and in which requests and replies may be lost.
    /// This method sends a request and waits for a reply. If a reply arrives
    /// within a timeout interval, This method returns Ok(_); otherwise
    /// this method returns Err(_). Thus this method may not return for a while.
    /// An Err(_) return can be caused by a dead server, a live server that
    /// can't be reached, a lost request, or a lost reply.
    ///
    /// This method is guaranteed to return (perhaps after a delay) *except* if
    /// the handler function on the server side does not return.  Thus there
    /// is no need to implement your own timeouts around this method.
    ///
    /// look at the comments in ../labrpc/src/mod.rs for more details.
    fn send_request_vote(&self, server: usize, args: &RequestVoteArgs) -> Result<RequestVoteReply> {
        let peer = &self.peers[server];
        // Your code here if you want the rpc becomes async.
        // Example:
        // ```
        // let (tx, rx) = channel();
        // peer.spawn(
        //     peer.request_vote(&args)
        //         .map_err(Error::Rpc)
        //         .then(move |res| {
        //             tx.send(res);
        //             Ok(())
        //         }),
        // );
        // rx.wait() ...
        // ```
        peer.request_vote(&args).map_err(Error::Rpc).wait()
    }

    fn start<M>(&mut self, command: &M) -> Result<(u64, u64)>
    where
        M: labcodec::Message,
    {
        if self.is_leader() {
            let index = self.log.len() as u64;
            let term = self.term();
            let mut buf = vec![];
            labcodec::encode(command, &mut buf).map_err(Error::Encode)?;
            self.push_log(index, term, &buf);
            Ok((index, term))
        } else {
            Err(Error::NotLeader)
        }
    }
}

// Choose concurrency paradigm.
//
// You can either drive the raft state machine by the rpc framework,
//
// ```rust
// struct Node { raft: Arc<Mutex<Raft>> }
// ```
//
// or spawn a new thread runs the raft state machine and communicate via
// a channel.
//
// ```rust
// struct Node { sender: Sender<Msg> }
// ```
#[derive(Clone)]
pub struct Node {
    // Your code here.
    pub me: usize,
    pub raft: Arc<Mutex<Raft>>,
    pub timeout_thread: Arc<Mutex<Option<thread::JoinHandle<()>>>>, //超时线程
    pub time_reset: Arc<Mutex<Option<Sender<i32>>>>,   //超时重置信号
    pub heartbeat_thread: Arc<Mutex<Option<thread::JoinHandle<()>>>>, //心跳线程
    pub heartbeat_reset: Arc<Mutex<Option<Sender<i32>>>>,  //心跳重置信号
    pub shutdown: Arc<Mutex<bool>>,     //关闭信号
}

impl Node {
    /// Create a new raft service.
    pub fn new(raft: Raft) -> Node {
        // Your code here.
        let inode = Node {
            me: raft.me,
            raft: Arc::new(Mutex::new(raft)),
            timeout_thread: Arc::new(Mutex::new(None)),
            time_reset: Arc::new(Mutex::new(None)),
            heartbeat_thread: Arc::new(Mutex::new(None)),
            heartbeat_reset: Arc::new(Mutex::new(None)),
            shutdown: Arc::new(Mutex::new(false)),
        };
        Node::creat_timeout_thread(inode.clone());
        Node::creat_heartbeat_thread(inode.clone());
        my_debug!("New inode:{}", inode.me);
        inode
        
    }
    pub fn creat_timeout_thread(inode: Node) {
        let (time_reset, recv) = mpsc::channel();
        let iinode = inode.clone();
        let tthread = thread::spawn(move || {
            loop {
                let rand_time = Duration::from_millis(
                    rand::thread_rng().gen_range(TIMEOUT_LOW_BOUND, TIMEOUT_HIGH_BOUND),
                );
                if let Ok(_re) = recv.recv_timeout(rand_time) {  //说明接收到重置信号
                    if *iinode.shutdown.lock().unwrap() == true {  //关闭
                        my_debug!("id:{} shutdown timeout_thread ", iinode.me);
                        break;
                    }
                    continue;
                }
                else {  //超时
                    if *iinode.shutdown.lock().unwrap() == true {  //关闭
                        my_debug!("id:{} shutdown timeout_thread ", iinode.me);
                        break;
                    }
                    if iinode.is_leader() {  //leader，无需操作
                        continue;
                    }
                    //需要进行投票程序
                    Node::do_vote(iinode.clone());
                }
            }

        });
        *inode.timeout_thread.lock().unwrap() = Some(tthread);
        *inode.time_reset.lock().unwrap() = Some(time_reset);
        
    }
    pub fn do_vote(inode: Node) {  //进行投票处理
        let mut raft = inode.raft.lock().unwrap();   //锁住
        if raft.is_leader() {
            return;
        }
        let mut term = raft.term();
        term += 1;
        raft.set_state(term, false, true);
        let id = raft.me;
        let (last_log_index, last_log_term) = raft.get_last_log_index_and_term();
        let args = RequestVoteArgs {
            term,
            candidate_id: id as u64,
            last_log_index,
            last_log_term,
        };
        my_debug!(
            "candidate:{} term:{} commit:{} args:{:?}",
            raft.me,
            raft.term(),
            raft.commit_index,
            args
        );

        let passed = Arc::new(Mutex::new(1)); //先给自己投一票
        let nums = raft.get_peers_num();
        let peers = raft.peers.clone();

        for i in 0..nums {
            if i == id {
                continue;
            }
            let time = Instant::now();
            let me = id;
            let nums = nums as u64;
            let iinode = inode.clone();
            let passed = Arc::clone(&passed);
            let args = args.clone();
            let peer = peers[i].clone();
            let term = term;
            //let tsend = mpsc::Sender::clone(&send);
            thread::spawn(move || {
                let ret = peer.request_vote(&args).map_err(Error::Rpc).wait();
                match ret {
                    Ok(rep) => {
                        let iiinode = iinode.clone();
                        let mut raft = iinode.raft.lock().unwrap();  //锁住
                        if rep.vote_granted {
                            *passed.lock().unwrap() += 1;
                            let time2 = time.elapsed().as_millis();
                            my_debug!("candidate:{} for vote:{} passed time:{}!", me, i, time2);
                            
                            if *passed.lock().unwrap() > nums / 2 && raft.is_candidate() && term == raft.term()
                            {  //投票成功数目超过半数，并仍是候选者状态,
                                let _ret = iinode.heartbeat_reset.lock().unwrap().clone().unwrap().send(1);  //重新心跳计时
                                raft.set_state(term, true, false); //成为leared
                                my_debug!("{} become leader!", raft.me);
                                thread::spawn(move || {  //用线程去广播，可释放锁
                                    Node::do_heartbeat(iiinode);  //广播心跳
                                });
                            }
                        } else if rep.term > raft.term() {  //遇到term更大节点
                            let _ret = iinode.time_reset.lock().unwrap().clone().unwrap().send(1);
                            raft.set_state(rep.term, false, false);  //变成follower
                            let time2 = time.elapsed().as_millis();
                            my_debug!("candidate:{} for vote:{} fail time:{}ms find have_better_leader because term!", me, i, time2);
                        }
                    }
                    Err(_) => {
                        //let time2 = time.elapsed().as_millis();
                        //my_debug!("candidate:{} for vote:{} error time:{}ms!", me, i, time2);
                    }
                }
            });
        }

    }

    pub fn creat_heartbeat_thread(inode: Node) {
        let (heartbeat_reset, recv) = mpsc::channel();
        let iinode = inode.clone();
        let tthread = thread::spawn(move || {
            loop {
                if let Ok(_re) = recv.recv_timeout(Duration::from_millis(HEARTBEAT_TIME)) {  //说明接收到重置信号
                    if *iinode.shutdown.lock().unwrap() == true {  //关闭
                        my_debug!("id:{} shutdown heartbeat_thread ", iinode.me);
                        break;
                    }
                    continue;
                }
                else {  //超时
                    if *iinode.shutdown.lock().unwrap() == true {  //关闭
                        my_debug!("id:{} shutdown heartbeat_thread ", iinode.me);
                        break;
                    }
                    if !iinode.is_leader() {  //不是leader，无需操作
                        continue;
                    }
                    //需要进行心跳
                    Node::do_heartbeat(iinode.clone());
                }
            }

        });
        *inode.heartbeat_thread.lock().unwrap() = Some(tthread);
        *inode.heartbeat_reset.lock().unwrap() = Some(heartbeat_reset);
    }
    
    pub fn do_heartbeat(inode: Node) {
        let raft = inode.raft.lock().unwrap();   //锁住
        if !raft.is_leader() {  //不是lear
            return;
        }
        let id = raft.me;
        let nums = raft.get_peers_num();
        let next_index = match raft.next_index.clone() {
            Some(index) => index.clone(),
            None => {
                return;
            }
        };
        my_debug!("leader:{} heart beat! term:{}", id, raft.term());
        my_debug!("leader:{} next_index:{:?}", id, next_index);
        let peers = raft.peers.clone();
        for i in 0..nums {
            if i == id {
                continue;
            }
            let me = id;
            let iinode = inode.clone();
            let peer = peers[i].clone();
            let prev_log_index = next_index[i] - 1;
            let time = Instant::now();

            if prev_log_index < raft.snapshot_index {  //需要发送snapshot,而不是append_entries
                let args = SnapshotArgs {
                    term: raft.term(),
                    leader_id: id as u64,
                    last_included_index: ratf.snapshot_index,
                    last_included_term: raft.snapshot_term,
                    snapshot: raft.persister.snapshot(),
                };
                thread::spawn(move || {
                    my_debug!("leader:{} do snapshot:{} args:[term:{} snapshot_index:{} snapshot_term:{}] ", 
                        me, i, args.term, args.last_included_index, args.last_included_term);
                    let ret = peer.install_snapshot(&args).map_err(Error::Rpc).wait();
                    match ret {
                        Ok(rep) => {
                            let mut raft = iinode.raft.lock().unwrap();
                            if rep.term > raft.term() { //发现存在比自己任期大的节点
                                my_debug!("leader:[{}:{}] set_follow because id:{} bigger term:{}",
                                    me, raft.term(), i, rep.term);
                                let _ret = iinode.time_reset.lock().unwrap().clone().unwrap().send(1);
                                raft.set_state(rep.term, false, false);
                            }
                            if raft.is_leader() {  //还是leader，更新next_index,match_index
                                raft.next_index.unwrap()[i] = args.last_included_index + 1;
                                raft.match_index.unwrap()[i] = args.last_included_index;
                                let time2 = time.elapsed().as_millis();
                                my_debug!("leader:{} do snapshot:{} success time:{}ms! ", me, i, time2);
                            }
                        },
                        Err(_) => {},
                });
                continue;
            }

            let mut args = RequestEntryArgs {
                term: raft.term(),
                leader_id: id as u64,
                prev_log_index: next_index[i] - 1,
                prev_log_term: 0,
                entries: vec![], //entries是prev_log_index的下一个
                leader_commit: raft.commit_index,
            };
            let entry = raft.get_log(args.prev_log_index); //获取log
            if entry.is_none() {
                my_debug!("error:leader:{} prev_log_index:{} no log - log_len:{}", id, args.prev_log_index, raft.log.len());
                return;
            }
            args.prev_log_term = entry.unwrap().term;
            for j in 0..MAX_SEND_ENTRIES {  //一次发送至多MAX_SEND_ENTRIES个log
                let entry_next = raft.get_log(next_index[i] + j);
                match entry_next {
                    Some(en) => {
                        let mut dat = vec![];
                        let _ret = labcodec::encode(&en, &mut dat).map_err(Error::Encode);
                        args.entries.push(dat);
                    }
                    None => {
                        break;
                    }
                }
            }
            thread::spawn(move || {
                my_debug!("leader:{} do heart beat:{} args:[term:{} prev_index:{} prev_term:{} commit:{} entry_num:{}] ", 
                me, i, args.term, args.prev_log_index, args.prev_log_term, args.leader_commit, args.entries.len());
                let ret = peer.append_entries(&args).map_err(Error::Rpc).wait();
                match ret {
                    Ok(rep) => {
                        let mut raft = iinode.raft.lock().unwrap();
                        if rep.success {
                            if rep.term == raft.term() {
                                raft.handle_append_reply(i , 1, rep.next_index);
                            }
                            let time2 = time.elapsed().as_millis();
                            my_debug!("leader:{} do heart beat:{} success time:{}ms! ", me, i, time2);
                        } else {
                            if rep.term > raft.term() {//发现存在比自己任期大的节点
                                my_debug!(
                                    "leader:[{}:{}] set_follow because id:{} bigger term:{}",
                                    me,
                                    raft.term(),
                                    i,
                                    rep.term
                                );
                                let _ret = iinode.time_reset.lock().unwrap().clone().unwrap().send(1);
                                raft.set_state(rep.term, false, false);
                            } else {
                                if rep.term == raft.term() {
                                    raft.handle_append_reply(i, 0, rep.next_index);
                                }
                            }
                            let time2 = time.elapsed().as_millis();
                            my_debug!(
                                "leader:{} do heart beat:{} failed time:{}ms!", me, i, time2);
                        }
                    }
                    Err(_) => {
                        //let time2 = time.elapsed().as_millis();
                        //my_debug!("leader:{} do heart beat:{} error time:{}ms! ", me, i, time2);
                    }
                }
            });
        }

    }

    pub fn save_state_and_snapshot(&self, data: Vec<u8>) {
        self.raft.lock().unwrap().save_state_and_snapshot(data);
    }

    pub fn check_and_do_compress_log(&self, maxraftstate: usize, index: u64) {
        self.raft.lock().unwrap().check_and_do_compress_log(maxraftstate, index);
    }

    /// the service using Raft (e.g. a k/v server) wants to start
    /// agreement on the next command to be appended to Raft's log. if this
    /// server isn't the leader, returns false. otherwise start the
    /// agreement and return immediately. there is no guarantee that this
    /// command will ever be committed to the Raft log, since the leader
    /// may fail or lose an election. even if the Raft instance has been killed,
    /// this function should return gracefully.
    ///
    /// the first return value is the index that the command will appear at
    /// if it's ever committed. the second return value is the current
    /// term. the third return value is true if this server believes it is
    /// the leader.
    /// This method must return quickly.
    pub fn start<M>(&self, command: &M) -> Result<(u64, u64)>
    where
        M: labcodec::Message,
    {
        // Your code here.
        // Example:
        // self.raft.start(command)
        //unimplemented!()
        if self.is_leader() {
            self.raft.lock().unwrap().start(command)
        } else {
            Err(Error::NotLeader)
        }
    }

    /// The current term of this peer.
    pub fn term(&self) -> u64 {
        // Your code here.
        // Example:
        // self.raft.term
        //unimplemented!()
        self.raft.lock().unwrap().term()
    }

    /// Whether this peer believes it is the leader.
    pub fn is_leader(&self) -> bool {
        // Your code here.
        // Example:
        // self.raft.leader_id == self.id
        //unimplemented!()
        self.raft.lock().unwrap().is_leader()
    }
    pub fn is_candidate(&self) -> bool {
        self.raft.lock().unwrap().is_candidate()
    }

    /// The current state of this peer.
    pub fn get_state(&self) -> State {
        State {
            term: self.term(),
            is_leader: self.is_leader(),
            is_candidate: self.is_candidate(),
        }
    }

    /// the tester calls kill() when a Raft instance won't be
    /// needed again. you are not required to do anything in
    /// kill(), but it might be convenient to (for example)
    /// turn off debug output from this instance.
    /// In Raft paper, a server crash is a PHYSICAL crash,
    /// A.K.A all resources are reset. But we are simulating
    /// a VIRTUAL crash in tester, so take care of background
    /// threads you generated with this Raft Node.
    pub fn kill(&self) {
        // Your code here, if desired.
        *self.shutdown.lock().unwrap() = true;
        match *self.time_reset.lock().unwrap() {
            Some(ref send) => {
                let _ret = send.send(1);
            },
            None => {}
        }
        match *self.heartbeat_reset.lock().unwrap() {
            Some(ref send) => {
                let _ret = send.send(1);
            },
            None => {}
        }
        let timeout_thread = self.timeout_thread.lock().unwrap().take();
        if timeout_thread.is_some() {
            let _ = timeout_thread.unwrap().join();
        }
        let heartbeat_thread = self.heartbeat_thread.lock().unwrap().take();
        if heartbeat_thread.is_some() {
            let _ = heartbeat_thread.unwrap().join();
        }

    }
}

impl RaftService for Node {
    // example RequestVote RPC handler.
    fn request_vote(&self, args: RequestVoteArgs) -> RpcFuture<RequestVoteReply> {
        // Your code here (2A, 2B).
        //unimplemented!()
        let mut raft = self.raft.lock().unwrap();
        let mut reply = RequestVoteReply {
            term: raft.term(),
            vote_granted: false,
        };
        if args.term < raft.term() {  //term更小
            reply.vote_granted = false;
        } else {
            if args.term > raft.term() {   //遇到term更大，则变为follower
                let _ret = self.time_reset.lock().unwrap().clone().unwrap().send(1);
                raft.set_state(args.term, false, false);
            }
            if raft.voted_for.is_none() {   //未投票
                let (last_log_index, last_log_term) = raft.get_last_log_index_and_term();
                if (args.last_log_term == last_log_term && args.last_log_index >= last_log_index) ||
				    args.last_log_term > last_log_term {  //如果日志更加新
                        reply.vote_granted = true;
                        raft.voted_for = Some(args.candidate_id as usize); 
                }
            }
        }
        Box::new(futures::future::result(Ok(reply)))
    }

    fn append_entries(&self, args: RequestEntryArgs) -> RpcFuture<RequestEntryReply> {
        let mut raft = self.raft.lock().unwrap();
        let mut reply = RequestEntryReply {
            term: raft.term(),
            success: false,
            next_index: 1,
        };
        //my_debug!("id:{} append_entries begin", raft.me);
        if args.term < raft.term() {
            my_debug!("warn:me[{}:{}] recv [{}:{}]", raft.me, raft.term(), args.leader_id, args.term);
            return Box::new(futures::future::result(Ok(reply)));
        } 
        if args.term == raft.term() && raft.is_leader() {  //遇到相同term的leader,???
            my_debug!("error:leader[{}:{}] recv leader [{}:{}]", raft.me, raft.term(), args.leader_id, args.term);
            return Box::new(futures::future::result(Ok(reply)));
        }
        //进行到这里，说明rpc有效
        let _ret = self.time_reset.lock().unwrap().clone().unwrap().send(1);  //先重置超时，防止处理时间过长
        if args.term == raft.term() && raft.is_candidate() {  //candidate遇到leader
            raft.set_state(args.term, false, false);
        }
        if args.term > raft.term() {  //遇到term更大，变成follower，无论是candidate还是leader，都变为follower
            raft.set_state(args.term, false, false);
        }
        reply.term = raft.term();
        if let Some(entry) = raft.get_log(args.prev_log_index) {  //
            if entry.term == args.prev_log_term {  //日志匹配
                if args.entries.len() != 0 {  //有附加的rpc
                    for i in 0..args.entries.len() {  //对entries进行处理
                        let log_encode = args.entries[i].clone();
                        match labcodec::decode(&log_encode) {
                            Ok(log) => {
                                let log: LogEntry = log;
                                let node_entry = raft.get_log(args.prev_log_index + 1 + i as u64);
                                match node_entry {
                                    Some(n_en) => {
                                        if n_en == log {  //log 相等，无需删除
                                            continue;
                                        } else {
                                            raft.delete_log(args.prev_log_index + i as u64);
                                            raft.push_log(log.index, log.term, &log.entry);
                                        }
                                    }
                                    None => {
                                        raft.push_log(log.index, log.term, &log.entry);
                                    }
                                }
                            }
                            Err(e) => {
                                panic!("{:?}", e);
                            }
                        }
                    }
                }
                reply.success = true;
                reply.next_index = args.prev_log_index + 1 + args.entries.len() as u64;
                let can_commit = cmp::min(args.leader_commit, reply.next_index - 1);
                if can_commit > raft.commit_index {
                    //返回success,可提交commit
                    let (last_log_index, _) = raft.get_last_log_index_and_term();
                    let new_commit_index: u64 = cmp::min(can_commit, last_log_index);
                    raft.set_commit_index(new_commit_index);
                }
                let _ret = self.time_reset.lock().unwrap().clone().unwrap().send(1);  //再次重置
                //my_debug!("id:{} append_entries end", raft.me);
                return Box::new(futures::future::result(Ok(reply)));
            }
            else {  //日志不匹配
                return Box::new(futures::future::result(Ok(reply)));
            }
        }
        else {  //日志不匹配
            return Box::new(futures::future::result(Ok(reply)));
        }
    }

    fn install_snapshot(&self, args: SnapshotArgs) -> RpcFuture<SnapshotReply> {
        let mut raft = self.raft.lock().unwrap();
        let mut reply = SnapshotReply {
            term: raft.term(),
        };
        if args.term < raft.term() {
            my_debug!("warn:me[{}:{}] recv term [{}:{}]", raft.me, raft.term(), args.leader_id, args.term);
            return Box::new(futures::future::result(Ok(reply)));
        }
        if args.last_included_index <= raft.snapshot_index {
		    my_debug!("warn:me[{}:{}] recv snapshot [{}:{}]", raft.me, raft.snapshot_index, args.leader_id, args.last_included_index);
		    return Box::new(futures::future::result(Ok(reply)));
	    }
        let _ret = self.time_reset.lock().unwrap().clone().unwrap().send(1);
        if args.term == raft.term() && raft.is_candidate() {  //candidate遇到leader
            raft.set_state(args.term, false, false);
        }
        if args.term > raft.term() {  //遇到term更大，变成follower，无论是candidate还是leader，都变为follower
            raft.set_state(args.term, false, false);
        }
        reply.term = raft.term();
        
        if args.last_included_index > (raft.snapshot_index + raft.log.len() - 1) { //所有log都要替换
            let log = LogEntry {
                index: args.last_included_index,
                term: args.last_included_term,
                entry: vec![],
            }
            raft.log = vec![log];
        }
        else {
            raft.delete_prev_log(args.last_included_index);
        }
        raft.snapshot_index = args.last_included_index;
        raft.snapshot_term = args.snapshot_term;
        raft.commit_index = args.last_included_index;
        raft.last_applied = args.last_included_index;

        raft.save_state_and_snapshot(args.snapshot.clone());  //保存state和snapshot

        let mesg = ApplyMsg {
            command_valid: true,
            command: vec![],
            command_index: raft.snapshot_index,
            is_snapshot: true,
            snapshot: args.snapshot.clone(),
        };
        let _ret = raft.apply_ch.unbounded_send(mesg);  //发送snapshot给kv_server
        my_debug!("id:{} apply snapshot:{}", raft.me, raft.snapshot_index);

        Box::new(futures::future::result(Ok(reply)))
    }
}
