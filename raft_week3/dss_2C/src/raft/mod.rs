extern crate rand;

use std::sync::{Arc, Mutex};

use std::sync::mpsc;
use std::sync::mpsc::Sender;
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
//use std::sync::atomic::{AtomicBool, AtomicI32, Ordering};
use std::thread;
use std::time::{Instant, Duration};
use rand::Rng;
use std::cmp;
use self::config::Entry;

#[macro_export]
macro_rules! my_debug {
    ($($arg: tt)*) => (
        println!("Debug[{}:{}]: {}", file!(), line!(),format_args!($($arg)*));
    )
}

const TIMEOUT_LOW_BOUND: u64 = 200;
const TIMEOUT_HIGH_BOUND: u64 = 350;
const HEART_BEAT_LOW_BOUND: u64 = 95;
const HEART_BEAT_HIGH_BOUND: u64 = 100;


//const BROADCAST_ERROR_WAIT_TIME: u64 = 10; //广播时接收reply，每个接收最多等待time ms，意味着如果有两个节点error，投票或者心跳接收会等待2*time ms，
const MAX_SEND_ENTRIES: u64 = 200;  //一次最大可发送的entries

pub struct ApplyMsg {
    pub command_valid: bool,
    pub command: Vec<u8>,
    pub command_index: u64,
}

/// State of a raft peer.
#[derive(Clone, PartialEq, Message)]
pub struct State {
    #[prost(uint64, tag="1")]
    pub term: u64,

    #[prost(bool, tag="2")]
    pub is_leader: bool,

    #[prost(bool, tag="3")]
    pub is_candidate: bool,
}

impl State {
    /// The current term of this peer.
    fn new() -> Self{
        State{
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
pub struct LogEntry{
    #[prost(uint64, tag="1")]
    pub index: u64,   //主要做检验，理论上以Vec的下标为主

    #[prost(uint64, tag="2")]
    pub term: u64,

    #[prost(bytes, tag="3")]
    pub entry: Vec<u8>,
}

impl LogEntry{
    fn new() -> Self{
        LogEntry{
            index: 0,
            term: 0,
            entry: vec![],
        }
    }

    fn from_data(index: u64, term: u64, src_entry: &Vec<u8>) -> Self{
        LogEntry{
            index,
            term,
            entry: src_entry.clone(),
        }
    }
}
//保持所有状态
#[derive(Clone, PartialEq, Message)]
pub struct RaftState {   //voted_for暂时觉得没必要保存
    #[prost(uint64, tag="1")]
    pub term: u64,

    #[prost(bool, tag="2")]
    pub is_leader: bool,

    #[prost(bool, tag="3")]
    pub is_candidate: bool,

    #[prost(uint64, tag="4")]
    pub commit_index: u64,

    #[prost(uint64, tag="5")]
    pub last_applied: u64,

    #[prost(bytes, repeated, tag="6")]
    pub logs: Vec<Vec<u8>>,
}
impl RaftState {
    fn new() -> Self{
        RaftState{
            term: 0,
            is_leader: false,
            is_candidate: false,
            commit_index: 0,
            last_applied: 0,
            logs: vec![],
        }
    }
}

// A single Raft peer.
pub struct Raft {
    // RPC end points of all peers
    peers: Vec<RaftClient>,
    // Object to hold this peer's persisted state
    persister: Box<dyn Persister>,
    // this peer's index into peers[]
    me: u64,
    state: State,
    apply_ch: UnboundedSender<ApplyMsg>,
    voted_for: Option<u64>,
    log: Vec<LogEntry>,         //日志条目，1开始，并且Vec下标为索引
    commit_index: u64,  //已提交的日志条目，增加Option主要是开始为None，值为log下标索引
    last_applied: u64,  //最后被应用到状态机的日志条目索引值，增加Option主要是开始为None，值为log下标索引
    next_index: Option<Vec<u64>>,   //对于每一个服务器，需要发送给他的下一个日志条目的索引值
    match_index: Option<Vec<u64>>,  //对于每一个服务器，已经复制给他的日志的最高索引值

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
            me: me as u64,
            state: State::new(),
            apply_ch,
            voted_for: None,
            log: vec![LogEntry::new()],
            commit_index: 0,
            last_applied: 0,
            next_index: None,
            match_index: None,
        };
        my_debug!("id:{} term:{} islead:{} iscondi:{}", me, rf.state.term(), rf.state.is_leader(), rf.state.is_candidate());
        // initialize from state persisted before a crash
        rf.restore(&raft_state);
        rf
    }
    pub fn term(&self) -> u64{
        self.state.term()
    } 

    pub fn is_leader(&self) -> bool {
        self.state.is_leader()
    }
    pub fn is_candidate(&self) -> bool {
        self.state.is_candidate()
    }
    pub fn get_id(&self) -> u64 {
        self.me
    }
    pub fn get_votefor(&self) -> Option<u64> {
        self.voted_for
    }
    pub fn set_votefor(&mut self, vote: Option<u64>) {
        self.voted_for = vote;
    }
    
    pub fn set_state(&mut self, term: u64, is_leader: bool, is_candidate: bool) {
        let leader = State{
            term,
            is_leader,
            is_candidate,
        };
        self.state = leader;
        my_debug!("set id:{} term:{} islead:{} iscondi:{}", self.me, self.state.term(), self.state.is_leader(), self.state.is_candidate());
        if self.state.is_leader() {
            self.next_index = Some(vec![ 1 ; self.peers.len()]);
            self.match_index = Some(vec![ 0; self.peers.len()]);
        }
        else if self.is_candidate() {
            self.next_index = None;
            self.match_index = None;
        }
        else {
            self.next_index = None;
            self.match_index = None;
        }
        self.persist();
    }

    pub fn get_peers_num(&self) -> usize {
        self.peers.len()
    }

    pub fn get_last_log_index(&self) -> usize {
        self.log.len() - 1 
    }

    pub fn get_last_log_term(&self) -> u64 {
        self.log[self.get_last_log_index()].term
    }

    pub fn get_next_index(&self) -> Option<Vec<u64>> {
        self.next_index.clone()
    }

    pub fn get_log(&self, index: u64) -> Option<LogEntry> {
        if ((self.log.len() - 1) as u64) < index {
            None
        }
        else {
            Some(self.log[index as usize].clone())
        }
    }

    pub fn get_commit_index(&self) -> u64 {
        self.commit_index 
    }

    pub fn update_next_and_match(&mut self, new_next_match: Vec<i32>) {
        my_debug!("id:{} new_next_match:{:?}", self.me, new_next_match);
        if !self.is_leader() || self.match_index == None || self.next_index == None {
            return;
        }
        let mut match_index = self.match_index.clone().unwrap();
        let mut next_index = self.next_index.clone().unwrap();
        my_debug!("id:{} before match_index:{:?}", self.me, match_index);
        my_debug!("id:{} before next_index:{:?}", self.me, next_index);
        for i in 0..self.get_peers_num() {
            if i == self.me as usize {
                continue;
            }
            if new_next_match[i] == 1 {  //收到success
                match_index[i] =  next_index[i] - 1;
                if next_index[i] < self.log.len() as u64 {
                    next_index[i] += 1;
                }
            }
            else if new_next_match[i] == 0 {  //收到fail
                next_index[i] -= 1;
            }
            //收到error，不变
        }
        self.next_index = Some(next_index.clone());
        self.match_index = Some(match_index.clone());
        my_debug!("id:{} after match_index:{:?}", self.me, match_index);
        my_debug!("id:{} after next_index:{:?}", self.me, next_index);
        //查看是否更新commit
        let mut new_commit_index: u64 = 0;
        for index in ((self.commit_index + 1)..(self.log.len() as u64)).rev() { //rev()逆序，从大到小开始检测
            let mut pass: usize = 0;
            for i in 0..self.get_peers_num() as usize {
                if i == self.me as usize {
                    continue;
                }
                if match_index[i] >= index {
                    pass += 1;
                }
            }
            //my_debug!("id:{} pass:{:?}", self.me, pass);
            if (pass + 1) > self.get_peers_num()/2 { //说明通过超过半数，可commit
                new_commit_index = index;
                break;
            }
        }
        if new_commit_index != 0 {
            my_debug!("id:{} new_commit_index:{:?}", self.me, new_commit_index);
            self.set_commit_index(new_commit_index);
        }
    }

    pub fn handle_append_reply(&mut self, id: u64, result: i32, for_next_index: u64) {  //1:sucess 0:fail -1:error
        if !self.is_leader() || self.match_index == None || self.next_index == None {
            return;
        }
        my_debug!("leader:{} handle_append_reply id:{} result:{} for_next_index:{}", self.me, id, result, for_next_index);
        if for_next_index < 1 || for_next_index > self.log.len() as u64 {
            return;
        }
        let mut match_index = self.match_index.clone().unwrap();
        let mut next_index = self.next_index.clone().unwrap();
        my_debug!("id:{} before match_index:{:?}", self.me, match_index);
        my_debug!("id:{} before next_index:{:?}", self.me, next_index);
        let id = id as usize;
        if result == 1 {  //收到success
            match_index[id] =  for_next_index - 1;
            next_index[id] = for_next_index;
        }
        else if result == 0 {  //收到fail
            if next_index[id] < (MAX_SEND_ENTRIES + 1) {
                next_index[id] = 1;
            }
            else {
                next_index[id] -= MAX_SEND_ENTRIES;
            }
        }
        //收到error，不变
        self.next_index = Some(next_index.clone());
        self.match_index = Some(match_index.clone());
        my_debug!("id:{} after match_index:{:?}", self.me, match_index);
        my_debug!("id:{} after next_index:{:?}", self.me, next_index);
        if result == 0 || result == -1 {
            return;
        } 
        //查看是否更新commit
        let mut new_commit_index: u64 = 0;
        for index in ((self.commit_index + 1)..(match_index[id] + 1)).rev() { //rev()逆序，从大到小开始检测
            let mut pass: usize = 0;
            for i in 0..self.get_peers_num() as usize {
                if i == self.me as usize {
                    continue;
                }
                if match_index[i] >= index {
                    pass += 1;
                }
            }
            if (pass + 1) > self.get_peers_num()/2 { //说明通过超过半数，可commit
                new_commit_index = index;
                break;
            }
        }
        if new_commit_index != 0 {
            let log = self.get_log(new_commit_index).unwrap();
            if log.term != self.term() {
                return;
            }
            my_debug!("id:{} new_commit_index:{:?}", self.me, new_commit_index);
            self.set_commit_index(new_commit_index);
        }

    }

    pub fn delete_log(&mut self,save_index: u64) { //删除save_index后面的log，save_index不删除
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
                },
                Err(e) => {},
            }
            my_debug!("id:{} delete log:[{}:{}:{:?}]", self.me, de.index, de.term,_entr);
        }

    }

    pub fn push_log(&mut self, index: u64, term: u64, entry: &Vec<u8>) {
        if self.log.len() as u64 != index {
            my_debug!("error:id:{} push index:{} error log:[{}:{}]", self.me, self.log.len(), index, term);
            return;
        }
        self.log.push(LogEntry::from_data(index, term, entry));
        self.persist(); 
        let mut _entr: Option<Entry> = None;
        match labcodec::decode(&entry) {
            Ok(en) => {
                _entr = Some(en);
            },
            Err(e) => {},
        }
        my_debug!("id:{} push log:[{}:{}:{:?}] ", self.me, index, term, _entr.unwrap());

    }

    pub fn set_commit_index(&mut self, new_commit_index: u64) {
        if new_commit_index < self.commit_index {
            my_debug!("error:id:{} set_commit_index fail:[{}-{}]", self.me, self.commit_index, new_commit_index);
            return;
        }
        my_debug!("id:{} set commit_index:[{}->{}]", self.me, self.commit_index, new_commit_index);
        self.commit_index = new_commit_index;
        if self.commit_index > self.last_applied { //更新状态机
        let last = self.last_applied;
            for i in last..self.commit_index {
                self.last_applied += 1; //并发送
                let mesg = ApplyMsg {
                    command_valid: true,
                    command: self.log[self.last_applied as usize].entry.clone(),
                    command_index: self.last_applied,
                };
                let _ret = self.apply_ch.unbounded_send(mesg);
                my_debug!("id:{} apply_ch:[{}]", self.me, self.last_applied);
                self.persist();
            }
        }

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
            logs: vec![],
        };
        for i in 1..self.log.len() { //从1开始，不保存index为0的空log
            let mut dat = vec![];
            let log = self.log[i].clone();
            let _ret = labcodec::encode(&log, &mut dat).map_err(Error::Encode);
            raft_state.logs.push(dat);
        }
        let _ret = labcodec::encode(&raft_state, &mut data).map_err(Error::Encode);
        self.persister.save_raft_state(data);
    }

    /// restore previously persisted state.
    fn restore(&mut self, data: &[u8]) {
        //println!("state:{:?}", data);
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

                my_debug!("id:{} restore state:{:?}", self.me, state);
                my_debug!("num:{}", state.logs.len());
                for i in 0..state.logs.len() {
                    let log_encode = state.logs[i].clone();
                     match labcodec::decode(&log_encode) {
                        Ok(log) => {
                            let log: LogEntry = log;
                            self.log.push(log.clone());
                            my_debug!("id:{} restore log :{:?}", self.me, log);
                        },
                        Err(e) => {
                            panic!("{:?}", e);
                        },
                     }
                }
                //my_debug!("id:{} restore set state:{:?}", self.me, self.state);

            },
            Err(e) => {
                panic!("{:?}", e);
            },
        }
    }

    /// example code to send a RequestVote RPC to a server.
    /// server is the index of the target server in peers.
    /// expects RPC arguments in args.
    ///
    /// The labrpc package simulates a lossy network, in which servers
    /// may be unreachable, and in which requests and replies may be lost.
    /// This method sends a request and waits for a reply. If a reply arrives
    /// within a timeout interval, This method returns OK(_); otherwise
    /// this method returns Err(_). Thus this method may not return for a while.
    /// An Err(_) return can be caused by a dead server, a live server that
    /// can't be reached, a lost request, or a lost reply.
    ///
    /// This method is guaranteed to return (perhaps after a delay) *except* if
    /// the handler function on the server side does not return.  Thus there
    /// is no need to implement your own timeouts around this method.
    ///
    /// look at the comments in ../labrpc/src/mod.rs for more details.
    pub fn send_request_vote(&self, server: usize, args: &RequestVoteArgs) -> Result<RequestVoteReply> {
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
        //             OK(())
        //         }),
        // );
        // rx.wait() ...
        // ```
        /*let (tx, rx) = mpsc::channel();
         peer.spawn(
             peer.request_vote(&args)
                 .map_err(Error::Rpc)
                .then(move |res| {
                     tx.send(res);
                     Ok(())
                 }),
         );
         rx.wait() */
        peer.request_vote(&args).map_err(Error::Rpc).wait()
    }

    pub fn get_peers(&self) -> Vec<RaftClient> {
        self.peers.clone()

    }
    pub fn send_append_entries(&self, server: usize, args: &RequestEntryArgs) -> Result<RequestEntryReply> {
        let peer = &self.peers[server];
        /*let (tx, rx) = mpsc::channel();
         peer.spawn(
             peer.append_entries(&args)
                 .map_err(Error::Rpc)
                .then(move |res| {
                     tx.send(res);
                     Ok(())
                 }),
         );
         rx.wait();
         Err(Error::Rpc)*/
        peer.append_entries(&args).map_err(Error::Rpc).wait()
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
        }
        else {
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
    raft: Arc<Mutex<Raft>>,
    timeout_thread: Arc<Mutex<Option<thread::JoinHandle<()>>>>,  //超时线程检测，只有follower有
    timeout_true: Arc<Mutex<bool>>,                               //超时线程信号，true时代表超时，false代表不超时，简单重置
    append_thread: Arc<Mutex<Option<thread::JoinHandle<()>>>>,   //心跳线程，只有leader有
    heart_beat_true: Arc<Mutex<i32>>,                             //心跳线程信号，0表示简单重置，1表示心跳，2表示发送包
    vote_thread: Arc<Mutex<Option<thread::JoinHandle<()>>>>,    //投票线程
    shutdown: Arc<Mutex<bool>>,                                  //关闭信号
    send: Arc<Mutex<Option<Sender<u32>>>>,


}

impl Node {
    /// Create a new raft service.
    pub fn new(raft: Raft) -> Node {
        // Your code here.
        let inode = Node {
            raft: Arc::new(Mutex::new(raft)),
            timeout_thread: Arc::new(Mutex::new(None)),
            timeout_true: Arc::new(Mutex::new(false)),
            append_thread: Arc::new(Mutex::new(None)),
            heart_beat_true: Arc::new(Mutex::new(0)),
            vote_thread: Arc::new(Mutex::new(None)),
            shutdown: Arc::new(Mutex::new(false)),
            send: Arc::new(Mutex::new(None)),
        };
        my_debug!("New inode:{}",inode.get_id());
        let recv = Node::creat_timeout_thread(inode.clone());
        Node::creat_vote_thread(inode.clone(),recv);
        Node::creat_append_thread(inode.clone());
        inode
    }
    pub fn creat_timeout_thread(inode: Node) -> mpsc::Receiver<u32> {
        let (vote_send, vote_recv) = mpsc::channel();
        let send = mpsc::Sender::clone(&vote_send);
        let iinode = inode.clone();
        let tthread = thread::spawn(move || {
            my_debug!("creat_timeout_thread:{:p}", &iinode);
            loop {
                let time = Instant::now();
                match iinode.is_leader() {
                    true => thread::park(),
                    false => {},
                }
                let rand_sleep = Duration::from_millis(rand::thread_rng().gen_range(TIMEOUT_LOW_BOUND, TIMEOUT_HIGH_BOUND));
                *iinode.timeout_true.lock().unwrap() = true;
                thread::park_timeout(rand_sleep);
                //my_debug!("id:{} timeout_thread unpark:{}", iinode.get_id(), iinode.timeout_true.load(Ordering::Relaxed));
                if *iinode.shutdown.lock().unwrap() == true { //关闭
                    my_debug!("id:{} shutdown timeout_thread ", iinode.get_id());
                    break;
                }
                let time2 = time.elapsed().as_millis();
                if *iinode.timeout_true.lock().unwrap() == true && time2 >= TIMEOUT_LOW_BOUND as u128 { //超时，需要成为候选者
                    let time2 = time.elapsed().as_millis();
                    my_debug!("id:{} vote_send time:{}ms timeout:{}",iinode.get_id(), time2 ,*iinode.timeout_true.lock().unwrap());
                    let _ret = vote_send.send(1); //发送成为候选者信号
                }
            }

        });
        *inode.timeout_thread.lock().unwrap() = Some(tthread);
        *inode.send.lock().unwrap() = Some(send);
        vote_recv
    }

    pub fn creat_vote_thread(iiinode: Node,vote_recv: mpsc::Receiver<u32>) {
        let inode = iiinode.clone();
        let vthread = thread::spawn(move || { //投票线程
            my_debug!("creat_vote_thread:{:p}", &inode);
            loop {
                vote_recv.recv().unwrap();  //接收到成为候选者
                if *inode.shutdown.lock().unwrap() == true { //关闭
                    my_debug!("id:{} shutdown vote_thread ", inode.get_id());
                    break;
                }
                let mut term = inode.term();
                term += 1;
                inode.set_state(term, false, true);
                let id = inode.get_id();
                let args = RequestVoteArgs {
                    term,
                    candidate_id: id as u64,
                    last_log_index: inode.get_last_log_index() as u64,
                    last_log_term: inode.get_last_log_term(),
                };
                my_debug!("candidate:{} term:{} commit:{} args:{:?}", inode.get_id(),inode.term(), inode.get_commit_index(), args);
                let passed = Arc::new(Mutex::new(1));  //先给自己投一票
                let have_better_leader = Arc::new(Mutex::new(false)); //存在term更大的节点
                //let mut handles = vec![];
                let nums = inode.get_peers_num();
                let peers;
                {
                    peers = inode.raft.lock().unwrap().get_peers();
                }
                //let (send, recv) = mpsc::channel();
                for i in 0..nums {
                    if i == id as usize {
                        continue;
                    }
                    let time = Instant::now();
                    let leader_id = id;
                    let nums = nums as u64;
                    let iinode = inode.clone();
                    let passed = Arc::clone(&passed);
                    let have_better_leader = Arc::clone(&have_better_leader);
                    let args = args.clone();
                    let peer = peers[i].clone();
                    let term = term;
                    //let tsend = mpsc::Sender::clone(&send);
                    thread::spawn(move || {
                        let ret = peer.request_vote(&args).map_err(Error::Rpc).wait();
                        match ret {
                            Ok(rep) => {
                                //let mut raft = iinode.raft.lock().unwrap();  //原子操作
                                if rep.vote_granted {
                                    *passed.lock().unwrap() += 1;
                                    let time2 = time.elapsed().as_millis();
                                    my_debug!("candidate:{} for vote:{} passed time:{}!", leader_id, i, time2);
                                    if *passed.lock().unwrap() > nums/2 && iinode.is_candidate() && term == iinode.term() {  //投票成功，并仍是候选者状态
                                            iinode.set_state(term, true, false); //成为leared
                                            //广播心跳
                                            my_debug!("{} become leader!", iinode.get_id());
                                            Node::send_followers_append_entries(iinode.clone());
                                            iinode.reset_append_thread();
                                            iinode.reset_timeout_thread();
                                    }
                                    //*passed.lock().unwrap() += 1;
                                    //my_debug!("candidate:{} for vote:{} passed!", leader_id, i);
                                }
                                else if rep.term > iinode.term() { //遇到term更大节点
                                    iinode.set_state(rep.term, false, false);
                                    iinode.set_votefor(None);
                                    iinode.reset_timeout_thread();
                                    //*have_better_leader.lock().unwrap() = true;
                                    let time2 = time.elapsed().as_millis();
                                    my_debug!("candidate:{} for vote:{} fail time:{}ms find have_better_leader because term!", leader_id, i, time2);

                                }
                                //else if rep.last_log_index > iinode.get_last_log_index() as u64 { //遇到log更新的节点
                                    //*have_better_leader.lock().unwrap() = true;
                                    //my_debug!("candidate:{} for vote:{} find have_better_leader because log index!", iinode.get_id(), i);
                                //}
                            },
                            Err(_) => {
                                //let time2 = time.elapsed().as_millis();
                                //my_debug!("candidate:{} for vote:{} error time:{}ms!", leader_id, i, time2);
                            },
                        }
                        //let _ret = tsend.send(1);
                    });
                    //handles.push(handle);
                }
                /*for handle in handles {
                    handle.join().unwrap();
                }*/
                /*for j in 0..(nums - 1){
                    let _ret = recv.recv_timeout(Duration::from_millis(BROADCAST_ERROR_WAIT_TIME));
                    if *have_better_leader.lock().unwrap() == true || (*passed.lock().unwrap() + 1 ) > nums/2 {
                        break;
                    }
                }
                //thread::sleep(Duration::from_millis(BROADCAST_WAIT_TIME));
                let time2 = time.elapsed().as_millis();
                my_debug!("candidate:{} vote time:{}ms", inode.get_id(), time2);
                let passed = *passed.lock().unwrap();
                if *have_better_leader.lock().unwrap() != true && (passed + 1 ) > nums/2  { //没有term更大的节点，并且超过半数同意
                    if inode.is_candidate() { //仍是候选者状态
                        inode.set_state(term, true, false); //成为leared
                        //广播心跳
                        my_debug!("{} become leader!", inode.get_id());
                        Node::send_followers_append_entries(inode.clone());
                        inode.reset_append_thread();
                        inode.reset_timeout_thread();
                    }
                }*/
            
            }

        });
        *iiinode.vote_thread.lock().unwrap() = Some(vthread);
    }

    pub fn reset_timeout_thread(&self) {
        match *self.timeout_thread.lock().unwrap() {
            Some(ref thread) => {
                my_debug!("{} reset_timeout_thread", self.get_id());
                *self.timeout_true.lock().unwrap() = false;
                thread.thread().unpark();
            }
            None => {
                return;
            }
        }
    }

    pub fn creat_append_thread(inode: Node) {
        let iinode = inode.clone();
        let athread = thread::spawn(move || {
            my_debug!("creat_append_thread:{:p}", &iinode);
            loop {
                match iinode.is_leader() {
                    true => {},
                    false => thread::park(),
                }
                let rand_sleep = Duration::from_millis(rand::thread_rng().gen_range(HEART_BEAT_LOW_BOUND, HEART_BEAT_HIGH_BOUND));
                *iinode.heart_beat_true.lock().unwrap() = 1;
                thread::park_timeout(rand_sleep);
                if *iinode.shutdown.lock().unwrap() == true { //关闭
                    my_debug!("id:{} shutdown append_thread ", iinode.get_id());
                    break;
                }
                if iinode.is_leader() && *iinode.heart_beat_true.lock().unwrap() == 1 { //心跳，广播,发送包和心跳一样
                    Node::send_followers_append_entries(iinode.clone());
                }
            }

        });
        *inode.append_thread.lock().unwrap() = Some(athread);
    }

    pub fn send_followers_append_entries(inode: Node) { //广播包或者心跳
        if !inode.is_leader() {
            return;
        }
        let id = inode.get_id();
        let nums = inode.get_peers_num();
        //let results = vec![Arc::new(AtomicI32::new(-1)); nums];  //  错误的写法，这样生成的vec是通过Arc.clone()，再插入,导致只有一个值，查找了好久的bug
        /*let mut results: Vec<Arc<Mutex<i32>>> = vec![];                       //  广播的结果  error:-1, false:0, true:1
        for _ in 0..nums {
            results.push(Arc::new(Mutex::new(-1)));
        } */
        //let have_bigger_term = Arc::new(Mutex::new(false));
        //let biggest_term = Arc::new(Mutex::new(0));
        //let mut handles = vec![];
        my_debug!("leader:{} heart beat! term:{}", id, inode.term());
        //let time = Instant::now();
        let peers;
        {
            peers = inode.raft.lock().unwrap().get_peers();
        }
        let next_index = match inode.get_next_index() {
            Some(index) => index ,
            None => { return; },
        };
        my_debug!("leader:{} next_index:{:?}", id, next_index);
        //let (send, recv) = mpsc::channel();
        for i in 0..nums {
            if i == id as usize {
                continue;
            }
            let leader_id = id;
            let iinode = inode.clone();
            let time = Instant::now();
            //let result = results[i].clone();
            //let have_bigger_term_i = Arc::clone(&have_bigger_term);
            //let biggest_term_i = Arc::clone(&biggest_term);
            let mut args = RequestEntryArgs {
                term: inode.term(),
                leader_id: id as u64,
                prev_log_index: next_index[i] - 1,
                prev_log_term: 0,
                entries: vec![], //entries是prev_log_index的下一个
                leader_commit: inode.get_commit_index(),
            };
            let entry = inode.get_log(args.prev_log_index); //一定可得到entry,可unwrap()
            args.prev_log_term = entry.unwrap().term;  
            for j in 0..MAX_SEND_ENTRIES {
                let entry_next = inode.get_log(next_index[i] + j);
                match entry_next {
                    Some(en) => {
                        let mut dat = vec![];
                        let _ret = labcodec::encode(&en, &mut dat).map_err(Error::Encode);
                        args.entries.push(dat);
                    },
                    None => {
                        break;
                    },
                }
            }
            let peer = peers[i].clone();
            //let tsend = mpsc::Sender::clone(&send);
            thread::spawn(move || {
                my_debug!("leader:{} do heart beat:{} args:{:?}! ", leader_id, i, args);
                let ret = peer.append_entries(&args).map_err(Error::Rpc).wait();
                match ret {
                    Ok(rep) => {
                        if rep.success  {
                            //*result.lock().unwrap() = 1;
                            if rep.term == iinode.term() {
                                iinode.handle_append_reply(i as u64, 1, rep.next_index);

                            }
                            let time2 = time.elapsed().as_millis();
                            my_debug!("leader:{} do heart beat:{} success time:{}ms! ", leader_id, i, time2);
                        }
                        else {
                            //*result.lock().unwrap() = 0;
                            if rep.term > iinode.term() {  //发现存在比自己任期大的节点
                                my_debug!("leader:[{}:{}] set_follow because id:{} bigger term:{}", leader_id, iinode.term(), i, rep.term);
                                iinode.set_state(rep.term, false, false);
                                iinode.set_votefor(None);
                                iinode.reset_timeout_thread();
                            }
                            else {
                                if rep.term == iinode.term() {
                                    iinode.handle_append_reply(i as u64, 0, rep.next_index);
                                }
                            }
                            let time2 = time.elapsed().as_millis();
                            my_debug!("leader:{} do heart beat:{} failed time:{}ms!", leader_id, i, time2);
                        } 
                    },
                    Err(_) => {
                        //let time2 = time.elapsed().as_millis();
                        //my_debug!("leader:{} do heart beat:{} error time:{}ms! ", leader_id, i, time2);
                    },
                }
                //let _ret = tsend.send(1);
            });
            //handles.push(handle);
        }
        /*for handle in handles {
            handle.join().unwrap();
        }*/
        /*for j in 0..(nums - 1){
            let _ret = recv.recv_timeout(Duration::from_millis(BROADCAST_ERROR_WAIT_TIME));  //对于一个error的reply，最多等BROADCAST_ERROR_WAIT_TIME毫秒
            //let _ret = recv.recv();
        }
        if *have_bigger_term.lock().unwrap() == true {  //发现存在比自己任期大的节点
            my_debug!("leader:[{}:{}] biggest_term:{}", inode.get_id(), inode.term(), *biggest_term.lock().unwrap());
            if inode.term() < *biggest_term.lock().unwrap() {
                my_debug!("leader:[{}:{}] set_follow", inode.get_id(), inode.term());
                inode.set_state(*biggest_term.lock().unwrap(), false, false);
                inode.set_votefor(None);
                inode.reset_timeout_thread();
            }
        }
        else{
            //let mut raft = inode.raft.lock().unwrap(); //为了保证原子性
            let mut new_next_match: Vec<i32> = vec![-1; nums];
            for j in 0..nums {
                //my_debug!("leader:[{}] results:{}", inode.get_id(), results[j].load(Ordering::Relaxed));
                new_next_match[j] = *results[j].lock().unwrap();
            }
            inode.update_next_and_match(new_next_match); //更新next_index和match_index

        }*/
        
        //thread::sleep(Duration::from_millis(5));
        /*let time2 = time.elapsed().as_millis();
        my_debug!("leader:{} append time:{}ms", inode.get_id(), time2);*/

    }

    pub fn reset_append_thread(&self) {
        match *self.append_thread.lock().unwrap() {
            Some(ref thread) => {
                *self.heart_beat_true.lock().unwrap() = 0;
                thread.thread().unpark();
            }
            None => {
                return;
            }
        }
    }

    pub fn reset_append_thread_heart_beat(&self) {
        match *self.append_thread.lock().unwrap() {
            Some(ref thread) => {
                *self.heart_beat_true.lock().unwrap() = 1;
                thread.thread().unpark();
            }
            None => {
                return;
            }
        }
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
        if self.is_leader() {
            let ret = self.raft.lock().unwrap().start(command);
            //self.reset_append_thread_heart_beat();
            ret
        }
        else {
            Err(Error::NotLeader)
        }
        // Your code here.
        // Example:
        // self.raft.start(command)
        //unimplemented!()
    }

    /// The current term of this peer.
    pub fn term(&self) -> u64 {
        // Your code here.
        // Example:
        // self.raft.term
        self.raft.lock().unwrap().term()
        //unimplemented!()
    }

    /// Whether this peer believes it is the leader.
    pub fn is_leader(&self) -> bool {
        // Your code here.
        // Example:
        // self.raft.leader_id == self.id
        self.raft.lock().unwrap().is_leader()
        //unimplemented!()
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

    pub fn get_votefor(&self) -> Option<u64> {
        self.raft.lock().unwrap().get_votefor()
    }

    pub fn set_votefor(&self, vote: Option<u64>) {
        let mut raft = self.raft.lock().unwrap();
        raft.set_votefor(vote);
    }

    pub fn set_state(&self, term: u64, is_leader: bool, is_candidate: bool) {
        let mut raft = self.raft.lock().unwrap();
        raft.set_state(term, is_leader, is_candidate);
    }

    pub fn get_peers_num(&self) -> usize {
        self.raft.lock().unwrap().get_peers_num()
    }

    pub fn get_id(&self) -> u64 {
        self.raft.lock().unwrap().get_id()
    }

    pub fn get_last_log_index(&self) -> usize {
        self.raft.lock().unwrap().get_last_log_index()
    }

    pub fn get_last_log_term(&self) -> u64 {
        self.raft.lock().unwrap().get_last_log_term()
    }

    pub fn get_next_index(&self) -> Option<Vec<u64>> {
        self.raft.lock().unwrap().get_next_index()
    }

    pub fn get_log(&self, index: u64) -> Option<LogEntry> {
        self.raft.lock().unwrap().get_log(index)
    }

    pub fn get_commit_index(&self) -> u64 {
        self.raft.lock().unwrap().get_commit_index() 
    }

    pub fn delete_log(&self, save_index: u64) {
        let mut raft = self.raft.lock().unwrap();
        raft.delete_log(save_index);
    }

    pub fn push_log(&self, index: u64, term: u64, entry: &Vec<u8>) {
        let mut raft = self.raft.lock().unwrap();
        raft.push_log(index, term, entry);
    }

    pub fn set_commit_index(&self, new_commit_index: u64) {
        let mut raft = self.raft.lock().unwrap();
        raft.set_commit_index(new_commit_index);
    }

    pub fn update_next_and_match(&self, new_next_match: Vec<i32>) {
        let mut raft = self.raft.lock().unwrap();
        raft.update_next_and_match(new_next_match);
    }

    pub fn handle_append_reply(&self, id: u64, result: i32, for_next_index: u64) {
        let mut raft = self.raft.lock().unwrap();
        raft.handle_append_reply(id, result, for_next_index);
    }

    /// the tester calls kill() when a Raft instance won't
    /// be needed again. you are not required to do anything
    /// in kill(), but it might be convenient to (for example)
    /// turn off debug output from this instance.
    pub fn kill(&self) {
        // Your code here, if desired.
        *self.shutdown.lock().unwrap() = true;
        match *self.send.lock().unwrap() {
            Some(ref send) => {
                let _ret = send.send(1);
            },
            None => {},
        }
        for _ in 0..2 {
            self.reset_timeout_thread();
            self.reset_append_thread();
            thread::sleep(Duration::from_millis(TIMEOUT_HIGH_BOUND + 1));
        }
        
        /*match vt {
            Some(thread) => {
                thread.join().unwrap();
            },
            None => {},
        }*/
        /*let mut send;
        {
            send = self.send.lock().unwrap().clone();
        }
        let _ret = send.unwrap().send(1);
        let mut vt;
        {
            vt = self.vote_thread.lock().unwrap().unwrap().clone();
        }
        vt.join().unwrap();
        self.reset_timeout_thread();
        self.timeout_thread.lock().unwrap().unwrap().join().unwrap();
        self.reset_append_thread();
        self.append_thread.lock().unwrap().unwrap().join().unwrap();*/


    }
}

impl RaftService for Node {
    // example RequestVote RPC handler.
    fn request_vote(&self, args: RequestVoteArgs) -> RpcFuture<RequestVoteReply> {
        // Your code here (2A, 2B).
        //my_debug!("id:{}:{} request_vote begin {:?}!", self.get_id(),self.term(), args);
        let mut reply = RequestVoteReply{
            term: self.term(),
            vote_granted: false,
            //last_log_index: 0,
        };
        //my_debug!("id:{}:{} {}->{}!", self.get_id(),self.term(), self.get_last_log_term(), args.last_log_term);
        //my_debug!("id:{}:{} {}->{}!", self.get_id(),self.term(), self.get_last_log_index(), args.last_log_index);
        if args.term <= self.term() { //term更小
            reply.vote_granted = false;
        }
        else {
            //self.set_state(args.term, false, false);   //这里的term一定遇到更大的term，所以更新term
            if args.last_log_term < self.get_last_log_term() as u64 {  //最后的日志的term更小
                reply.vote_granted = false;
            }
            else if args.last_log_term == self.get_last_log_term() && args.last_log_index < self.get_last_log_index() as u64 { //最后的日志的term相同，但是个数更少
                reply.vote_granted = false;
                //reply.last_log_index = self.get_last_log_index() as u64;
            }
            else {
                reply.vote_granted = true;
                //self.set_state(args.term, false, false);
                self.set_votefor(Some(args.candidate_id));
                //self.reset_timeout_thread();
            }
            self.set_state(args.term, false, false);
            reply.term = self.term();
            self.reset_timeout_thread();

        }
        //my_debug!("id:{} request_vote {} end {:?}! ", self.get_id(), reply.vote_granted, reply);
        Box::new(futures::future::result(Ok(reply)))
        //unimplemented!()
    }

    fn append_entries(&self, args: RequestEntryArgs) -> RpcFuture<RequestEntryReply> {
        my_debug!("id:{} append_entries begin {:?}!", self.get_id(),args);
        let mut reply = RequestEntryReply {
            term: self.term(),
            success: false,
            next_index: 1,
        };
        if args.term < self.term() {
            my_debug!("error:me[{}:{}] recv [{}:{}]", self.get_id(), self.term(), args.leader_id, args.term);
        }
        else {
            //self.reset_timeout_thread();
            let entry = self.get_log(args.prev_log_index);
            match entry {
                Some(en) => { //说明存在entry
                    if en.term == args.prev_log_term {  //可以match成功，args.entries有用
                        if args.entries.len() == 0 {  //说明是心跳

                        }
                        else {  //说明args.entries有效，
                            for i in 0..args.entries.len() {
                                let log_encode = args.entries[i].clone();
                                match labcodec::decode(&log_encode) {
                                    Ok(log) => {
                                        let log: LogEntry = log;
                                        let node_entry = self.get_log(args.prev_log_index + 1 + i as u64);
                                        match node_entry {
                                            Some(n_en) => {
                                                if n_en == log { //log 相等，无需删除
                                                    continue;
                                                }
                                                else {
                                                    self.delete_log(args.prev_log_index + i as u64);
                                                    self.push_log(log.index, log.term, &log.entry);
                                                }
                                            },
                                            None => {
                                                self.push_log(log.index, log.term, &log.entry);
                                            }


                                        }
                                    },
                                    Err(e) => {
                                        panic!("{:?}", e);
                                    },
                                } 
                            }
                        }
                        //进入这里，说明匹配成功，可以返回success
                        reply.success = true;
                        reply.next_index = args.prev_log_index + 1 + args.entries.len() as u64;
                        let can_commit = cmp::min(args.leader_commit, reply.next_index - 1);
                        if can_commit > self.get_commit_index() {  //返回success,可提交commit
                            let new_commit_index: u64 = cmp::min(can_commit, self.get_last_log_index() as u64);
                            self.set_commit_index(new_commit_index);
                        }

                    }
                    else { //说明false，args.entries无用,并且当前节点的log[args.prev_log_index]有错，等到可以匹配后再一次删后面的所有
                        my_debug!("error:match fail me[{}:{}-{}:{}] recv [{}:{}-{}:{}]", self.get_id(), self.term(), self.get_last_log_index(), en.term, args.leader_id, args.term, args.prev_log_index, args.prev_log_term);
                    }

                },
                None => {},  //false,说明匹配失败，
            }
            //进行到这里，说明leader有效（一个term只有一个leader）
            self.set_state(args.term, false, false);
            self.set_votefor(None);
            reply.term = self.term();
            self.reset_timeout_thread();

        }
        my_debug!("id:{} append_entries end {:?}!", self.get_id(), reply);
        Box::new(futures::future::result(Ok(reply)))
    }
}
