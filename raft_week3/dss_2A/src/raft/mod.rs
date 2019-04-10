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
use std::sync::atomic::{AtomicBool, AtomicI32, Ordering};
use std::thread;
use std::time::{Instant, Duration};
use rand::Rng;

#[macro_export]
macro_rules! my_debug {
    ($($arg: tt)*) => (
        //println!("Debug[{}:{}]: {}", file!(), line!(),format_args!($($arg)*));
    )
}

const TIMEOUT_LOW_BOUND: u64 = 150;
const TIMEOUT_HIGH_BOUND: u64 = 200;
const HEART_BEAT_LOW_BOUND: u64 = 20;
const HEART_BEAT_HIGH_BOUND: u64 = 25;

const BROADCAST_ERROR_WAIT_TIME: u64 = 2; //广播时接收reply，每个接收最多等待2ms，意味着如果有两个节点error，投票或者心跳接收会等待4ms，

pub struct ApplyMsg {
    pub command_valid: bool,
    pub command: Vec<u8>,
    pub command_index: u64,
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

// A single Raft peer.
pub struct Raft {
    // RPC end points of all peers
    peers: Vec<RaftClient>,
    // Object to hold this peer's persisted state
    persister: Box<dyn Persister>,
    // this peer's index into peers[]
    me: u64,
    state: Arc<State>,
    voted_for: Option<u64>,

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
            state: Arc::default(),
            voted_for: None,
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
        self.state = Arc::new(leader);
        my_debug!("set id:{} term:{} islead:{} iscondi:{}", self.me, self.state.term(), self.state.is_leader(), self.state.is_candidate());
    }

    pub fn get_peers_num(&self) -> usize {
        self.peers.len()
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

    fn start<M>(&self, command: &M) -> Result<(u64, u64)>
    where
        M: labcodec::Message,
    {
        let index = 0;
        let term = 0;
        let is_leader = true;
        let mut buf = vec![];
        labcodec::encode(command, &mut buf).map_err(Error::Encode)?;
        // Your code here (2B).

        if is_leader {
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
    raft: Arc<Mutex<Raft>>,
    timeout_thread: Arc<Mutex<Option<thread::JoinHandle<()>>>>,  //超时线程检测，只有follower有
    timeout_true: Arc<AtomicBool>,                               //超时线程信号，true时代表超时，false代表不超时，简单重置
    append_thread: Arc<Mutex<Option<thread::JoinHandle<()>>>>,   //心跳线程，只有leader有
    heart_beat_true: Arc<AtomicI32>,                             //心跳线程信号，0表示简单重置，1表示心跳，2表示发送包
    vote_thread: Arc<Mutex<Option<thread::JoinHandle<()>>>>,    //投票线程
    shutdown: Arc<AtomicBool>,                                  //关闭信号
    send: Arc<Mutex<Option<Sender<u32>>>>,


}

impl Node {
    /// Create a new raft service.
    pub fn new(raft: Raft) -> Node {
        // Your code here.
        let inode = Node {
            raft: Arc::new(Mutex::new(raft)),
            timeout_thread: Arc::new(Mutex::new(None)),
            timeout_true: Arc::new(AtomicBool::new(false)),
            append_thread: Arc::new(Mutex::new(None)),
            heart_beat_true: Arc::new(AtomicI32::new(0)),
            vote_thread: Arc::new(Mutex::new(None)),
            shutdown: Arc::new(AtomicBool::new(false)),
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
                match iinode.is_leader() {
                    true => thread::park(),
                    false => {},
                }
                let rand_sleep = Duration::from_millis(rand::thread_rng().gen_range(TIMEOUT_LOW_BOUND, TIMEOUT_HIGH_BOUND));
                iinode.timeout_true.store(true, Ordering::Relaxed);
                thread::park_timeout(rand_sleep);
                my_debug!("id:{} timeout_thread unpark:{}", iinode.get_id(), iinode.timeout_true.load(Ordering::Relaxed));
                if iinode.shutdown.load(Ordering::Relaxed) == true { //关闭
                    my_debug!("id:{} shutdown timeout_thread ", iinode.get_id());
                    break;
                }
                if iinode.timeout_true.load(Ordering::Relaxed) == true && iinode.get_votefor() == None { //超时，并且没有投票，需要成为候选者
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
                if inode.shutdown.load(Ordering::Relaxed) == true { //关闭
                    my_debug!("id:{} shutdown vote_thread ", inode.get_id());
                    break;
                }
                let mut term = inode.term();
                term += 1;
                inode.set_state(term, false, true);
                my_debug!("candidate:{} term:{}", inode.get_id(),inode.term());
                let id = inode.get_id();
                let args = RequestVoteArgs {
                    term,
                    candidate_id: id as u64,
                };

                let passed = Arc::new(Mutex::new(0));
                let have_better_leader = Arc::new(Mutex::new(false)); //存在term更大的节点
                let mut handles = vec![];
                let nums = inode.get_peers_num();
                let time = Instant::now();
                let peers;
                {
                    peers = inode.raft.lock().unwrap().get_peers();
                }
                let (send, recv) = mpsc::channel();
                for i in 0..nums {
                    if i == id as usize {
                        continue;
                    }
                    let iinode = inode.clone();
                    let passed = Arc::clone(&passed);
                    let have_better_leader = Arc::clone(&have_better_leader);
                    let args = args.clone();
                    let peer = peers[i].clone();
                    let tsend = mpsc::Sender::clone(&send);
                    let handle = thread::spawn(move || {
                        let ret = peer.request_vote(&args).map_err(Error::Rpc).wait();
                        match ret {
                            Ok(rep) => {
                                if rep.vote_granted {
                                    *passed.lock().unwrap() += 1;
                                    my_debug!("candidate:{} for vote:{} passed!", iinode.get_id(), i);
                                }
                                else if rep.term > term { //遇到term更大的节点
                                    *have_better_leader.lock().unwrap() = true;
                                    my_debug!("candidate:{} for vote:{} find have_better_leader!", iinode.get_id(), i);
                                }
                            },
                            Err(_) => {
                                my_debug!("candidate:{} for vote:{} error!", iinode.get_id(), i);
                            },
                        }
                        let _ret = tsend.send(1);
                    });
                    handles.push(handle);
                }
                /*for handle in handles {
                    handle.join().unwrap();
                }*/
                for j in 0..(nums - 1){
                    let _ret = recv.recv_timeout(Duration::from_millis(BROADCAST_ERROR_WAIT_TIME));
                    if *have_better_leader.lock().unwrap() == true || (*passed.lock().unwrap() + 1 ) > nums/2 {
                        break;
                    }
                }
                //thread::sleep(Duration::from_millis(BROADCAST_WAIT_TIME));
                let time2 = time.elapsed().as_millis();
                my_debug!("candidate:{} time:{}", inode.get_id(), time2);
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
                }
            
            }

        });
        *iiinode.vote_thread.lock().unwrap() = Some(vthread);
    }

    pub fn reset_timeout_thread(&self) {
        match *self.timeout_thread.lock().unwrap() {
            Some(ref thread) => {
                my_debug!("{} reset_timeout_thread", self.get_id());
                self.timeout_true.store(false, Ordering::Relaxed);
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
            my_debug!("creat_vote_thread:{:p}", &iinode);
            loop {
                match iinode.is_leader() {
                    true => {},
                    false => thread::park(),
                }
                let rand_sleep = Duration::from_millis(rand::thread_rng().gen_range(HEART_BEAT_LOW_BOUND, HEART_BEAT_HIGH_BOUND));
                iinode.heart_beat_true.store(1, Ordering::Relaxed);
                thread::park_timeout(rand_sleep);
                if iinode.shutdown.load(Ordering::Relaxed) == true { //关闭
                    my_debug!("id:{} shutdown append_thread ", iinode.get_id());
                    break;
                }
                if iinode.is_leader() && iinode.heart_beat_true.load(Ordering::Relaxed) == 1 { //心跳，广播
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
        let args = RequestEntryArgs {
            term: inode.term(),
            leader_id: id as u64,
        };
        let passed = Arc::new(Mutex::new(0));
        let mut handles = vec![];
        let nums = inode.get_peers_num();
        my_debug!("leader:{} heart beat! term:{}", id, inode.term());
        let time = Instant::now();
        let peers;
        {
            peers = inode.raft.lock().unwrap().get_peers();
        }
        let (send, recv) = mpsc::channel();
        for i in 0..nums {
            if i == id as usize {
                continue;
            }
            let iinode = inode.clone();
            let passed = Arc::clone(&passed);
            let args = args.clone();
            let peer = peers[i].clone();
            let tsend = mpsc::Sender::clone(&send);
            let handle = thread::spawn(move || {
                let ret = peer.append_entries(&args).map_err(Error::Rpc).wait();
                match ret {
                    Ok(rep) => {
                        if rep.success  {
                            *passed.lock().unwrap() += 1;
                            my_debug!("leader:{} for heart beat:{} success! ", iinode.get_id(), i);
                        }
                        else {
                            my_debug!("leader:{} for heart beat:{} failed! ", iinode.get_id(), i);
                        } 
                    },
                    Err(_) => {
                        my_debug!("leader:{} for heart beat:{} error! ", iinode.get_id(), i);
                    },
                }
                let _ret = tsend.send(1);
            });
            handles.push(handle);
        }
        /*for handle in handles {
            handle.join().unwrap();
        }*/
        for j in 0..(nums - 1){
            let _ret = recv.recv_timeout(Duration::from_millis(BROADCAST_ERROR_WAIT_TIME));
            if (*passed.lock().unwrap() + 1 ) > nums/2 {
                break;
            }
        }
        //thread::sleep(Duration::from_millis(5));
        let time2 = time.elapsed().as_millis();
        my_debug!("leader:{} time:{}", inode.get_id(), time2);
        let passed = *passed.lock().unwrap();
        if (passed + 1 ) > nums/2  {

        }
        else{
            my_debug!("error:leader:{} send_followers_append_entries no pass!", id);
        }

    }

    pub fn reset_append_thread(&self) {
        match *self.append_thread.lock().unwrap() {
            Some(ref thread) => {
                self.heart_beat_true.store(0, Ordering::Relaxed);
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
                self.heart_beat_true.store(1, Ordering::Relaxed);
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
        // Your code here.
        // Example:
        // self.raft.start(command)
        self.raft.lock().unwrap().start(command)
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

    /// the tester calls kill() when a Raft instance won't
    /// be needed again. you are not required to do anything
    /// in kill(), but it might be convenient to (for example)
    /// turn off debug output from this instance.
    pub fn kill(&self) {
        // Your code here, if desired.
        self.shutdown.store(true, Ordering::Relaxed);
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
        let mut reply = RequestVoteReply{
            term: self.term(),
            vote_granted: false,
        };
        if args.term <= self.term() {
            reply.vote_granted = false;
        }
        else {
            reply.vote_granted = true;
            self.set_state(args.term, false, false);
            self.set_votefor(Some(args.candidate_id));
            self.reset_timeout_thread();
        }
        Box::new(futures::future::result(Ok(reply)))
        //unimplemented!()
    }

    fn append_entries(&self, args: RequestEntryArgs) -> RpcFuture<RequestEntryReply> {
        let mut reply = RequestEntryReply {
            term: self.term(),
            success: false,
        };
        if args.term < self.term() {
            my_debug!("error:me[{}:{}] recv [{}:{}]", self.get_id(), self.term(), args.leader_id, args.term);
        }
        else {
            self.set_state(args.term, false, false);
            self.reset_timeout_thread();
            self.set_votefor(None);
            reply.term = self.term();
            reply.success = true;
        }
        Box::new(futures::future::result(Ok(reply)))
    }
}
