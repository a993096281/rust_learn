use super::service::*;
use crate::raft;
use futures::stream::Stream;
use futures::sync::mpsc::{unbounded, UnboundedReceiver};
use futures::Async;
use labrpc::RpcFuture;
use std::thread;

use std::collections::HashMap;
//use std::sync::mpsc;
//se std::sync::mpsc::Sender;
use std::sync::{Arc, Mutex};
//use std::time::Duration;
#[macro_export]
macro_rules! kv_debug {
    ($($arg: tt)*) => (
        println!("Debug[{}:{}]: {}", file!(), line!(),format_args!($($arg)*));
    )
}

const WAIT_CHECK_TIME: u64 = 100; //单位秒

#[derive(Clone, PartialEq, Debug)]
pub struct LatestReply {
    pub seq: u64,      //请求seq
    pub result: i32,   // -1代表失效，0代表等待raft，1代表成功
    pub value: String, //get操作时的结果
}

#[derive(Clone, PartialEq, Message)]
pub struct OpEntry {
    #[prost(uint64, tag = "1")]
    pub seq: u64, //操作的id
    #[prost(string, tag = "2")]
    pub client_name: String, //
    #[prost(uint64, tag = "3")]
    pub op: u64, //1代表get，2代表put，3代表append
    #[prost(string, tag = "4")]
    pub key: String, //
    #[prost(string, tag = "5")]
    pub value: String, //
}

pub struct KvServer {
    pub rf: raft::Node,
    pub me: usize,
    // snapshot if log grows this big
    pub maxraftstate: Option<usize>,
    // Your definitions here.
    pub apply_ch: UnboundedReceiver<raft::ApplyMsg>,

    pub db: HashMap<String, String>,                   //数据库存储
    pub ack: HashMap<u64, (String, u64)>,                // 日志index <-> (client_name,seq)
    pub latest_requests: HashMap<String, HashMap<u64, LatestReply>>, //client_name <-> (请求seq <-> 回复;对应客户端的最新回复)
}

impl KvServer {
    pub fn new(
        servers: Vec<raft::service::RaftClient>,
        me: usize,
        persister: Box<dyn raft::persister::Persister>,
        maxraftstate: Option<usize>,
    ) -> KvServer {
        // You may need initialization code here.

        let (tx, apply_ch) = unbounded();
        let rf = raft::Raft::new(servers, me, persister, tx);

        KvServer {
            me,
            maxraftstate,
            rf: raft::Node::new(rf),
            apply_ch,
            db: HashMap::new(),
            ack: HashMap::new(),
            latest_requests: HashMap::new(),
        }
    }
    pub fn get_state(&self) -> raft::State {
        self.rf.get_state()
    }
}

// Choose concurrency paradigm.
//
// You can either drive the kv server by the rpc framework,
//
// ```rust
// struct Node { server: Arc<Mutex<KvServer>> }
// ```
//
// or spawn a new thread runs the kv server and communicate via
// a channel.
//
// ```rust
// struct Node { sender: Sender<Msg> }
// ```
#[derive(Clone)]
pub struct Node {
    // Your definitions here.
    pub me: usize,
    pub server: Arc<Mutex<KvServer>>,
}

impl Node {
    pub fn new(kv: KvServer) -> Node {
        // Your code here.
        let node = Node {
            me: kv.me,
            server: Arc::new(Mutex::new(kv)),
        };
        let inode = node.clone();
        /*let apply_ch = node.server.lock().unwrap().apply_ch.clone();
        apply_ch.for_each(move |cmd: raft::ApplyMsg| {
            kv_debug!("cmd:{:?}", cmd);

        })
        .map_err(move |e| println!("error: apply stopped: {:?}", e));*/
        thread::spawn(move || {
            loop {
                if let Ok(Async::Ready(Some(apply_msg))) =
                    futures::executor::spawn(futures::lazy(|| {
                        inode.server.lock().unwrap().apply_ch.poll()
                    }))
                    .wait_future()
                {
                    if !apply_msg.command_valid {
                        continue;
                    }
                    let command_index = apply_msg.command_index;
                    let mut _entr: OpEntry;
                    match labcodec::decode(&apply_msg.command) {
                        Ok(en) => {
                            _entr = en;
                        }
                        Err(e) => {
                            kv_debug!("error:decode error");
                            continue;
                        }
                    }
                    kv_debug!(
                        "server:{} command_index:{} {:?}",
                        inode.get_id(),
                        command_index,
                        _entr
                    );
                    let mut server = inode.server.lock().unwrap();
                    if server.latest_requests.get(&_entr.client_name).is_none()
                        || server.latest_requests.get(&_entr.client_name).unwrap().get(&_entr.seq).is_none()
                        || server.latest_requests.get(&_entr.client_name).unwrap().get(&_entr.seq).unwrap().result != 1
                    {
                        //可更新
                        let mut lre = LatestReply {
                            seq: _entr.seq,
                            result: 1,
                            value: String::new(),
                        };
                        match _entr.op {
                            1 => {
                                //get
                                if server.db.get(&_entr.key).is_some() {
                                    lre.value = server.db.get(&_entr.key).unwrap().clone();
                                }
                                if server.latest_requests.get(&_entr.client_name).is_none() {
                                    server.latest_requests.insert(_entr.client_name.clone(), HashMap::new());
                                }
                                server.latest_requests.get_mut(&_entr.client_name).unwrap().insert(_entr.seq, lre.clone());
                                kv_debug!("server:{} client:{} apply:{:?}", server.me, _entr.client_name, lre);
                            }
                            2 => {
                                //put
                                server.db.insert(_entr.key.clone(), _entr.value.clone());
                                if server.latest_requests.get(&_entr.client_name).is_none() {
                                    server.latest_requests.insert(_entr.client_name.clone(), HashMap::new());
                                }
                                server.latest_requests.get_mut(&_entr.client_name).unwrap().insert(_entr.seq, lre.clone());
                                kv_debug!("server:{} client:{} apply:{:?}", server.me, _entr.client_name, lre);
                            }
                            3 => {
                                //append
                                if server.db.get(&_entr.key).is_some() {
                                    server.db.get_mut(&_entr.key).unwrap().push_str(&_entr.value.clone());
                                } else {
                                    server.db.insert(_entr.key.clone(), _entr.value.clone());
                                }
                                if server.latest_requests.get(&_entr.client_name).is_none() {
                                    server.latest_requests.insert(_entr.client_name.clone(), HashMap::new());
                                }
                                server.latest_requests.get_mut(&_entr.client_name).unwrap().insert(_entr.seq, lre.clone());
                                kv_debug!("server:{} client:{} apply:{:?}", server.me, _entr.client_name, lre);
                            }
                            _ => {
                                //error
                                kv_debug!("error:apply op error");
                                continue;
                            }
                        }
                    }
                    let mut client_name = _entr.client_name.clone();
                    let mut seq = _entr.seq;
                    if server.ack.get(&command_index).is_some() {
                        client_name = server.ack.get(&command_index).unwrap().0.clone();
                        seq = server.ack.get(&command_index).unwrap().1;
                    }
                    if client_name != _entr.client_name {
                        server.latest_requests.get_mut(&client_name.clone()).unwrap().get_mut(&seq).unwrap().result = -1;
                    }
                    server.ack.remove(&command_index);
                    /*if let Some(client_name) = server.ack.get(&command_index) {
                        //需要校验
                        /*if *client_name != _entr.client_name {
                            server.latest_requests.get_mut(&client_name.clone()).unwrap().result = -1;
                        }*/
                        
                        server.ack.remove(&command_index);
                    }*/
                }
            }
            /*while let Ok(Async::Ready(Some(apply_msg))) =
                futures::executor::spawn(futures::lazy(|| inode.server.lock().unwrap().apply_ch.poll())).wait_future() {
                    kv_debug!("cmd:{:?}", apply_msg);
            }*/
        });
        node
    }

    /// the tester calls Kill() when a KVServer instance won't
    /// be needed again. you are not required to do anything
    /// in Kill(), but it might be convenient to (for example)
    /// turn off debug output from this instance.
    pub fn kill(&self) {
        // Your code here, if desired.
        self.server.lock().unwrap().rf.kill();
    }

    /// The current term of this peer.
    pub fn term(&self) -> u64 {
        self.get_state().term()
    }

    /// Whether this peer believes it is the leader.
    pub fn is_leader(&self) -> bool {
        self.get_state().is_leader()
    }

    pub fn get_state(&self) -> raft::State {
        // Your code here.
        self.server.lock().unwrap().get_state()
    }
    pub fn get_id(&self) -> usize {
        self.me
    }
}

impl KvService for Node {
    fn get(&self, arg: GetRequest) -> RpcFuture<GetReply> {
        // Your code here.
        kv_debug!("server:{} get:{:?}", self.get_id(), arg);
        let mut reply = GetReply {
            wrong_leader: true,
            err: String::new(),
            value: String::new(),
        };
        if !self.is_leader() {
            return Box::new(futures::future::result(Ok(reply)));
        }
        //let (send, recv) = mpsc::channel();
        //let index;
        //let term;
        {
            //锁住
            //kv_debug!("server:{} get for lock()", self.get_id());
            let mut server = self.server.lock().unwrap();
            //kv_debug!("server:{} get lock()", self.get_id());
            //thread::sleep(Duration::from_millis(300));
            if server.latest_requests.get(&arg.client_name).is_some() {
                //该客户端的回复
                let hashmap = server.latest_requests.get_mut(&arg.client_name).unwrap();
                if let Some(re) = hashmap.get(&arg.seq) {
                    if re.result == 1 {  //可返回结果
                        reply.wrong_leader = false;
                        reply.err = String::from("OK");
                        reply.value = re.value.clone();
                        kv_debug!("server:{} client:{} get reply:{:?}", self.get_id(), arg.client_name, reply);
                        return Box::new(futures::future::result(Ok(reply)));
                    }
                    else if re.result == 0 { //等待raft
                        reply.wrong_leader = false;
                        
                        kv_debug!("server:{} client:{} get wait reply:{:?}", self.get_id(), arg.client_name, reply);
                        return Box::new(futures::future::result(Ok(reply)));
                    }
                }
                
            }
            let cmd = OpEntry {
                seq: arg.seq,
                client_name: arg.client_name.clone(),
                op: 1,
                key: arg.key.clone(),
                value: String::new(),
            };
            match server.rf.start(&cmd) {
                //发送log
                Ok((index1, term1)) => {  //发送成功
                    //index = index1;
                    //term = term1;
                    //server.ack.entry(index).or_insert(send);
                    server.ack.insert(index1, (arg.client_name.clone(), arg.seq));
                    let lre = LatestReply {
                            seq: arg.seq,
                            result: 0,
                            value: String::new(),
                    };
                    if server.latest_requests.get(&arg.client_name).is_none() {
                        server.latest_requests.insert(arg.client_name.clone(), HashMap::new());
                    }
                    server.latest_requests.get_mut(&arg.client_name).unwrap().insert(arg.seq, lre.clone());
                    reply.wrong_leader = false;
                    kv_debug!("server:{} client:{} start:{:?}", self.get_id(), arg.client_name, cmd);
                    //kv_debug!("server:{} client:{} get start reply:{:?}", self.get_id(), arg.client_name, reply);
                    return Box::new(futures::future::result(Ok(reply)));

                }
                Err(_) => {
                    reply.wrong_leader = true;
                    reply.err = String::from("");
                    return Box::new(futures::future::result(Ok(reply)));
                }
            }
            //kv_debug!("server:{} client:{} start:{:?}", self.get_id(), arg.client_name, cmd);
        }
        //kv_debug!("server:{} get unlock()", self.get_id());
        /*if recv
            .recv_timeout(Duration::from_secs(WAIT_CHECK_TIME))
            .is_ok()
        {
            //收到回复
            {
                //锁
                kv_debug!("server:{} recv get lock()", self.get_id());
                let mut server = self.server.lock().unwrap();
                if let Some(re) = server.latest_requests.get(&arg.client_name) {
                    //该客户端的回复
                    if arg.seq == re.seq {
                        //
                        reply.wrong_leader = false;
                        reply.err = String::from("OK");
                        reply.value = re.value.clone();
                        server.ack.remove(&index);
                        kv_debug!("server:{} get reply:{:?}", self.get_id(), reply);
                        return Box::new(futures::future::result(Ok(reply)));
                    } else {
                        kv_debug!(
                            "kverror:server:{} arg.seq:{} re.seq:{}",
                            server.me,
                            arg.seq,
                            re.seq
                        );
                    }
                }
            }
        } else {
            //超时
            kv_debug!("kverror:time out arg:{:?}", arg);
            let mut server = self.server.lock().unwrap();
            server.ack.remove(&index);
        }*/
        //reply.wrong_leader = true;
        //reply.err = String::from("");
        //return Box::new(futures::future::result(Ok(reply)));
        //unimplemented!()
    }

    fn put_append(&self, arg: PutAppendRequest) -> RpcFuture<PutAppendReply> {
        // Your code here.
        kv_debug!("server:{} putappend:{:?}", self.get_id(), arg);
        let mut reply = PutAppendReply {
            wrong_leader: true,
            err: String::new(),
        };
        if !self.is_leader() {
            return Box::new(futures::future::result(Ok(reply)));
        }
        //let (send, recv) = mpsc::channel();
        //let index;
        //let term;
        {
            //锁住
            //kv_debug!("server:{} putappend for lock()", self.get_id());
            let mut server = self.server.lock().unwrap();
            //kv_debug!("server:{} putappend lock()", self.get_id());
            //thread::sleep(Duration::from_millis(300));
            if server.latest_requests.get(&arg.client_name).is_some() {
                //该客户端的回复
                let hashmap = server.latest_requests.get_mut(&arg.client_name).unwrap();
                if let Some(re) = hashmap.get(&arg.seq) {
                    if re.result == 1{
                        //可直接返回结果
                        reply.wrong_leader = false;
                        reply.err = String::from("OK");
                        kv_debug!("server:{} client:{} putappend reply:{:?}", self.get_id(), arg.client_name, reply);
                        return Box::new(futures::future::result(Ok(reply)));
                    }
                    else if re.result == 0 {
                        reply.wrong_leader = false;
                        
                        kv_debug!("server:{} client:{} putappend wait reply:{:?}", self.get_id(), arg.client_name, reply);
                        return Box::new(futures::future::result(Ok(reply)));
                    }
                }

            }
            let cmd = OpEntry {
                seq: arg.seq,
                client_name: arg.client_name.clone(),
                op: (arg.op + 1) as u64,
                key: arg.key.clone(),
                value: arg.value.clone(),
            };
            match server.rf.start(&cmd) {
                //发送log
                Ok((index1, term1)) => {
                    //index = index1;
                    //term = term1;
                    //server.ack.entry(index).or_insert(send);
                    //server.ack.insert(index, send);
                    server.ack.insert(index1, (arg.client_name.clone(), arg.seq));
                    let lre = LatestReply {
                            seq: arg.seq,
                            result: 0,
                            value: String::new(),
                    };
                    if server.latest_requests.get(&arg.client_name).is_none() {
                        server.latest_requests.insert(arg.client_name.clone(), HashMap::new());
                    }
                    server.latest_requests.get_mut(&arg.client_name).unwrap().insert(arg.seq, lre.clone());
                    reply.wrong_leader = false;
                    kv_debug!("server:{} client:{} start:{:?}", self.get_id(), arg.client_name, cmd);
                    //kv_debug!("server:{} client:{} get start reply:{:?}", self.get_id(), arg.client_name, reply);
                    return Box::new(futures::future::result(Ok(reply)));

                }
                Err(_) => {
                    reply.wrong_leader = true;
                    reply.err = String::from("");
                    return Box::new(futures::future::result(Ok(reply)));
                }
            }
            //kv_debug!("server:{} start:{:?}", self.get_id(), cmd);
        }
        //kv_debug!("server:{} putappend unlock()", self.get_id());
        /*if recv.recv_timeout(Duration::from_secs(WAIT_CHECK_TIME)).is_ok()
        {
            //收到回复
            {
                //锁
                kv_debug!("server:{} recv putappend lock()", self.get_id());
                let mut server = self.server.lock().unwrap();
                if let Some(re) = server.latest_requests.get(&arg.client_name) {
                    //该客户端的回复
                    if arg.seq == re.seq {
                        //
                        reply.wrong_leader = false;
                        reply.err = String::from("OK");
                        server.ack.remove(&index);
                        kv_debug!("server:{} putappend reply:{:?}", self.get_id(), reply);
                        return Box::new(futures::future::result(Ok(reply)));
                    } else {
                        kv_debug!(
                            "kverror:server:{} arg.seq:{} re.seq:{}",
                            server.me,
                            arg.seq,
                            re.seq
                        );
                    }
                }
            }
        } else {
            //超时
            kv_debug!("kverror:time out arg:{:?}", arg);
            let mut server = self.server.lock().unwrap();
            server.ack.remove(&index);
        }*/
        //reply.wrong_leader = true;
        //reply.err = String::from("");
        //return Box::new(futures::future::result(Ok(reply)));

        //unimplemented!()
    }
}