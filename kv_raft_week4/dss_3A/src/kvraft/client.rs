use std::fmt;

use super::service;
use futures::Future;
//use super::errors;

use std::sync::{Arc, Mutex};
use std::thread;
use std::time::Duration;

enum Op {
    Put(String, String),
    Append(String, String),
}

pub struct Clerk {
    pub name: String,
    servers: Vec<service::KvClient>,
    // You will have to modify this struct.
    op_id: Arc<Mutex<u64>>,     //操作的id，从1开始
    leader_id: Arc<Mutex<u64>>, //leader的id，默认为0,是否需要Arc有待思考，很可能影响多线程
}

impl fmt::Debug for Clerk {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Clerk").field("name", &self.name).finish()
    }
}

impl Clerk {
    pub fn new(name: String, servers: Vec<service::KvClient>) -> Clerk {
        // You'll have to add code here.
        Clerk {
            name,
            servers,
            op_id: Arc::new(Mutex::new(0)),
            leader_id: Arc::new(Mutex::new(0)),
        }
    }

    pub fn get_seq(&self) -> u64 {
        let mut op_id = self.op_id.lock().unwrap();
        *op_id += 1;
        *op_id
    }

    /// fetch the current value for a key.
    /// returns "" if the key does not exist.
    /// keeps trying forever in the face of all other errors.
    //
    // you can send an RPC with code like this:
    // let reply = self.servers[i].get(args).unwrap();
    pub fn get(&self, key: String) -> String {
        // You will have to modify this function.
        let args = service::GetRequest {
            key: key,
            seq: self.get_seq(), //seq += 1;
            client_name: self.name.clone(),
        };
        let mut leader = *self.leader_id.lock().unwrap();
        loop {
            let ret = self.servers[leader as usize].get(&args.clone()).wait();
            match ret {
                Ok(reply) => {
                    //println!("clerk:{:?}", reply);
                    if reply.err == "OK" {
                        //成功提交，并返回
                        if !reply.wrong_leader {
                            *self.leader_id.lock().unwrap() = leader;
                        }
                        return reply.value;
                    }
                    if !reply.wrong_leader {
                        //正常leader,
                        *self.leader_id.lock().unwrap() = leader;
                    //thread::sleep(Duration::from_millis(100)); //等成功提交
                    } else {
                        //错误leader
                        leader = (leader + 1) % self.servers.len() as u64;
                    }
                }
                Err(_) => {
                    println!("clerk:");
                }
            }
            //leader = (leader + 1) % self.servers.len();
            thread::sleep(Duration::from_millis(20));
        }

        //unimplemented!()
    }

    /// shared by Put and Append.
    //
    // you can send an RPC with code like this:
    // let reply = self.servers[i].put_append(args).unwrap();
    fn put_append(&self, op: Op) {
        // You will have to modify this function.
        let mut key;
        let mut value;
        let for_op;
        let seq = self.get_seq();

        match op {
            Op::Put(k, v) => {
                key = k;
                value = v;
                for_op = service::Op::Put;
            }
            Op::Append(k, v) => {
                key = k;
                value = v;
                for_op = service::Op::Append;
            }
        }

        let args = service::PutAppendRequest {
            key: key,
            value: value,
            op: for_op as i32,
            seq: seq,
            client_name: self.name.clone(),
        };
        let mut leader = *self.leader_id.lock().unwrap();
        loop {
            let ret = self.servers[leader as usize]
                .put_append(&args.clone())
                .wait();
            match ret {
                Ok(reply) => {
                    //println!("clerk:{:?}", reply);
                    if reply.err == "OK" {
                        //成功提交，并返回
                        if !reply.wrong_leader {
                            *self.leader_id.lock().unwrap() = leader;
                        }
                        return;
                    }
                    if !reply.wrong_leader {
                        //正常leader
                        *self.leader_id.lock().unwrap() = leader;
                    //thread::sleep(Duration::from_millis(100)); //等成功提交
                    } else {
                        //错误leader
                        leader = (leader + 1) % self.servers.len() as u64;
                    }
                }
                Err(e) => {
                    println!("clerk:{:?}", e);
                }
            }
            //leader = (leader + 1) % self.servers.len();
            thread::sleep(Duration::from_millis(120));
        }

        //unimplemented!()
    }

    pub fn put(&self, key: String, value: String) {
        self.put_append(Op::Put(key, value))
    }

    pub fn append(&self, key: String, value: String) {
        self.put_append(Op::Append(key, value))
    }
}
