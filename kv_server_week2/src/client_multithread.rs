extern crate grpcio;
extern crate lib;
extern crate chrono;

use std::sync::Arc;

use grpcio::{ChannelBuilder, EnvBuilder};

use lib::protos::kvserver::{ResponseStatus, GetRequest, PutRequest, DeleteRequest, ScanRequest};
use lib::protos::kvserver_grpc::KvdbClient;

use std::collections::HashMap;
use std::thread;

use chrono::prelude::*;

pub struct Client {
    pub client: KvdbClient,
}

impl Client {
    pub fn new(host: String, port: u16) -> Self {
        let addr = format!("{}:{}", host, port);
        let env = Arc::new(EnvBuilder::new().build());
        let ch = ChannelBuilder::new(env).connect(addr.as_ref());
        let kv_client = KvdbClient::new(ch);

        Client {
            client: kv_client,
        }
    }
    pub fn get(&self, key: String) -> Option<String> {
        let mut request = GetRequest::new();
        request.set_key(key);
        let ret = self.client.get(&request).expect("RPC failed");
        match ret.status {
            ResponseStatus::kSuccess => Some(ret.value),
            ResponseStatus::kNotFound | ResponseStatus::kFailed | ResponseStatus::kNoType => None
        }
    }
    pub fn put(&self, key: String, value: String) -> bool {
        let mut request = PutRequest::new();
        request.set_key(key);
        request.set_value(value);
        let ret = self.client.put(&request).expect("RPC failed");
        match ret.status {
            ResponseStatus::kSuccess => true,
            ResponseStatus::kNotFound | ResponseStatus::kFailed | ResponseStatus::kNoType => false
        }
    }
    pub fn delete(&self, key: String) -> bool {
        let mut request = DeleteRequest::new();
        request.set_key(key);
        let ret = self.client.delete(&request).expect("RPC failed");
        match ret.status {
            ResponseStatus::kSuccess | ResponseStatus::kNotFound => true,
            ResponseStatus::kFailed | ResponseStatus::kNoType => false
        }
    }
    pub fn scan(&self, key_start: String, key_end: String) -> Option<HashMap<String,String>> {
        let mut request = ScanRequest::new();
        request.set_key_start(key_start);
        request.set_key_end(key_end);
        let ret = self.client.scan(&request).expect("RPC failed");
        match ret.status {
            ResponseStatus::kSuccess => Some(ret.key_value),
            ResponseStatus::kNotFound | ResponseStatus::kFailed | ResponseStatus::kNoType => None
        }

    }
    
}
const THREAD_NUM: u32 = 1000;
const PUT_NUM: u32 = 20;  //每个线程的put数量
const KEY_SIZE: usize = 16;
const VALUE_SIZE: usize = 4096;
const TEST_HOST: &'static str = "127.0.0.1";
const TEST_PORT: u16 = 20001;
fn getkey(k: i32) ->  String {
    format!("{:>0width$}", k, width=KEY_SIZE)
}
fn getvalue(v: i32) -> String {
    format!("{:>0width$}", v, width=VALUE_SIZE)
}
fn main(){

    let mut handles = vec![];
    let start_time = Local::now();

    for i in 0..THREAD_NUM {
        let handle = thread::spawn(move || {
            let client = Client::new(TEST_HOST.clone().to_string(), TEST_PORT);
            for j in 0..PUT_NUM {
                let ret = client.put(getkey((i*THREAD_NUM*PUT_NUM + j) as i32), getvalue((i*THREAD_NUM*PUT_NUM + j) as i32));
                match ret {
                    true => {}
                    false => {
                        println!("{}:{}:put error!", i, j);
                    }
                }
            }
        });
        handles.push(handle);
    }
    
    for handle in handles {
        handle.join().unwrap();
    }
    let end_time = Local::now();
    let use_time = end_time.timestamp_millis() - start_time.timestamp_millis();
    println!("use:{}ms , {:.2}s", use_time, (use_time as f64 ) / 1000.0);
    println!("Thread num:{} , every thread put:{}",THREAD_NUM, PUT_NUM);
    println!("Key size:{} bytes , Value size:{} bytes",KEY_SIZE, VALUE_SIZE);

}