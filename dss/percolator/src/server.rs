use crate::msg::*;
use crate::service::*;
use crate::*;

use std::collections::BTreeMap;
use std::sync::{Arc, Mutex};
use std::time;
use std::time::Duration;

use labrpc::RpcFuture;
use labrpc::*;
// TTL is used for a lock key.
// If the key's lifetime exceeds this value, it should be cleaned up.
// Otherwise, the operation should back off.
const TTL: u64 = Duration::from_millis(100).as_nanos() as u64;

#[derive(Clone)]
pub struct TimestampOracle {
    // You definitions here if needed.
    lock: Arc<Mutex<i32>>,   //获取时间时，需要锁，防止获取相同的时间
}
impl Default for TimestampOracle {
    fn default() -> Self {
        TimestampOracle {
            lock: Arc::new(Mutex::new(0)),
        }
    }
}

impl timestamp::Service for TimestampOracle {
    // example get_timestamp RPC handler.
    fn get_timestamp(&self, _: TimestampRequest) -> RpcFuture<TimestampResponse> {
        // Your code here.
        //unimplemented!()
        let _lock = self.lock.lock().unwrap();  //锁住，原子操作
        let now = time::SystemTime::now();
        let timestamp = TimestampResponse {
            timestamp: now.duration_since(time::UNIX_EPOCH).expect("").as_nanos() as u64,
        };
        Box::new(futures::future::result(Ok(timestamp)))
    }
}

// Key is a tuple (raw key, timestamp).
pub type Key = (Vec<u8>, u64);

#[derive(Clone, PartialEq)]
pub enum Value {
    Timestamp(u64),
    Vector(Vec<u8>),
}
impl Value {
    pub fn get_timestamp(&self) -> u64 {
        match self {
            Value::Timestamp(ts) => *ts,
            _ => {
                panic!("error:use Value error");
            },
        }
    }

    pub fn get_vector(&self) -> Vec<u8> {
        match self {
            Value::Vector(vec) => vec.clone(),
            _ => {
                panic!("error:use Value error");
            },
        }
    }
}

#[derive(Debug, Clone)]
pub struct Write(Vec<u8>, Vec<u8>);

pub enum Column {
    Write,
    Data,
    Lock,
}

// KvTable is used to simulate Google's Bigtable.
// It provides three columns: Write, Data, and Lock.
#[derive(Clone, Default)]
pub struct KvTable {
    write: BTreeMap<Key, Value>,       //实际存储 key_ts  <-> timestamp
    data: BTreeMap<Key, Value>,        //实际存储 key_ts  <-> value 
    lock: BTreeMap<Key, Value>,        //实际存储 key_ts  <-> primary_key
}

impl KvTable {
    // Reads the latest key-value record from a specified column
    // in MemoryStorage with a given key and a timestamp range.
    #[inline]
    fn read(
        &self,
        key: Vec<u8>,
        column: Column,
        ts_start_inclusive: Option<u64>,
        ts_end_inclusive: Option<u64>,
    ) -> Option<(&Key, &Value)> {
        // Your code here.
        //unimplemented!()
        let key_start = match ts_start_inclusive {
            None => (key.clone(), 0),
            Some(ts) => (key.clone(), ts),
        };
        let key_end = match ts_end_inclusive {
            None => (key.clone(), std::u64::MAX),
            Some(ts) => (key.clone(), ts),
        };
        let mut r = match column {
            Column::Write => self.write.range(key_start..=key_end),
            Column::Data => self.data.range(key_start..=key_end),
            Column::Lock => self.lock.range(key_start..=key_end),
        };
        r.next_back()
    }

    // Writes a record to a specified column in MemoryStorage.
    #[inline]
    fn write(&mut self, key: Vec<u8>, column: Column, ts: u64, value: Value) {
        // Your code here.
        //unimplemented!()
        let key_ts = (key, ts);
        match column {
            Column::Write => {
                let _ = self.write.insert(key_ts, value);
            }
            Column::Data => {
                let _ = self.data.insert(key_ts, value);
            }
            Column::Lock => {
                let _ = self.lock.insert(key_ts, value);
            }
        }
    }

    #[inline]
    // Erases a record from a specified column in MemoryStorage.
    fn erase(&mut self, key: Vec<u8>, column: Column, commit_ts: u64) {
        // Your code here.
        //unimplemented!()
        match column {
            Column::Data => {
                let tmp = self.data.clone();
                for (key_ts, _) in tmp.iter() {
                    if key.as_slice() == key_ts.0.as_slice() && key_ts.1 <= commit_ts {
                        let _ = self.data.remove(&key_ts);
                    }
                }
            }
            Column::Lock => {
                let tmp = self.lock.clone();
                for (key_ts, _) in tmp.iter() {
                    if key.as_slice() == key_ts.0.as_slice() && key_ts.1 <= commit_ts {
                        let _ = self.lock.remove(&key_ts);
                    }
                }
            }
            _ => {} //write无需删除
        }
    }
    fn get_primary_lock_keys(&self, ts: u64, primary: Vec<u8>) -> Vec<Key> {  //获取primary相关的所有keys的锁
        let mut keys: Vec<Key> = vec![];
        for (key_ts, v) in self.lock.iter() {
            if v.get_vector() == primary && key_ts.1 == ts {  //匹配
                keys.push(key_ts.clone());
            }
        }
        keys
    }

    fn get_primary_commit_ts(&self, ts: u64, primary: Vec<u8>) -> Option<u64> {  //获取primary_ts的write库的写入
        for (key_ts, v) in self.write.iter() {
            if v.get_timestamp() == ts && key_ts.0 == primary {  //匹配
                return Some(key_ts.1);
            }
        }
        None
    }
}

// MemoryStorage is used to wrap a KvTable.
// You may need to get a snapshot from it.
#[derive(Clone, Default)]
pub struct MemoryStorage {
    pub data: Arc<Mutex<KvTable>>,
}

impl transaction::Service for MemoryStorage {
    // example get RPC handler.
    fn get(&self, req: GetRequest) -> RpcFuture<GetResponse> {
        // Your code here.
        //unimplemented!()
        let key = req.key.clone();
        let snapshot = self.data.lock().unwrap().clone(); //复制kvtable,不一直占用锁

        if snapshot.read(key.clone(), Column::Lock, None, Some(req.start_ts)).is_some() //该key在start_ts之前锁住
        {
            //乐观锁，尝试是否需要清除锁
            self.back_off_maybe_clean_up_lock(req.start_ts, key.clone());
            return Box::new(futures::future::result(Err(Error::Other(String::from("error:back off")))));
        }

        let data_ts = match snapshot.read(key.clone(), Column::Write, None, Some(req.start_ts)) {  //
            Some(kv) => kv.1.get_timestamp(),
            None => {  //未找到，空value
                let reply = GetResponse {
                    value: vec![],
                };
                return Box::new(futures::future::result(Ok(reply)));
            }
        };
        let value = match snapshot.read(key.clone(), Column::Data, Some(data_ts), Some(data_ts)) {
            Some(kv) => kv.1.get_vector(),
            None => {  //未找到Data,
                let reply = GetResponse {
                    value: vec![],
                };
                return Box::new(futures::future::result(Ok(reply)));
            }
        };
        let reply = GetResponse {
            value: value.clone(),
        };
        Box::new(futures::future::result(Ok(reply)))

    }

    // example prewrite RPC handler.
    fn prewrite(&self, req: PrewriteRequest) -> RpcFuture<PrewriteResponse> {
        // Your code here.
        //unimplemented!()
        let mut data = self.data.lock().unwrap();

        if data.read(req.w_key.clone(), Column::Write, Some(req.start_ts), None).is_some() {  //出现写入冲突
            
            return Box::new(futures::future::result(Err(Error::Other(String::from("error:write conflict")))));
        }

        if data.read(req.w_key.clone(), Column::Lock, None, None,).is_some(){
            // key被lock
            return Box::new(futures::future::result(Err(Error::Other(String::from("error:key lock")))));
        }

        //存入数据，并存入lock
        data.write(req.w_key.clone(), Column::Data, req.start_ts, Value::Vector(req.w_value.clone())); 
        data.write(req.w_key.clone(), Column::Lock, req.start_ts, Value::Vector(req.primary_key.clone()));

        let reply = PrewriteResponse {};
        Box::new(futures::future::result(Ok(reply)))
    }

    // example commit RPC handler.
    fn commit(&self, req: CommitRequest) -> RpcFuture<CommitResponse> {
        // Your code here.
        //unimplemented!()
        let mut data = self.data.lock().unwrap();


        if data.read(req.write_key.clone(), Column::Lock, Some(req.start_ts), Some(req.start_ts)).is_none()
        {  //primary的锁不在了，
            if data.read(req.write_key.clone(), Column::Write, Some(req.commit_ts), Some(req.commit_ts)).is_some() { 
                //如果读取到该数据commit_ts已经写入，直接返回成功,防止重复写入
                return Box::new(futures::future::result(Ok(CommitResponse {})));
            }
            return Box::new(futures::future::result(Err(Error::Other(String::from("error:unlock")))));
        }

        if !req.is_primary && data.read(req.primary_value.clone(), Column::Write, Some(req.commit_ts), Some(req.commit_ts)).is_none() {
            //如果secondaries发现primary没有写入，直接返回失败
            return Box::new(futures::future::result(Err(Error::Other(String::from("error:primary not write error")))));
        }

        //写入write，并清除锁，如果
        data.write(req.write_key.clone(), Column::Write, req.commit_ts, Value::Timestamp(req.start_ts));
        data.erase(req.write_key.clone(), Column::Lock, req.start_ts);

        Box::new(futures::future::result(Ok(CommitResponse {})))
    }
}

impl MemoryStorage {
    fn back_off_maybe_clean_up_lock(&self, start_ts: u64, key: Vec<u8>) {
        // Your code here.
        //unimplemented!()
        let mut data = self.data.lock().unwrap();
        if let Some((key_ts, value)) = data.read(key.clone(), Column::Lock, None, Some(start_ts)) {  //锁还在
            let now = time::SystemTime::now();
            let current_ts = now.duration_since(time::UNIX_EPOCH).expect("").as_nanos() as u64;
            if current_ts - key_ts.1 > TTL {  //该锁锁住的时间超过TTL
                let primary = value.get_vector();
                let ts = key_ts.1;
                if data.read(primary.clone(), Column::Lock, Some(ts), Some(ts)).is_some()  //查看primary有没有提交
                {  //在，说明primary没有提交到write，放弃该事务
                    let uncommitted_keys = data.get_primary_lock_keys(ts, primary.clone());
                    for k in uncommitted_keys {   //清除primary事务的所有key的锁和数据
                        data.erase(k.0.clone(), Column::Data, ts);
                        data.erase(k.0.clone(), Column::Lock, ts);
                    }
                } else {  //不在，说明primary已经提交，但是secondaries未提交完成，帮助该事务的secondaries提交
                    let uncommitted_keys = data.get_primary_lock_keys(ts, primary.clone());
                    let commit_ts = data.get_primary_commit_ts(ts, primary).unwrap();

                    for k in uncommitted_keys {  //
                        data.write(k.0.clone(), Column::Write, commit_ts, Value::Timestamp(ts));
                        data.erase(k.0.clone(), Column::Lock, commit_ts);
                    }
                }
                return;
            }
        }
    }
}
