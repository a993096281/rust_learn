
use grpcio::Environment;


use crate::protos::proxy::KvPair;
use crate::{Key, Value, Result};
use crate::raw::{PdClient, KvClient};

use std::collections::HashMap;
use std::sync::{Arc, RwLock};

const MAX_RAW_KV_SCAN_LIMIT: u32 = 10240;   //最大raw_sacn的limit

#[macro_export]
macro_rules! my_debug {
    ($($arg: tt)*) => (
        println!("Debug[{}:{}]: {}", file!(), line!(),format_args!($($arg)*));
    )
}

#[derive(Clone)]
pub struct RawProxy{
    pd: Arc<PdClient>,
    tikv: Arc<RwLock<HashMap<String, Arc<KvClient>>>>,   // ip:port <-> KvClient
    env: Arc<Environment>,
}

impl RawProxy {
    pub fn get(&self, key: Key) -> Result<Value> {
        let mut raw_context;
        match self.pd.creat_raw_context(key.clone()) {  //向pd获取key的raw_context
            Ok(res) => {
                raw_context = res;
            },
            Err(e) => {
                return Err(e);
            },
        }
        let mut kvclient;
        match self.get_kv_client(raw_context.address()) {  //获取kvclient
            Ok(res) => {
                kvclient = res;
            },
            Err(e) => {
                return Err(e);
            }
        }
        match kvclient.raw_get(raw_context, key.clone()) {  //向kvclient发送请求
            Ok(res) => {
                return Ok(res);
            },
            Err(e) => {
                return Err(e);
            }
        }
    }
    pub fn put(&self, key: Key, value: Value) -> Result<()> {
        let mut raw_context;
        match self.pd.creat_raw_context(key.clone()) {  //向pd获取key的raw_context
            Ok(res) => {
                raw_context = res;
            },
            Err(e) => {
                return Err(e);
            },
        }
        let mut kvclient;
        match self.get_kv_client(raw_context.address()) {  //获取kvclient
            Ok(res) => {
                kvclient = res;
            },
            Err(e) => {
                return Err(e);
            }
        }
        match kvclient.raw_put(raw_context, key.clone(), value.clone()) {  //向kvclient发送请求
            Ok(_) => {
                return Ok(());
            },
            Err(e) => {
                return Err(e);
            }
        }
    }
    pub fn delete(&self, key: Key) -> Result<()> {
        let mut raw_context;
        match self.pd.creat_raw_context(key.clone()) {  //向pd获取key的raw_context
            Ok(res) => {
                raw_context = res;
            },
            Err(e) => {
                return Err(e);
            },
        }
        let mut kvclient;
        match self.get_kv_client(raw_context.address()) {  //获取kvclient
            Ok(res) => {
                kvclient = res;
            },
            Err(e) => {
                return Err(e);
            }
        }
        match kvclient.raw_delete(raw_context, key.clone()) {  //向kvclient发送请求
            Ok(_) => {
                return Ok(());
            },
            Err(e) => {
                return Err(e);
            }
        }
    }
    pub fn scan(&self, key_start: Key, key_end: Key, limit: u32) -> Result<Vec<KvPair>> {
        //key_start为空，代表最小，key_end为空，代表最大
        //region的start_key和end_key有可能为空，
        //grpc包不能传太大，不然grpc会报错，默认似乎是4MB，
        let mut result: Vec<KvPair> = vec![];    
        let limit = if limit > MAX_RAW_KV_SCAN_LIMIT {  //对limit进行限制
            MAX_RAW_KV_SCAN_LIMIT
        }
        else {
            limit
        };
        let mut need_limit = limit;
        let mut need_key_start: Key = key_start.clone();
        loop {   //循环scan，可能向多个region请求
            
            let mut raw_context;
            match self.pd.creat_raw_context(need_key_start.clone()) {  //向pd获取key的raw_context
                Ok(res) => {
                    raw_context = res;
                },
                Err(e) => {
                    return Err(e);
                },
            }
            let mut kvclient;
            match self.get_kv_client(raw_context.address()) {  //获取kvclient
                Ok(res) => {
                    kvclient = res;
                },
                Err(e) => {
                    return Err(e);
                }
            }
            match kvclient.raw_scan(raw_context.clone(), need_key_start.clone(), key_end.clone(), need_limit) {  //向kvclient发送请求
                Ok(mut kvs) => {
                    need_limit = need_limit - kvs.len() as u32;  //limit更新
                    result.append(&mut kvs);
                    if need_limit == 0 || raw_context.end_key().is_empty() || key_end <= raw_context.end_key() {
                        //limit 为0，或没有下一个region了， 或key_end到了,可返回ok
                        return Ok(result);
                    }
                    need_key_start = raw_context.end_key().clone(); //更新key_start,继续下一个regioncan
                    
                },
                Err(e) => {
                    return Err(e);
                }
            }
        }
        
    }
    pub fn connect(endpoint: String, env: Arc<Environment>) -> Result<RawProxy> {
        let pd = match PdClient::connect(endpoint.clone(), env.clone()) {
            Ok(p) => { p },
            Err(e) => {
                return Err(e);
            },
        };
        let raw_proxy = RawProxy {
            pd: Arc::new(pd),
            tikv: Arc::new(RwLock::new(HashMap::new())),
            env,
        };
        Ok(raw_proxy)
    }
    fn get_kv_client(&self, address: String) -> Result<Arc<KvClient>> {
        if let Some(kvclient) = self.tikv.read().unwrap().get(&address) {
            return Ok(Arc::clone(kvclient));
        };
        let mut kvclient;
        match KvClient::connect(address.clone(), self.env.clone()) {
            Ok(kv) => {
                kvclient = Arc::new(kv);
            },
            Err(e) => {
                return Err(e);
            },
        }
        self.tikv.write().unwrap().insert(address.clone(), kvclient.clone());
        Ok(kvclient)
    }

}
#[cfg(test)]
mod tests {
    use super::*;
    use grpcio::EnvBuilder;
    #[test]
    fn test_rawproxy() {
        let env = Arc::new(
            EnvBuilder::new()
                .cq_count(1)       // 设置队列深度和poll线程
                .name_prefix("tikv_raw_proxy_grpc")   //设置线程名称
                .build(),
        );
        let db = RawProxy::connect("127.0.0.1:2379".to_string(), env.clone()).unwrap();
        
        let key = "bbb".to_string().into_bytes();
        let value = "bbb".to_string().into_bytes();

        println!("put:{:?}", db.put(key.clone(), value.clone()));
        println!("get:{:?}", db.get(key.clone()));
        println!("get:{:?}", db.get("aaa2".to_string().into_bytes()));

        println!("delete:{:?}", db.delete(key.clone()));
        println!("get:{:?}", db.get(key.clone()));
        
    }

    fn getvalue(size: usize) -> String {  //生成长度为size+1的value
        format!("{:>0width$}", 1, width=size)
    }
    #[test]
    fn test_put_more() {
        let env = Arc::new(
            EnvBuilder::new()
                .cq_count(1)       // 设置队列深度和poll线程
                .name_prefix("tikv_raw_proxy_grpc")   //设置线程名称
                .build(),
        );
        let db = RawProxy::connect("127.0.0.1:2379".to_string(), env.clone()).unwrap();
        let mut _ok = 0;
        for i in 0..1000 {
            let key = format!("e{}", i);
            let value = getvalue(4096); //4k value
            let _ret = db.put(key.clone().into_bytes(), value.clone().into_bytes());
            if _ret.is_ok() {
                _ok +=1;
            }
        }
        println!("put ok:{}", _ok);
    }
    #[test]
    fn test_scan() {
        let env = Arc::new(
            EnvBuilder::new()
                .cq_count(1)       // 设置队列深度和poll线程
                .name_prefix("tikv_raw_proxy_grpc")   //设置线程名称
                .build(),
        );
        let db = RawProxy::connect("127.0.0.1:2379".to_string(), env.clone()).unwrap();
        let key1 = "d5080".to_string().into_bytes();
        let key2 = "f".to_string().into_bytes();
        println!("scan:{:?}", db.scan(key1.clone(), key2.clone(), 10));
    }
}
