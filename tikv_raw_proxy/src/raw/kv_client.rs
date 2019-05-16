
use kvproto::{errorpb, kvrpcpb, tikvpb_grpc::TikvClient};
use grpcio::{ChannelBuilder, EnvBuilder};
use grpcio::Environment;

use crate::{Key, Value, Result};
use crate::raw::RawContext;

use std::sync::{Arc, RwLock};

macro_rules! raw_request {
    ($context:expr, $type:ty) => {{
        let mut req = <$type>::new();
        req.set_context($context.into());
        req
    }};
}

#[derive(Clone)]
pub struct KvClient{
    kvclient: TikvClient,
    address: String,
}
impl KvClient {
    pub fn connect(endpoint: String, env: Arc<Environment>) -> Result<KvClient> {
        let ch = ChannelBuilder::new(env).connect(endpoint.clone().as_ref());
        let kvclient = TikvClient::new(ch);
        
        let kv = KvClient {
            kvclient,
            address: endpoint,
        };
        Ok(kv)
    }
    pub fn raw_get(&self, context: RawContext, key: Key) -> Result<Value> {
        let mut req = raw_request!(context, kvrpcpb::RawGetRequest);
        req.set_key(key.clone());

        match self.kvclient.raw_get(&req) {
            Ok(resp) => {  
                if !resp.get_region_error().get_message().is_empty() || !resp.get_error().is_empty() { //存在错误
                    my_debug!("{:?}", resp);
                    return Err(self.handle_error(resp.get_region_error().clone(), resp.get_error().clone().to_string()));
                }
                //my_debug!("{:?}", resp);
                return Ok(resp.get_value().clone().to_vec());
            },
            Err(e) => {
                return Err(e.to_string());
            }
        }

    }
    pub fn raw_put(&self, context: RawContext, key: Key, value: Value) -> Result<()> {
        let mut req = raw_request!(context, kvrpcpb::RawPutRequest);
        req.set_key(key.clone());
        req.set_value(value.clone());

        match self.kvclient.raw_put(&req) {
            Ok(resp) => {  
                if !resp.get_region_error().get_message().is_empty() || !resp.get_error().is_empty() { //存在错误
                    my_debug!("{:?}", resp);
                    return Err(self.handle_error(resp.get_region_error().clone(), resp.get_error().clone().to_string()));
                }
                
                //println!("{:?}", resp);
                return Ok(());
            },
            Err(e) => {
                return Err(e.to_string());
            }
        }
    }
    pub fn raw_delete(&self, context: RawContext, key: Key) -> Result<()> {
        let mut req = raw_request!(context, kvrpcpb::RawDeleteRequest);
        req.set_key(key.clone());

        match self.kvclient.raw_delete(&req) {
            Ok(resp) => {  
                if !resp.get_region_error().get_message().is_empty() || !resp.get_error().is_empty() { //存在错误
                    my_debug!("{:?}", resp);
                    return Err(self.handle_error(resp.get_region_error().clone(), resp.get_error().clone().to_string()));
                }
                //println!("{:?}", resp);
                return Ok(());
            },
            Err(e) => {
                return Err(e.to_string());
            }
        }
    }
    pub fn raw_scan(&self, context: RawContext, key_start: Key, key_end: Key, limit: u32) -> Result<()> {
        let mut req = raw_request!(context, kvrpcpb::RawScanRequest);
        req.set_start_key(key_start.clone());
        req.set_limit(limit);
        req.set_end_key(key_end.clone());

        match self.kvclient.raw_scan(&req) {
            Ok(resp) => {  
                my_debug!("raw_scan {}", resp.get_kvs().len());
                //my_debug!("raw_scan {:?}", resp);
                if !resp.get_region_error().get_message().is_empty() { //存在错误
                    my_debug!("{:?}", resp);
                    return Err(self.handle_error(resp.get_region_error().clone(), String::new()));
                }
                //println!("{:?}", resp);
                return Ok(());
            },
            Err(e) => {
                return Err(e.to_string());
            }
        }
        //Err("no done".to_string())
    }

    fn handle_error(&self, region_error: errorpb::Error, error: String) -> String {
        //let mut err;
        if !region_error.get_message().is_empty() {   //region的error出现了错误
            //return "maybe try again".to_string();  //感觉可以重试
            return region_error.get_message().clone().to_string();
        }
        if !error.is_empty() {  //出现error，直接返回出现的错误
            return error;
        }
        return "no error".to_string();
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::raw::PdClient;

    #[test]
    fn test_kvclient() {
        let env = Arc::new(
            EnvBuilder::new()
                .cq_count(1)       // 设置队列深度和poll线程
                .name_prefix("tikv_raw_proxy_grpc")   //设置线程名称
                .build(),
        );
        let pd = PdClient::connect("127.0.0.1:2379".to_string(), env.clone()).unwrap();
        let kv1 = KvClient::connect("127.0.0.1:20160".to_string(), env.clone()).unwrap();
        let kv2 = KvClient::connect("127.0.0.1:20161".to_string(), env.clone()).unwrap();
        let kv3 = KvClient::connect("127.0.0.1:20162".to_string(), env.clone()).unwrap();
        let key = "aaa".to_string().into_bytes();
        let value = "bbb".to_string().into_bytes();
        let rcont = pd.creat_raw_context(key.clone()).unwrap();
        println!("{:?}", rcont);
        println!("kv1:{:?}", kv1.raw_put(rcont.clone(), key.clone(), value.clone()));
        println!("kv2:{:?}", kv2.raw_put(rcont.clone(), key.clone(), value.clone()));
        println!("kv3:{:?}", kv3.raw_put(rcont.clone(), key.clone(), value.clone()));

        println!("kv1:{:?}", kv1.raw_get(rcont.clone(), key.clone()));
        println!("kv2:{:?}", kv2.raw_get(rcont.clone(), key.clone()));
        println!("kv3:{:?}", kv3.raw_get(rcont.clone(), key.clone()));
        
    }
    #[test]
    fn test_scan_kvclient() {
        let env = Arc::new(
            EnvBuilder::new()
                .cq_count(1)       // 设置队列深度和poll线程
                .name_prefix("tikv_raw_proxy_grpc")   //设置线程名称
                .build(),
        );
        let pd = PdClient::connect("127.0.0.1:2379".to_string(), env.clone()).unwrap();
        let kv1 = KvClient::connect("127.0.0.1:20162".to_string(), env.clone()).unwrap();
        let key = "b9900".to_string().into_bytes();
        let key2 = "ccc".to_string().into_bytes();
        let rcont = pd.creat_raw_context(key.clone()).unwrap();
        println!("{:?}", rcont);
        println!("kv1:{:?}", kv1.raw_scan(rcont.clone(), key.clone(), key2.clone(), 200));
    }
}