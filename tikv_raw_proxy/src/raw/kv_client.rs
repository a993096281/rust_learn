
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
            Ok(resp) => {  //如果error ？？
                return Ok(resp.take_value());
            },
            Err(e) => {
                return Err(e.to_string());
            }
        }

    }
}