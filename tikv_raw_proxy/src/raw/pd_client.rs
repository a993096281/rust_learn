
use kvproto::{pdpb, pdpb_grpc};
use grpcio::{ChannelBuilder, EnvBuilder};
use grpcio::Environment;

use crate::{Key, Value, Result};
use crate::raw::RawContext;

use std::sync::{Arc, RwLock};

macro_rules! pd_request {
    ($cluster_id:expr, $type:ty) => {{
        let mut request = <$type>::new();
        let mut header = ::kvproto::pdpb::RequestHeader::new();
        header.set_cluster_id($cluster_id);
        request.set_header(header);
        request
    }};
}

#[derive(Clone)]
pub struct PdClient{
    pub pdclient: pdpb_grpc::PdClient,
    pub cluster_id: u64,
}

impl PdClient {
    pub fn connect(endpoint: String, env: Arc<Environment>) -> Result<PdClient> {
        let ch = ChannelBuilder::new(env).connect(endpoint.as_ref());
        let pdclient = pdpb_grpc::PdClient::new(ch);
        
        let cluster_id = match pdclient.get_members(&pdpb::GetMembersRequest::new()) {
            Ok(resp) => {
                resp.get_header().get_cluster_id()
            },
            Err(e) => {
                return Err(e.to_string());
            }
        };
        let pd = PdClient {
            pdclient,
            cluster_id,
        };
        Ok(pd)
    }

    pub fn creat_raw_context(&self, key: &Key) -> Result<RawContext> {

        let mut request = pd_request!(self.cluster_id, pdpb::GetRegionRequest); //主要是为了添加RequestHeader
        request.set_region_key(key.clone());
        let mut region;
        let mut leader;
        let mut store;
        match self.pdclient.get_region(&request) {   //获取key的region
            Ok(mut resp) => {
                region = resp.take_region();
                leader = resp.take_leader();
                
                //return Ok(raw_context);
                //println!("{:?}",resp);
            },
            Err(e) => {
                return Err(e.to_string());
            }
        }
        let store_id = leader.get_store_id();
        let mut request = pd_request!(self.cluster_id, pdpb::GetStoreRequest);
        request.set_store_id(store_id);
        match self.pdclient.get_store(&request) {   //获取store的信息
            Ok(mut resp) => {
                store = resp.take_store();
            },
            Err(e) => {
                return Err(e.to_string());
            }
        }
        let raw_context = RawContext {
            region,
            leader,
            store,
        };
        Ok(raw_context)
        
    }

    

    
    

}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_pdclient() {
        let env = Arc::new(
            EnvBuilder::new()
                .cq_count(1)       // 设置队列深度和poll线程
                .name_prefix("tikv_raw_proxy_grpc")   //设置线程名称
                .build(),
        );
        let client = PdClient::connect("127.0.0.1:2379".to_string(), env).unwrap();
        //let pdclient = client.pdclient.clone();
        //let resp = pdclient.get_members(&pdpb::GetMembersRequest::new());
        //println!("{:?}",client.members);
        //let resp = pdclient
        let resp = client.creat_raw_context(&"dv".to_string().into_bytes()).unwrap();
        println!("{:?}",resp);
    }
}
