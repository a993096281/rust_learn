
use kvproto::{pdpb, pdpb_grpc, metapb};
use grpcio::ChannelBuilder;
use grpcio::Environment;

use crate::{Key, Result};
use crate::raw::RawContext;

use std::sync::Arc;

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

    pub fn creat_raw_context(&self, key: Key) -> Result<RawContext> {  //根据key创建RawContext

        let mut region;
        let mut leader;
        let mut store;
        match self.get_region_and_leader(key.clone()) {  //获取region和leader
            Ok(resp) => {
                region = resp.0;
                leader = resp.1;
                
            },
            Err(e) => {
                return Err(e.to_string());
            }
        }
        let store_id = leader.get_store_id();
        
        match self.get_store(store_id) {   //获取store的信息
            Ok(resp) => {
                store = resp;
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
    pub fn get_region_and_leader(&self, key: Key) -> Result<(metapb::Region, metapb::Peer)> {   //根据key获取region和leader
        let mut request = pd_request!(self.cluster_id, pdpb::GetRegionRequest); //主要是为了添加RequestHeader
        request.set_region_key(key.clone());
        let mut region;
        let mut leader;
        match self.pdclient.get_region(&request) {   //获取key的region
            Ok(mut resp) => {
                my_debug!("get region key:{} res:{:?}", String::from_utf8(key.clone()).unwrap(), resp);
                region = resp.take_region();
                leader = resp.take_leader();
                if leader.get_store_id() == 0 {  //没有leader
                    return Err("no learer".to_string());
                }
                return Ok((region, leader));
                //println!("{:?}",resp);
            },
            Err(e) => {
                return Err(e.to_string());
            }
        }
    } 
    pub fn get_store(&self, store_id: u64) -> Result<metapb::Store> {   //根据store_id获取Store
        let mut request = pd_request!(self.cluster_id, pdpb::GetStoreRequest);  ////主要是为了添加RequestHeader
        request.set_store_id(store_id);
        match self.pdclient.get_store(&request) {   //获取store的信息
            Ok(mut resp) => {
                return Ok(resp.take_store());
            },
            Err(e) => {
                return Err(e.to_string());
            }
        }
    }
    /*pub fn get_region_by_id(&self, region_id: u64) -> Result<(metapb::Region, metapb::Peer)> {
        let mut request = pd_request!(self.cluster_id, pdpb::GetRegionByIDRequest); //主要是为了添加RequestHeader
        request.set_region_id(region_id);
        let mut region;
        let mut leader;
        match self.pdclient.get_region_by_id(&request) {   //获取key的region
            Ok(mut resp) => {
                my_debug!("get region id:{} res:{:?}", region_id, resp);
                //my_debug!("get region start:{} end:{}", resp.get_region().get_start_key().len(), resp.get_region().get_end_key().len());
                region = resp.take_region();
                leader = resp.take_leader();
                if region.get_id() == 0 {  //没有该region
                    return Err("no region".to_string());
                }
                if leader.get_store_id() == 0 {  //没有leader
                    return Err("no learer".to_string());
                }
                return Ok((region, leader));
                //println!("{:?}",resp);
            },
            Err(e) => {
                return Err(e.to_string());
            }
        }
    }*/

}

#[cfg(test)]
mod tests {
    use super::*;
    use grpcio::EnvBuilder;

    #[test]
    fn test_pdclient() {
        let env = Arc::new(
            EnvBuilder::new()
                .cq_count(1)       // 设置队列深度和poll线程
                .name_prefix("tikv_raw_proxy_grpc")   //设置线程名称
                .build(),
        );
        let pd = PdClient::connect("127.0.0.1:2379".to_string(), env).unwrap();

        //let pdclient = client.pdclient.clone();
        //let resp = pdclient.get_members(&pdpb::GetMembersRequest::new());
        //println!("{:?}",client.members);
        //let resp = pdclient
        //let resp = pd.get_region_and_leader("d555".to_string().into_bytes());
       // println!("{:?}",resp);
        //let resp = pd.get_region_by_id(2);
        //let resp = pd.get_region_by_id(1);
        //let resp = pd.get_region_by_id(3);
        //println!("{:?}",resp);
        println!("{:?}",pd.creat_raw_context("".to_string().into_bytes()));
        println!("{:?}",pd.creat_raw_context("zzz".to_string().into_bytes()));
        
        
    }
}
