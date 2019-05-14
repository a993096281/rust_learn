

use crate::raw::RawProxy;
use crate::protos::proxy::{ResponseStatus, GetRequest, GetResponse, PutRequest, PutResponse, DeleteRequest, DeleteResponse, ScanRequest, ScanResponse};
use crate::protos::proxy_grpc::{self, Proxy};
use grpcio::{Environment, RpcContext, ServerBuilder, UnarySink};
use futures::Future;

use std::sync::Arc;

#[derive(Clone)]
struct Service{
    db: Arc<RawProxy>,
}

impl Proxy for Service {
    fn get(&mut self, ctx: RpcContext, req: GetRequest, sink: UnarySink<GetResponse>){
        let mut response = GetResponse::new();
        //println!("Received GetRequest {{ {:?} }}", req);
        let db = self.db.clone();
        let ret = db.get(&req.key);
        match ret {
            Ok(value) => {
                response.set_status(ResponseStatus::kSuccess);
                response.set_value(value);
            }
            Err(e) => {
                response.set_status(ResponseStatus::kFailed);
                response.set_err(e);
            }
        }

        let f = sink.success(response.clone())
            .map(move |_| println!("reply with  {{ {:?} }}", response))
            .map_err(move |err| eprintln!("Failed to reply: {:?}", err));
        ctx.spawn(f)
    }
    fn put(&mut self, ctx: RpcContext, req: PutRequest, sink: UnarySink<PutResponse>) {

    }
    fn delete(&mut self, ctx: RpcContext, req: DeleteRequest, sink: UnarySink<DeleteResponse>) {

    }
    fn scan(&mut self, ctx: RpcContext, req: ScanRequest, sink: UnarySink<ScanResponse>) {

    }
}

impl Service {

}