

use crate::raw::RawProxy;
use crate::protos::proxy::{ResponseStatus, GetRequest, GetResponse, PutRequest, PutResponse, DeleteRequest, DeleteResponse, ScanRequest, ScanResponse};
use crate::protos::proxy_grpc::{self, Proxy};
use grpcio::{Environment, EnvBuilder, RpcContext, ServerBuilder, UnarySink, Server as GrpcServer};
use futures::Future;
use futures::sync::oneshot;

use crate::{Key, Value, Result};

use std::sync::Arc;
use std::{io, thread};
use std::io::Read;

#[macro_export]
macro_rules! server_debug {
    ($($arg: tt)*) => (
        println!("Debug[{}:{}]: {}", file!(), line!(),format_args!($($arg)*));
    )
}

#[derive(Clone)]
struct Service{
    db: Arc<RawProxy>,
    env: Arc<Environment>,
}

impl Proxy for Service {
    fn get(&mut self, ctx: RpcContext, req: GetRequest, sink: UnarySink<GetResponse>){
        let mut response = GetResponse::new();
        server_debug!("Received GetRequest {{ {:?} }}", req);
        let db = self.db.clone();
        let ret = db.get(req.key.clone());
        match ret {
            Ok(value) => {
                response.set_status(ResponseStatus::kSuccess);
                response.set_value(value);
            },
            Err(e) => {
                response.set_status(ResponseStatus::kFailed);
                response.set_err(e);
            }
        }

        let f = sink.success(response.clone())
            .map(move |_| server_debug!("reply with  {{ {:?} }}", response))
            .map_err(move |err| eprintln!("Failed to reply: {:?}", err));
        ctx.spawn(f)
    }
    fn put(&mut self, ctx: RpcContext, req: PutRequest, sink: UnarySink<PutResponse>) {
        let mut response = PutResponse::new();
        server_debug!("Received PutRequest {{ {:?} }}", req);
        let db = self.db.clone();
        let ret = db.put(req.key.clone(), req.value.clone());
        match ret {
            Ok(_) => {
                response.set_status(ResponseStatus::kSuccess);
            },
            Err(e) => {
                response.set_status(ResponseStatus::kFailed);
                response.set_err(e);
            }
        }

        let f = sink.success(response.clone())
            .map(move |_| server_debug!("reply with  {{ {:?} }}", response))
            .map_err(move |err| eprintln!("Failed to reply: {:?}", err));
        ctx.spawn(f)
    }
    fn delete(&mut self, ctx: RpcContext, req: DeleteRequest, sink: UnarySink<DeleteResponse>) {
        let mut response = DeleteResponse::new();
        server_debug!("Received DeleteRequest {{ {:?} }}", req);
        let db = self.db.clone();
        let ret = db.delete(req.key.clone());
        match ret {
            Ok(_) => {
                response.set_status(ResponseStatus::kSuccess);
            },
            Err(e) => {
                response.set_status(ResponseStatus::kFailed);
                response.set_err(e);
            }
        }

        let f = sink.success(response.clone())
            .map(move |_| server_debug!("reply with  {{ {:?} }}", response))
            .map_err(move |err| eprintln!("Failed to reply: {:?}", err));
        ctx.spawn(f)
    }
    fn scan(&mut self, ctx: RpcContext, req: ScanRequest, sink: UnarySink<ScanResponse>) {
        
    }
}

impl Service {
    pub fn new(endpoint: String) -> Result<Service> {
        let env = Arc::new(
            EnvBuilder::new()
                .cq_count(2)       // 设置队列深度和poll线程
                .name_prefix("tikv_raw_proxy_grpc")   //设置线程名称
                .build(),
        );
        let raw_proxy = match RawProxy::connect(endpoint.clone(), env.clone()) {
            Ok(rp) => { rp },
            Err(e) => {
                return Err(e);
            },
        };
        let service = Service {
            db: Arc::new(raw_proxy),
            env,
        };
        Ok(service)
    }
}

pub struct KvServer {
    grpc_server: GrpcServer,
}

impl KvServer {
    pub fn creat(host: String, port: u16) -> Result<KvServer> {
        let addr = format!("{}:{}", host, port);
        let mut service;
        match Service::new(addr) {
            Ok(se) => {
                service = se;
            },
            Err(e) => {
                return Err(e);
            }
        }
        let service = proxy_grpc::create_proxy(service);
        let env = Arc::new(Environment::new(1));
        let grpc_server = ServerBuilder::new(env)
            .register_service(service)
            .bind(host.as_ref(), port.clone()).build().unwrap();
        let kvserver = KvServer {
            grpc_server: grpc_server,
        };
        Ok(kvserver)
    }
    pub fn start(&mut self) {
        self.grpc_server.start();

        for &(ref host, port) in self.grpc_server.bind_addrs() {
            server_debug!("listening on {}:{}", host, port);
        }
    }
    pub fn stop(&mut self) {
        println!("stoping kvserver...");
        let _ = self.grpc_server.shutdown().wait();
    }
}

fn main() {
    let host = "127.0.0.1".to_string();
    let port: u16 = 20001;
    let mut server;
    match KvServer::creat(host.clone(), port) {
        Ok(mut se) => {
            server = se;
        },
        Err(e) => {
            panic!("error:{}", e);
        }
    }

    server.start();

    let (tx, rx) = oneshot::channel();
    thread::spawn(move || {
        println!("Press ENTER to exit...");
        let _ = io::stdin().read(&mut [0]).unwrap();
        tx.send(())
    });
    let _ = rx.wait();
    
    server.stop();  
}