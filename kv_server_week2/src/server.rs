extern crate grpcio;
extern crate lib;



use std::io::Read;
use std::sync::Arc;
use std::{io, thread};

use futures::sync::oneshot;
use futures::Future;
use grpcio::{Environment, RpcContext, ServerBuilder, UnarySink};


use lib::protos::kvserver::{ResponseStatus, GetRequest, GetResponse, PutRequest, PutResponse, DeleteRequest, DeleteResponse, ScanRequest, ScanResponse};
use lib::protos::kvserver_grpc::{self, Kvdb};

use lib::engine::dbengine::DbEngine;

#[derive(Clone)]
struct DbService{
    db_engine: DbEngine,
}

impl Kvdb for DbService{
    fn get(&mut self, ctx: RpcContext, req: GetRequest, sink: UnarySink<GetResponse>){
        let mut response = GetResponse::new();
        //println!("Received GetRequest {{ {:?} }}", req);
        let engine = &mut self.db_engine;
        let ret = engine.get(&req.key);
        match ret {
            Ok(op) => match op {
                Some(value) => {
                    response.set_status(ResponseStatus::kSuccess);
                    response.set_value(value);
                }
                None => response.set_status(ResponseStatus::kNotFound),
            }
            Err(_) => response.set_status(ResponseStatus::kFailed),
        }

        let f = sink.success(response.clone())
            //.map(move |_| println!("Responded with  {{ {:?} }}", response))
            .map_err(move |err| eprintln!("Failed to reply: {:?}", err));
        ctx.spawn(f)
    }
    fn put(&mut self, ctx: RpcContext, req: PutRequest, sink: UnarySink<PutResponse>) {
        let mut response = PutResponse::new();
        //println!("Received PutRequest {{ {:?} }}", req);
        let engine = &mut self.db_engine;
        let ret = engine.put(&req.key, &req.value);
        match ret {
            Ok(_) => {
                response.set_status(ResponseStatus::kSuccess);
            }
            Err(_) => response.set_status(ResponseStatus::kFailed),
        }
        let f = sink.success(response.clone())
            //.map(move |_| println!("Responded with  {{ {:?} }}", response))
            .map_err(move |err| eprintln!("Failed to reply: {:?}", err));
        ctx.spawn(f)
    }
    fn delete(&mut self, ctx: RpcContext, req: DeleteRequest, sink: UnarySink<DeleteResponse>) {
        let mut response = DeleteResponse::new();
        //println!("Received DeleteResponse {{ {:?} }}", req);
        let engine = &mut self.db_engine;
        let ret = engine.delete(&req.key);
        match ret {
            Ok(op) => match op {
                        Some(_) => response.set_status(ResponseStatus::kSuccess),
                        None => response.set_status(ResponseStatus::kNotFound),
                    }
            Err(_) => response.set_status(ResponseStatus::kFailed),
        }
        let f = sink.success(response.clone())
            //.map(move |_| println!("Responded with  {{ {:?} }}", response))
            .map_err(move |err| eprintln!("Failed to reply: {:?}", err));
        ctx.spawn(f)
    }
    fn scan(&mut self, ctx: RpcContext, req: ScanRequest, sink: UnarySink<ScanResponse>) { // key_start <= key < key_end
        let mut response = ScanResponse::new();
        //println!("Received ScanRequest {{ {:?} }}", req);
        let engine = &mut self.db_engine;
        let ret = engine.scan(&req.key_start, &req.key_end); // key_start <= key < key_end
        match ret {
            Ok(op) => match op {
                Some(key_value) => {
                    response.set_status(ResponseStatus::kSuccess);
                    response.set_key_value(key_value);
                }
                None => response.set_status(ResponseStatus::kNotFound),
            }
            Err(_) => response.set_status(ResponseStatus::kFailed),
        }

        let f = sink.success(response.clone())
            //.map(move |_| println!("Responded with  {{ {:?} }}", response))
            .map_err(move |err| eprintln!("Failed to reply: {:?}", err));
        ctx.spawn(f)
    }
}

impl DbService{
    pub fn new() -> Self {
        println!("new DbService");
        DbService {
            db_engine: DbEngine::new(),
        }
    }
    pub fn stop(&mut self) {
        self.db_engine.flush();
    }
}

fn main(){
    let env = Arc::new(Environment::new(1));
    let mut db = DbService::new();
    let service  = kvserver_grpc::create_kvdb(db.clone());
    let mut server = ServerBuilder::new(env)
        .register_service(service)
        .bind("127.0.0.1", 20001)
        .build()
        .unwrap();

    server.start();
    for &(ref host, port) in server.bind_addrs() {
        println!("listening on {}:{}", host, port);
    }
    let (tx, rx) = oneshot::channel();
    thread::spawn(move || {
        println!("Press ENTER to exit...");
        let _ = io::stdin().read(&mut [0]).unwrap();
        tx.send(())
    });
    let _ = rx.wait();
    db.stop();
    let _ = server.shutdown().wait();   
}