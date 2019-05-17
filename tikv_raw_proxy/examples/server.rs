use tikv_raw_proxy::server::KvServer;

use futures::Future;
use futures::sync::oneshot;

use std::{io, thread};
use std::io::Read;

fn main() {
    let host = "127.0.0.1".to_string(); 
    let port: u16 = 20001;
    let mut server;
    match KvServer::creat(host.clone(), port, "127.0.0.1:2379".to_string()) { //前两个参数是代理自己的监听ip:端口，后一个参数是tikv的ip:端口
        Ok(se) => {
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