use crate::server::KvServer;
use crate::client::Client;
use std::time::Instant;

#[test]
fn test_basic() {
    let host = "127.0.0.1".to_string(); 
    let port: u16 = 20001;
    let endpoint = "127.0.0.1:2379".to_string(); //tikv的ip

    let mut server;
    match KvServer::creat(host.clone(), port, endpoint.clone()) { 
        Ok(se) => {
            server = se;
        },
        Err(e) => {
            panic!("error:{}", e);
        }
    }
    server.start();

    let client = Client::new(host.clone(), port);

    println!("put:{:?}", client.put("aaa".to_string().into_bytes(), "abc".to_string().into_bytes()));
    println!("put:{:?}", client.put("aaa2".to_string().into_bytes(), "abcd".to_string().into_bytes()));
    println!("put:{:?}", client.put("aaa3".to_string().into_bytes(), "abcde".to_string().into_bytes()));

    println!("get:{:?}", client.get("aaa".to_string().into_bytes()));
    println!("get:{:?}", client.get("aaa2".to_string().into_bytes()));

    println!("put:{:?}", client.put("aaa".to_string().into_bytes(), "123".to_string().into_bytes()));
    println!("get:{:?}", client.get("aaa".to_string().into_bytes()));

    println!("delete:{:?}", client.get("aaa2".to_string().into_bytes()));
    println!("get:{:?}", client.get("aaa2".to_string().into_bytes()));

    println!("put:{:?}", client.put("aaa2".to_string().into_bytes(), "abcd".to_string().into_bytes()));
    println!("put:{:?}", client.put("aaa4".to_string().into_bytes(), "abcdef".to_string().into_bytes()));

    println!("scan:{:?}", client.scan("".to_string().into_bytes(), "".to_string().into_bytes(), 10));
    println!("scan:{:?}", client.scan("a".to_string().into_bytes(), "b".to_string().into_bytes(), 10));
    println!("scan:{:?}", client.scan("aaa1".to_string().into_bytes(), "aaa3".to_string().into_bytes(), 10));
    println!("scan:{:?}", client.scan("aaa1".to_string().into_bytes(), "aaa1".to_string().into_bytes(), 10));
    println!("scan:{:?}", client.scan("aaa2".to_string().into_bytes(), "".to_string().into_bytes(), 10));
    println!("scan:{:?}", client.scan("".to_string().into_bytes(), "aaa3".to_string().into_bytes(), 10));

    server.stop();
}

fn getvalue(size: usize) -> String {  //生成长度为size+1的value
        format!("{:>0width$}", 1, width=size)
}

#[test]
fn test_performance() {
    let value_size: usize = 4096;
    let put_num: usize = 2000;

    let host = "127.0.0.1".to_string(); 
    let port: u16 = 20001;
    let endpoint = "127.0.0.1:2379".to_string(); //tikv的ip
    let mut server;
    match KvServer::creat(host.clone(), port, endpoint.clone()) { 
        Ok(se) => {
            server = se;
        },
        Err(e) => {
            panic!("error:{}", e);
        }
    }
    server.start();
    let client = Client::new(host.clone(), port);

    let time = Instant::now();
    let mut ok = 0;
    let value = getvalue(value_size);
    
    for i in 0..put_num {
        let key = format!("{}", i);
        let ret = client.put(key.into_bytes(), value.clone().into_bytes());
        if ret.is_ok() {
            ok +=1;
        }
    }

    let time2 = time.elapsed().as_millis();
    println!("put_num:{} value_size:{} ok:{} err:{}", put_num, value_size, ok, put_num - ok);
    println!("use time:{}s {}ms; iops:{:.2}", time2 / 1000, time2 % 1000, put_num as f64 / (time2 as f64 / 1000.0));

    server.stop();
}