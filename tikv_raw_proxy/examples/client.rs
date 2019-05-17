use tikv_raw_proxy::client::Client;


fn main() {
    let host = "127.0.0.1".to_string(); 
    let port: u16 = 20001;
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
}