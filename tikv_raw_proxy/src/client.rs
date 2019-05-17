
use std::sync::Arc;

use grpcio::{ChannelBuilder, EnvBuilder};

use crate::protos::proxy::{ResponseStatus, GetRequest, PutRequest, DeleteRequest, ScanRequest};
use crate::protos::proxy_grpc::ProxyClient;

use crate::{Key, Value, Result};

pub struct Client {
    pub client: ProxyClient,
}

impl Client {
    pub fn new(host: String, port: u16) -> Self {
        let addr = format!("{}:{}", host, port);
        let env = Arc::new(EnvBuilder::new().build());
        let ch = ChannelBuilder::new(env).connect(addr.as_ref());
        let proxy_client = ProxyClient::new(ch);

        Client {
            client: proxy_client,
        }
    }
    pub fn get(&self, key: Key) -> Result<Value> {
        let mut request = GetRequest::new();
        request.set_key(key);
        match self.client.get(&request) {
            Ok(res) => {
                match res.status {
                    ResponseStatus::kSuccess => Ok(res.value),
                    _ => Err(res.err),
                }
            },
            Err(e) => {
                return Err(e.to_string());
            }
        }
    }
    pub fn put(&self, key: Key, value: Value) -> Result<()> {
        let mut request = PutRequest::new();
        request.set_key(key);
        request.set_value(value);
        match self.client.put(&request) {
            Ok(res) => {
                match res.status {
                    ResponseStatus::kSuccess => Ok(()),
                    _ => Err(res.err),
                }
            },
            Err(e) => {
                return Err(e.to_string());
            }
        }
    }
    pub fn delete(&self, key: Key) -> Result<()> {
        let mut request = DeleteRequest::new();
        request.set_key(key);
        match self.client.delete(&request) {
            Ok(res) => {
                match res.status {
                    ResponseStatus::kSuccess => Ok(()),
                    _ => Err(res.err),
                }
            },
            Err(e) => {
                return Err(e.to_string());
            }
        }
    }
    pub fn scan(&self, key_start: Key, key_end: Key, limit: u32) -> Result<Vec<(Key, Value)>> {
        let mut request = ScanRequest::new();
        request.set_key_start(key_start);
        request.set_key_end(key_end);
        request.set_limit(limit);
        match self.client.scan(&request) {
            Ok(mut res) => {
                match res.status {
                    ResponseStatus::kSuccess => {
                        let kvs = res.take_pair().into_vec().into_iter().map( |mut kv| (kv.take_key(), kv.take_value())).collect();
                        return Ok(kvs);
                    },
                    _ => Err(res.err),
                }
            },
            Err(e) => {
                return Err(e.to_string());
            }
        }
    }
}