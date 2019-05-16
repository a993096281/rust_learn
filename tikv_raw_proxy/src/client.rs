
use std::sync::Arc;

use grpcio::{ChannelBuilder, EnvBuilder};

use crate::protos::proxy::{ResponseStatus, GetRequest, GetResponse, PutRequest, PutResponse, DeleteRequest, DeleteResponse, ScanRequest, ScanResponse};
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
}