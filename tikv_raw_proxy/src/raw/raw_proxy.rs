
use grpcio::Environment;

use crate::{Key, Value, Result};
use crate::raw::{PdClient, KvClient};
use std::collections::HashMap;
use std::sync::{Arc, RwLock};

#[derive(Clone)]
pub struct RawProxy{
    pd: Arc<PdClient>,
    tikv: Arc<RwLock<HashMap<String, Arc<KvClient>>>>,
}

impl RawProxy {
    pub fn get(&self, key: &Key) -> Result<Value> {
        Err("err".to_string())
    }
    pub fn put(&self, key: &Key, value: &Value) -> Result<()> {
        Ok(())
    }
    pub fn delete(&self, key: &Key) -> Result<()> {
        Ok(())
    }
    pub fn scan(&self, key_start: &Key, key_end: &Key, limit: u32) -> Result<()> {
        Ok(())
    }
    pub fn connect(endpoint: String, env: Arc<Environment>) -> Result<RawProxy> {
        let pd = match PdClient::connect(endpoint.clone(), env.clone()) {
            Ok(p) => { p },
            Err(e) => {
                return Err(e);
            },
        };
        let raw_proxy = RawProxy {
            pd: Arc::new(pd),
            tikv: Arc::new(RwLock::new(HashMap::new())),
        };
        Ok(raw_proxy)

    }


}
