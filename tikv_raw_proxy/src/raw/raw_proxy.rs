
use crate::{Key, Value, Result};

#[derive(Clone)]
pub struct RawProxy{

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

    }


}
