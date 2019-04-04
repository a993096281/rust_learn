use std::collections::{BTreeMap, HashMap};
use std::sync::{Arc, RwLock};

use std::fs::OpenOptions;
use std::io::prelude::*;
use std::io::BufReader;

use super::log::Log;
use super::LOG_PATH;

use super::LogOption;

#[derive(Clone)]
pub struct DbEngine{
    db: Arc<RwLock<BTreeMap<String,String>>>,
    log: Arc<RwLock<Log>>,
}

impl DbEngine{
    pub fn new() -> Self {
        println!("new DbEngine");
        let mut enfine = DbEngine {
            db: Arc::new(RwLock::new(BTreeMap::new())),
            log: Arc::new(RwLock::new(Log::new(LOG_PATH))),
        };
        enfine.recover();
        enfine
    }
    pub fn get(&self, key: &String) -> Result<Option<String>,()> {
        let db = self.db.read().unwrap();
        let ret = db.get(key);
        match ret {
            Some(s) => Ok(Some(s.clone())),
            None => Ok(None)
        }
        
    }
    pub fn put(&mut self, key: &String, value: &String) -> Result<i32,()> {
        self.log.write().unwrap().record(LogOption::TypePut,key,value);
        let ret = self.db.write().unwrap().insert(key.clone(),value.clone());
        match ret {
            Some(_) => Ok(1),  //有覆盖的key
            None => Ok(0)
        }
        
    }
    pub fn delete(&mut self, key: &String) -> Result<Option<String>,()> {
        self.log.write().unwrap().record(LogOption::TypeDelete,key,&"".to_string());
        let ret = self.db.write().unwrap().remove(key);
        match ret {
            Some(s) => Ok(Some(s)),  //删除存在的key-value
            None => Ok(None)         //删除不存在的key-value
        }
        
    }
    pub fn scan(&self, key_start: &String, key_end: &String) -> Result<Option<HashMap<String,String>>,()> { // key_start <= key < key_end
        let mut hmap = HashMap::new();
        for (k, v) in self.db.read().unwrap().range(key_start.clone()..key_end.clone()){
            println!("scan[{}:{}]", k, v);
            hmap.insert(k.clone(),v.clone());
        }
        if hmap.len() != 0 {
            Ok(Some(hmap))
        }
        else {
            Ok(None)
        }
    }
    pub fn recover(&mut self) {
        println!("DbEngine recovering...");
        let path = self.log.read().unwrap().file_path.clone();
        let f = OpenOptions::new().read(true).open(path.clone());
        match f {
            Err(_) => println!("recovery file:{} not exist!",path),
            Ok(f) => {
                let file = BufReader::new(&f);
                let mut it = file.lines();

                let mut line = it.next();
                let mut data_pool = self.db.write().unwrap();
                while line.is_some() {
                    let l = line.unwrap().unwrap();
                    if l == "1" {
                        let key = it.next().unwrap().unwrap();
                        let val = it.next().unwrap().unwrap();
                        data_pool.insert(key, val);
                    } else if l == "0" {
                        let key = it.next().unwrap().unwrap();
                        let _val = it.next().unwrap().unwrap();
                        data_pool.remove(&key);
                    }
                    line = it.next()
                }
            }
        }
        println!("DbEngine recovering done!");
    }
    pub fn stop(&mut self) {
        self.log.write().unwrap().flush();
        println!("DbEngine stop!");
    }
}