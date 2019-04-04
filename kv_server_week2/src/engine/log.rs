use std::fs::OpenOptions;
use std::io::prelude::*;
use std::io::BufWriter;

use super::LogOption;

pub const FLUSH_NUM_TRIGGER: u64 = 1;
#[derive(Clone)]
pub struct Log{
    pub file_path: String,
    batch: Vec<(LogOption,String,String)>,
}

impl Log{
    pub fn new(log_path: &str) -> Self {
        println!("new log:{}", log_path);
        Log {
            file_path: String::from(log_path.clone()),
            batch: Vec::new(),
        }
    }
    pub fn record(&mut self,option: LogOption,key: &String, value: &String) {
        self.if_need_flush_do();
        self.batch.push((option,key.clone(),value.clone()));
        //self.batch.write_fmt(format_args!("{}\n{}\n{}\n", option.to_string(),key.clone(),value.clone())).expect("record error");
    }
    pub fn flush(&mut self) {
        let f = OpenOptions::new().create(true).append(true).open(self.file_path.clone()).unwrap();
        let mut writer = BufWriter::new(f);
        for (op, k , v) in self.batch.iter() {
            writer.write_fmt(format_args!("{}\n{}\n{}\n", op.to_string(),k.clone(),v.clone())).expect("record error");
        }
        self.batch.clear();
        
    }
    fn if_need_flush_do(&mut self){
        if self.batch.len() as u64 >= FLUSH_NUM_TRIGGER {
            self.flush();
        }
    }
}