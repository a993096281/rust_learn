use crate::service::{TSOClient, TransactionClient};

use labrpc::*;
use futures_timer::Delay;
use futures::Future;
use std::thread;
use std::time::Duration;
//use std::time;
use crate::msg::{CommitRequest, GetRequest, PrewriteRequest, TimestampRequest};
// BACKOFF_TIME_MS is the wait time before retrying to send the request.
// It should be exponential growth. e.g.
//|  retry time  |  backoff time  |
//|--------------|----------------|
//|      1       |       100      |
//|      2       |       200      |
//|      3       |       400      |
const BACKOFF_TIME_MS: u64 = 100;
// RETRY_TIMES is the maximum number of times a client attempts to send a request.
const RETRY_TIMES: usize = 3;

#[derive(Clone, Debug)]
pub struct Transaction {
    pub start_ts: u64,   //时间
    pub writes: Vec<(Vec<u8>, Vec<u8>)>,  //key,value
}

impl Transaction {
    pub fn new() -> Transaction {
        Transaction {
            start_ts: 0,
            writes: vec![],
        }
    }
}

/// Client mainly has two purposes:
/// One is getting a monotonically increasing timestamp from TSO (Timestamp Oracle).
/// The other is do the transaction logic.
#[derive(Clone)]
pub struct Client {
    // Your definitions here.
    tso_client: TSOClient,
    txn_client: TransactionClient,
    txn: Transaction
}

impl Client {
    /// Creates a new Client.
    pub fn new(tso_client: TSOClient, txn_client: TransactionClient) -> Client {
        // Your code here.
        Client {
            tso_client,
            txn_client,
            txn: Transaction::new(),
        }
    }

    /// Gets a timestamp from a TSO.
    pub fn get_timestamp(&self) -> Result<u64> {
        // Your code here.
        //unimplemented!()
        let args = TimestampRequest{};

        let mut backoff = BACKOFF_TIME_MS;
        //let time = time::Instant::now();
        for i in 0..RETRY_TIMES {
            match self.tso_client.get_timestamp(&args.clone()).wait() {
                Ok(reply) => {
                    //println!("ok time:{}", reply.timestamp);
                    return Ok(reply.timestamp);
                }
                Err(_) => {

                    Delay::new(Duration::from_millis(backoff)).wait().unwrap();
                    //let time2 = time.elapsed().as_millis();
                    //println!("time2:{:?}", time2);
                    backoff *= 2;
                    continue;
                }
            }
        }
        Err(Error::Timeout)
    }

    /// Begins a new transaction.
    pub fn begin(&mut self) {
        // Your code here.
        //unimplemented!()
        let start_ts = match self.get_timestamp() {
            Ok(ts) => ts,
            Err(Error::Timeout) => {
                println!("get_timestamp timeout");
                return;
            }
            _ => panic!("error:get_timestamp"),
        };
        self.txn = Transaction {
            start_ts,
            writes: vec![],
        };
    }

    /// Gets the value for a given key.
    pub fn get(&self, key: Vec<u8>) -> Result<Vec<u8>> {
        // Your code here.
        //unimplemented!()
        let mut backoff = BACKOFF_TIME_MS;
        for i in 0..RETRY_TIMES {
            let args = GetRequest {
                start_ts: self.txn.start_ts,
                key: key.clone(),
            };
            match self.txn_client.get(&args.clone()).wait()
            {
                Ok(reply) => {
                    return Ok(reply.value);
                }
                Err(_) => {
                    Delay::new(Duration::from_millis(backoff)).wait().unwrap();
                    backoff *= 2;
                    continue;
                }
            }
        }
        Err(Error::Timeout)
    }

    /// Sets keys in a buffer until commit time.
    pub fn set(&mut self, key: Vec<u8>, value: Vec<u8>) {
        // Your code here.
        self.txn.writes.push((key, value));
    }

    /// Commits a transaction.
    pub fn commit(&self) -> Result<bool> {
        // Your code here.
        //unimplemented!()
        let primary = &self.txn.writes[0];   //primary
        let secondaries = &self.txn.writes[1..];  //其它

        //prewrite 阶段
        let primary_args = PrewriteRequest {
            start_ts: self.txn.start_ts,
            w_key: primary.0.clone(),
            w_value: primary.1.clone(),
            primary_key: primary.0.clone(),
            primary_value: primary.1.clone(),
        };
        if self.txn_client.prewrite(&primary_args.clone()).wait().is_err() //primary 出错
        {
            return Ok(false);
        }

        for w in secondaries {
            let w_args = PrewriteRequest {
                start_ts: self.txn.start_ts,
                w_key: w.0.clone(),
                w_value: w.1.clone(),
                primary_key: primary.0.clone(),
                primary_value: primary.1.clone(),
            };
            if self.txn_client.prewrite(&w_args.clone()).wait().is_err()   //secondaries 出错
            {
                return Ok(false);
            }
        }

        // commit 阶段
        let commit_ts = match self.get_timestamp() {
            Ok(ts) => ts,
            Err(Error::Timeout) => {
                println!("get_timestamp timeout");
                return Ok(false);
            }
            _ => {
                panic!("error:get_timestamp");
                return Ok(false);

            }
        };
        let primary_args = CommitRequest {
            is_primary: true,
            start_ts: self.txn.start_ts,
            commit_ts,
            write_key: primary.0.clone(),
            write_value: primary.1.clone(),
            primary_key: primary.0.clone(),
            primary_value: primary.1.clone(),
        };
        match self.txn_client.commit(&primary_args.clone()).wait()
        {
            Ok(_) => {}
            Err(Error::Other(e)) => {
                if e == "resphook" {
                    return Err(Error::Other("resphook".to_owned()));
                } else {
                    return Ok(false);
                }
            }
            Err(_) => return Ok(false),
        }
        //只要primary提交成功，说明commit成功，
        //secondaries就算没有提交成功，也会被其它线程下次读它的时候提交成功
        for w in secondaries {
            let start_ts = self.txn.start_ts;
            let txn_client = self.txn_client.clone();
            let write = w.clone();
            let primary = primary.clone();
            thread::spawn(move || {
                let w_args = CommitRequest {
                    is_primary: false,
                    start_ts,
                    commit_ts,
                    write_key: write.0.clone(),
                    write_value: write.1.clone(),
                    primary_key: primary.0.clone(),
                    primary_value: primary.1.clone(),

                };
                let _ = txn_client.commit(&w_args.clone()).wait();
            });
        }

        Ok(true)
    }
}
