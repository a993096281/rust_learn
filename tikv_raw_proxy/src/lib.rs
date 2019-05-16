extern crate grpcio;
extern crate futures;

pub mod protos;
pub mod client;
pub mod server;

mod raw;


#[cfg(test)]
mod tests;

use std::result;

pub type Key = Vec<u8>;
pub type Value = Vec<u8>;
pub type Result<T> = result::Result<T, String>;


