use std::net::SocketAddr;

use crate::{
    consumer::{Consumer, Subscribe},
    producer::Producer,
};

type Result<T> = std::result::Result<T, Error>;

#[derive(Debug)]
pub enum Error {}

impl std::error::Error for Error {}

impl std::fmt::Display for Error {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        todo!()
    }
}

pub struct Client {
    addr: SocketAddr,
}

impl Client {
    pub fn new(addr: SocketAddr) -> Self {
        Self { addr }
    }

    pub fn new_producer(&self, topic: &str) -> Producer {
        todo!()
    }

    pub fn new_consumer(&self, subscribe: &Subscribe) -> Consumer {
        todo!()
    }
}
