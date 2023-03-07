use std::fmt::Display;

use tokio::sync::{mpsc, oneshot};

use crate::protocol::{self, ReturnCode};

type Result<T> = std::result::Result<T, Error>;

#[derive(Debug)]
pub enum Error {}

impl std::error::Error for Error {}

impl Display for Error {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        todo!()
    }
}

pub struct Message {
    pub client_id: u64,
    pub packet: protocol::Packet,
    pub res_tx: Option<oneshot::Sender<ReturnCode>>,
    pub client_tx: Option<mpsc::Sender<protocol::Packet>>,
}

pub struct Broker {}
