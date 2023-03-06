use std::fmt::Display;

use tokio::sync::{mpsc, oneshot};

use crate::protocol;

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
    client_id: u64,
    packet: protocol::Packet,
    res_tx: Option<oneshot::Sender<protocol::Packet>>,
    client_tx: Option<mpsc::Sender<protocol::Packet>>,
}

pub struct Broker {}
