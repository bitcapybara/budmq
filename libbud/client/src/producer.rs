use libbud_common::protocol::ReturnCode;
use tokio::sync::{mpsc, oneshot};

use crate::connector;

type Result<T> = std::result::Result<T, Error>;

#[derive(Debug)]
pub enum Error {}

impl std::error::Error for Error {}

impl std::fmt::Display for Error {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        todo!()
    }
}

impl From<connector::Error> for Error {
    fn from(value: connector::Error) -> Self {
        todo!()
    }
}

pub struct ProducerMessage {
    pub topic: String,
    pub sequence_id: u64,
    pub data: Vec<u8>,
    pub res_tx: oneshot::Sender<connector::Result<ReturnCode>>,
}

pub struct Producer {
    tx: mpsc::UnboundedSender<ProducerMessage>,
}

impl Producer {
    pub fn new(topic: &str, tx: mpsc::UnboundedSender<ProducerMessage>) -> Self {
        todo!()
    }

    pub fn send(&self, data: &[u8]) -> Result<()> {
        todo!()
    }
}
