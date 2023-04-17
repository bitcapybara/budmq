use bud_common::protocol::{Packet, Publish, ReturnCode};
use bytes::Bytes;
use tokio::sync::{mpsc, oneshot};

use crate::connector::{self, OutgoingMessage};

type Result<T> = std::result::Result<T, Error>;

#[derive(Debug)]
pub enum Error {
    FromServer(ReturnCode),
    Internal(String),
    Connector(connector::Error),
}

impl std::error::Error for Error {}

impl std::fmt::Display for Error {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Error::FromServer(code) => write!(f, "receive from server: {code}"),
            Error::Internal(e) => write!(f, "internal error: {e}"),
            Error::Connector(e) => write!(f, "connector error: {e}"),
        }
    }
}

impl From<connector::Error> for Error {
    fn from(e: connector::Error) -> Self {
        Self::Connector(e)
    }
}

impl<T> From<mpsc::error::SendError<T>> for Error {
    fn from(e: mpsc::error::SendError<T>) -> Self {
        Self::Internal(e.to_string())
    }
}

impl From<oneshot::error::RecvError> for Error {
    fn from(e: oneshot::error::RecvError) -> Self {
        Self::Internal(e.to_string())
    }
}

pub struct ProducerMessage {
    pub topic: String,
    pub sequence_id: u64,
    pub data: Bytes,
    pub res_tx: oneshot::Sender<connector::Result<ReturnCode>>,
}

pub struct Producer {
    topic: String,
    sequence_id: u64,
    tx: mpsc::UnboundedSender<OutgoingMessage>,
}

impl Producer {
    pub fn new(topic: &str, tx: mpsc::UnboundedSender<OutgoingMessage>) -> Self {
        Self {
            topic: topic.to_string(),
            sequence_id: 0,
            tx,
        }
    }

    pub async fn send(&mut self, data: &[u8]) -> Result<()> {
        self.sequence_id += 1;
        let (res_tx, res_rx) = oneshot::channel();
        self.tx.send(OutgoingMessage {
            packet: Packet::Publish(Publish {
                topic: self.topic.clone(),
                sequence_id: self.sequence_id,
                payload: Bytes::copy_from_slice(data),
            }),
            res_tx,
        })?;
        match res_rx.await?? {
            ReturnCode::Success => Ok(()),
            code => Err(Error::FromServer(code)),
        }
    }
}
