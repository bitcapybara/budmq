use bytes::Bytes;
use libbud_common::protocol::{Packet, Publish, ReturnCode};
use tokio::sync::{mpsc, oneshot};

use crate::connector::{self, OutgoingMessage};

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

impl<T> From<mpsc::error::SendError<T>> for Error {
    fn from(value: mpsc::error::SendError<T>) -> Self {
        todo!()
    }
}

impl From<oneshot::error::RecvError> for Error {
    fn from(value: oneshot::error::RecvError) -> Self {
        todo!()
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
        let res = res_rx.await?;
        Ok(())
    }
}
