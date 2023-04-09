use bytes::Bytes;
use tokio::sync::mpsc;

use crate::connector::OutgoingMessage;

type Result<T> = std::result::Result<T, Error>;

#[derive(Debug)]
pub enum Error {}

impl std::error::Error for Error {}

impl std::fmt::Display for Error {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        todo!()
    }
}

pub enum SubType {
    Exclusive,
    Shared,
}

pub struct Subscribe {
    topic: String,
    sub_name: String,
    sub_type: SubType,
}

pub struct ConsumeMessage {
    pub payload: Bytes,
}

pub struct Consumer {
    pub id: u64,
    /// default total permits: 1000
    current_permits: u64,
    rx: mpsc::UnboundedReceiver<ConsumeMessage>,
}

impl Consumer {
    pub async fn new(
        sub: &Subscribe,
        server_tx: mpsc::UnboundedSender<OutgoingMessage>,
        consumer_rx: mpsc::UnboundedReceiver<ConsumeMessage>,
    ) -> Result<Self> {
        // send permits packet
        todo!()
    }

    pub async fn next(&mut self) -> Option<ConsumeMessage> {
        self.rx.recv().await
    }
}
