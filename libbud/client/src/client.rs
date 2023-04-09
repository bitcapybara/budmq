use std::{
    io,
    net::SocketAddr,
    sync::{atomic::AtomicU32, Arc},
};

use libbud_common::{mtls::MtlsProvider, protocol};
use s2n_quic::{connection, provider};
use tokio::sync::mpsc;

use crate::{
    connector::{self, Connector, OutgoingMessage},
    consumer::{self, Consumer, ConsumerSender, Consumers, Subscribe, CONSUME_CHANNEL_CAPACITY},
    producer::{self, Producer},
};

type Result<T> = std::result::Result<T, Error>;

#[derive(Debug)]
pub enum Error {
    StreamClosed,
    UnexpectedPacket,
}

impl std::error::Error for Error {}

impl std::fmt::Display for Error {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        todo!()
    }
}

impl From<io::Error> for Error {
    fn from(value: io::Error) -> Self {
        todo!()
    }
}

impl From<provider::StartError> for Error {
    fn from(value: provider::StartError) -> Self {
        todo!()
    }
}

impl From<connection::Error> for Error {
    fn from(value: connection::Error) -> Self {
        todo!()
    }
}

impl From<producer::Error> for Error {
    fn from(value: producer::Error) -> Self {
        todo!()
    }
}

impl From<consumer::Error> for Error {
    fn from(value: consumer::Error) -> Self {
        todo!()
    }
}

impl From<protocol::Error> for Error {
    fn from(value: protocol::Error) -> Self {
        todo!()
    }
}

impl<T> From<mpsc::error::SendError<T>> for Error {
    fn from(value: mpsc::error::SendError<T>) -> Self {
        todo!()
    }
}

impl From<connector::Error> for Error {
    fn from(value: connector::Error) -> Self {
        todo!()
    }
}

pub struct Client {
    server_tx: mpsc::UnboundedSender<OutgoingMessage>,
    consumers: Consumers,
}

impl Client {
    pub async fn new(addr: SocketAddr, provider: MtlsProvider) -> Result<Self> {
        // Channel for sending messages to the server
        let (server_tx, server_rx) = mpsc::unbounded_channel();
        let consumers = Consumers::new();

        // connector task loop
        let connector_task = Connector::new(addr, provider)
            .await?
            .run(server_rx, consumers.clone());
        let connector_handle = tokio::spawn(connector_task);

        Ok(Self {
            server_tx,
            consumers,
        })
    }

    pub async fn new_producer(&self, topic: &str) -> Result<Producer> {
        let producer = Producer::new(topic, self.server_tx.clone());
        Ok(producer)
    }

    pub async fn new_consumer(&mut self, subscribe: &Subscribe) -> Result<Consumer> {
        let (consumer_tx, consumer_rx) = mpsc::unbounded_channel();
        let permits = Arc::new(AtomicU32::new(CONSUME_CHANNEL_CAPACITY));
        let consumer = Consumer::new(
            permits.clone(),
            subscribe,
            self.server_tx.clone(),
            consumer_rx,
        )
        .await?;
        self.consumers
            .add_consumer(
                consumer.id,
                ConsumerSender::new(consumer.id, permits, self.server_tx.clone(), consumer_tx),
            )
            .await;
        Ok(consumer)
    }
}
