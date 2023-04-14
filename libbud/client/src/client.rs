use std::{
    net::SocketAddr,
    sync::{atomic::AtomicU32, Arc},
};

use libbud_common::{
    mtls::MtlsProvider,
    protocol::{Connect, Packet, ReturnCode},
};
use tokio::sync::{mpsc, oneshot};

use crate::{
    connector::{self, Connector, ConsumerSender, OutgoingMessage},
    consumer::{self, Consumer, Consumers, SubscribeMessage, CONSUME_CHANNEL_CAPACITY},
    producer::Producer,
};

type Result<T> = std::result::Result<T, Error>;

#[derive(Debug)]
pub enum Error {
    FromServer(ReturnCode),
    Internal(String),
    Connector(connector::Error),
    Consumer(consumer::Error),
}

impl std::error::Error for Error {}

impl std::fmt::Display for Error {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Error::FromServer(e) => write!(f, "receive server error: {e}"),
            Error::Internal(e) => write!(f, "internal error: {e}"),
            Error::Connector(e) => write!(f, "connector error: {e}"),
            Error::Consumer(e) => write!(f, "consumer error: {e}"),
        }
    }
}

impl From<consumer::Error> for Error {
    fn from(e: consumer::Error) -> Self {
        Self::Consumer(e)
    }
}

impl<T> From<mpsc::error::SendError<T>> for Error {
    fn from(e: mpsc::error::SendError<T>) -> Self {
        Self::Internal(e.to_string())
    }
}

impl From<connector::Error> for Error {
    fn from(e: connector::Error) -> Self {
        Self::Connector(e)
    }
}

impl From<oneshot::error::RecvError> for Error {
    fn from(e: oneshot::error::RecvError) -> Self {
        Self::Internal(e.to_string())
    }
}

pub struct Client {
    consumer_id_gen: u64,
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
        // TODO End Notification and Waiting
        tokio::spawn(connector_task);

        // send connect packet
        const KEEP_ALIVE: u16 = 10000;
        let (conn_res_tx, conn_res_rx) = oneshot::channel();
        server_tx.send(OutgoingMessage {
            packet: Packet::Connect(Connect {
                keepalive: KEEP_ALIVE,
            }),
            res_tx: conn_res_tx,
        })?;
        match conn_res_rx.await?? {
            ReturnCode::Success => {}
            code => return Err(Error::FromServer(code)),
        }

        Ok(Self {
            server_tx,
            consumers,
            consumer_id_gen: 0,
        })
    }

    // TODO Drop?
    async fn close(self) -> Result<()> {
        let (disconn_res_tx, disconn_res_rx) = oneshot::channel();
        self.server_tx.send(OutgoingMessage {
            packet: Packet::Disconnect,
            res_tx: disconn_res_tx,
        })?;
        match disconn_res_rx.await?? {
            ReturnCode::Success => Ok(()),
            code => Err(Error::FromServer(code)),
        }
    }

    pub fn new_producer(&self, topic: &str) -> Producer {
        Producer::new(topic, self.server_tx.clone())
    }

    pub async fn new_consumer(&mut self, subscribe: SubscribeMessage) -> Result<Consumer> {
        let (consumer_tx, consumer_rx) = mpsc::unbounded_channel();
        let permits = Arc::new(AtomicU32::new(CONSUME_CHANNEL_CAPACITY));
        self.consumer_id_gen += 1;
        let consumer = Consumer::new(
            self.consumer_id_gen,
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
