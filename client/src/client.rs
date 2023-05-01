use std::{
    net::SocketAddr,
    sync::{atomic::AtomicU32, Arc},
};

use bud_common::{
    helper::wait_result,
    mtls::MtlsProvider,
    protocol::{Packet, ReturnCode},
};
use log::trace;
use tokio::{
    sync::{mpsc, oneshot},
    task::JoinHandle,
};
use tokio_util::sync::CancellationToken;

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

pub struct ClientBuilder {
    addr: SocketAddr,
    provider: MtlsProvider,
    // default to 10000ms
    keepalive: u16,
}

impl ClientBuilder {
    const DEFAULT_KEEPALIVE_MS: u16 = 10000;
    pub fn new(addr: SocketAddr, provider: MtlsProvider) -> Self {
        Self {
            addr,
            provider,
            keepalive: Self::DEFAULT_KEEPALIVE_MS,
        }
    }

    pub fn keepalive(mut self, keepalive: u16) -> Self {
        self.keepalive = keepalive;
        self
    }

    pub async fn build(self) -> Result<Client> {
        // Channel for sending messages to the server
        let (server_tx, server_rx) = mpsc::unbounded_channel();
        let consumers = Consumers::new();
        let token = CancellationToken::new();

        // connector task loop
        trace!("client::build: start connector task loop");
        let connector_task = Connector::new(
            self.addr,
            self.keepalive,
            self.provider,
            consumers.clone(),
            server_tx.clone(),
        )
        .run(server_rx, token.clone());
        let connector_handle = tokio::spawn(connector_task);

        Ok(Client {
            server_tx,
            consumers,
            consumer_id_gen: 0,
            connector_handle,
            token,
        })
    }
}

pub struct Client {
    consumer_id_gen: u64,
    server_tx: mpsc::UnboundedSender<OutgoingMessage>,
    consumers: Consumers,
    token: CancellationToken,
    connector_handle: JoinHandle<connector::Result<()>>,
}

impl Client {
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
            &subscribe,
            self.server_tx.clone(),
            consumer_rx,
        )
        .await?;
        let sender = ConsumerSender::new(consumer.id, permits, self.server_tx.clone(), consumer_tx);
        self.consumers
            .add_consumer(consumer.id, subscribe, sender)
            .await;
        Ok(consumer)
    }

    // TODO Drop?
    pub async fn close(self) -> Result<()> {
        trace!("client::close: close the client");

        // send disconnect message to server
        trace!("client::close: send DISCONNECT packet to server");
        let (disconn_res_tx, disconn_res_rx) = oneshot::channel();
        self.server_tx.send(OutgoingMessage {
            packet: Packet::Disconnect,
            res_tx: disconn_res_tx,
        })?;
        disconn_res_rx.await.ok();
        // cancel token
        self.token.cancel();

        // wait for connection handler exit
        trace!("client::close: waiting for connecotor task exit");
        wait_result(self.connector_handle, "connector task loop").await;

        Ok(())
    }
}
