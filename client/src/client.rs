use std::net::SocketAddr;

use bud_common::{mtls::MtlsProvider, protocol::ReturnCode};
use tokio::sync::{mpsc, oneshot};

use crate::{
    connection::{writer::OutgoingMessage, ConnectionHandle},
    consumer::{self, Consumer, SubscribeMessage},
    producer::{self, Producer},
};

type Result<T> = std::result::Result<T, Error>;

#[derive(Debug)]
pub enum Error {
    FromServer(ReturnCode),
    Internal(String),
    Producer(producer::Error),
    Consumer(consumer::Error),
}

impl std::error::Error for Error {}

impl std::fmt::Display for Error {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Error::FromServer(e) => write!(f, "receive server error: {e}"),
            Error::Internal(e) => write!(f, "internal error: {e}"),
            Error::Consumer(e) => write!(f, "consumer error: {e}"),
            Error::Producer(e) => write!(f, "producer error: {e}"),
        }
    }
}

impl From<producer::Error> for Error {
    fn from(e: producer::Error) -> Self {
        Self::Producer(e)
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

impl From<oneshot::error::RecvError> for Error {
    fn from(e: oneshot::error::RecvError) -> Self {
        Self::Internal(e.to_string())
    }
}

pub struct ClientBuilder {
    addr: SocketAddr,
    server_name: String,
    provider: MtlsProvider,
    // default to 10000ms
    keepalive: u16,
}

impl ClientBuilder {
    const DEFAULT_KEEPALIVE_MS: u16 = 10000;
    pub fn new(addr: SocketAddr, server_name: &str, provider: MtlsProvider) -> Self {
        Self {
            addr,
            provider,
            keepalive: Self::DEFAULT_KEEPALIVE_MS,
            server_name: server_name.to_string(),
        }
    }

    pub fn keepalive(mut self, keepalive: u16) -> Self {
        self.keepalive = keepalive;
        self
    }

    pub async fn build(self) -> Result<Client> {
        // Channel for sending messages to the server
        let (server_tx, _server_rx) = mpsc::unbounded_channel();
        let conn_handle =
            ConnectionHandle::new(&self.addr, &self.server_name, self.provider, self.keepalive);

        Ok(Client {
            server_tx,
            consumer_id_gen: 0,
            conn_handle,
        })
    }
}

pub struct Client {
    consumer_id_gen: u64,
    server_tx: mpsc::UnboundedSender<OutgoingMessage>,
    conn_handle: ConnectionHandle,
}

impl Client {
    /// TODO use common/writer to send messages
    ///
    /// * ordered: use ordered writer per producer
    /// * unordered: use a global shared writer for all producers
    pub async fn new_producer(&self, topic: &str, name: &str, ordered: bool) -> Result<Producer> {
        Ok(Producer::new(topic, name, ordered, self.conn_handle.clone()).await?)
    }

    pub async fn new_consumer(&mut self, _subscribe: SubscribeMessage) -> Result<Consumer> {
        todo!()
    }
}
