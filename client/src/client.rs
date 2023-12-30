use std::{net::SocketAddr, time::Duration};

use bud_common::mtls::Certs;
use bud_common::{io::writer::Request, types::AccessMode};

use tokio::sync::mpsc;

use crate::{
    connection::{self, ConnectionHandle},
    consumer::{self, Consumer, SubscribeMessage},
    producer::{self, Producer},
};

type Result<T> = std::result::Result<T, Error>;

#[derive(Debug, thiserror::Error)]
pub enum Error {
    /// error from producer
    #[error("Producer error: {0}")]
    Producer(#[from] producer::Error),
    /// error from consumer
    #[error("Consumer error: {0}")]
    Consumer(#[from] consumer::Error),
    /// error from connection
    #[error("Connection error: {0}")]
    Connection(#[from] connection::Error),
}

#[derive(Clone)]
pub struct RetryOptions {
    pub max_retry_count: usize,
    pub min_retry_delay: Duration,
}

pub struct ClientBuilder {
    addr: SocketAddr,
    server_name: String,
    certs: Certs,
    // default to 10000ms
    keepalive: u16,
    retry_opts: Option<RetryOptions>,
}

impl ClientBuilder {
    const DEFAULT_KEEPALIVE_MS: u16 = 10000;
    pub fn new(addr: SocketAddr, server_name: &str, certs: Certs) -> Self {
        Self {
            addr,
            certs,
            keepalive: Self::DEFAULT_KEEPALIVE_MS,
            server_name: server_name.to_string(),
            retry_opts: None,
        }
    }

    pub fn keepalive(mut self, keepalive: u16) -> Self {
        self.keepalive = keepalive;
        self
    }

    pub fn retry(mut self, retry_opts: RetryOptions) -> Self {
        self.retry_opts = Some(retry_opts);
        self
    }

    pub async fn build(self) -> Result<Client> {
        // Channel for sending messages to the server
        let (server_tx, _server_rx) = mpsc::unbounded_channel();
        let conn_handle =
            ConnectionHandle::new(&self.addr, &self.server_name, self.certs, self.keepalive);

        Ok(Client {
            server_tx,
            consumer_id: 0,
            producer_id: 0,
            conn_handle,
            retry_opts: self.retry_opts,
        })
    }
}

pub struct Client {
    server_tx: mpsc::UnboundedSender<Request>,
    conn_handle: ConnectionHandle,
    consumer_id: u64,
    producer_id: u64,
    retry_opts: Option<RetryOptions>,
}

impl Client {
    pub async fn new_producer(
        &mut self,
        topic: &str,
        name: &str,
        access_mode: AccessMode,
        ordered: bool,
    ) -> Result<Producer> {
        self.producer_id += 1;
        Ok(Producer::new(
            name,
            self.producer_id,
            topic,
            access_mode,
            ordered,
            self.retry_opts.clone(),
            self.conn_handle.clone(),
        )
        .await?)
    }

    pub async fn new_consumer(
        &mut self,
        name: &str,
        subscribe: SubscribeMessage,
    ) -> Result<Consumer> {
        self.consumer_id += 1;
        Ok(Consumer::new(
            self.consumer_id,
            name,
            self.conn_handle.clone(),
            &subscribe,
            self.retry_opts.clone(),
        )
        .await?)
    }

    pub async fn close(&self) -> Result<()> {
        let conn = self.conn_handle.get_base_connection(false).await?;
        conn.disconnect().await?;
        Ok(())
    }
}
