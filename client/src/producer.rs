use std::{borrow::Borrow, net::AddrParseError, sync::Arc};

use bud_common::{protocol::ReturnCode, types::AccessMode};
use bytes::Bytes;
use log::{trace, warn};
use tokio::sync::oneshot;

use crate::{
    client::RetryOptions,
    connection::{self, Connection, ConnectionHandle},
    retry_op::producer_reconnect,
};

type Result<T> = std::result::Result<T, Error>;

#[derive(Debug, thiserror::Error)]
pub enum Error {
    /// error from connection
    #[error("Connection error: {0}")]
    Connection(#[from] connection::Error),
    /// parse SocketAddr
    #[error("Parse socket addr error: {0}")]
    SocketAddr(#[from] AddrParseError),
}

pub struct ProducerMessage {
    pub topic: String,
    pub sequence_id: u64,
    pub data: Bytes,
    pub res_tx: oneshot::Sender<connection::Result<ReturnCode>>,
}

pub struct Producer {
    id: u64,
    name: String,
    topic: String,
    access_mode: AccessMode,
    sequence_id: u64,
    /// connect to broker holding current topic
    conn: Arc<Connection>,
    /// use to get connection by broker addr
    conn_handle: ConnectionHandle,
    ordered: bool,
    retry_opts: Option<RetryOptions>,
}

impl Producer {
    pub async fn new(
        name: &str,
        id: u64,
        topic: &str,
        access_mode: AccessMode,
        ordered: bool,
        retry_opts: Option<RetryOptions>,
        conn_handle: ConnectionHandle,
    ) -> Result<Self> {
        trace!("get connection from lookup topic");
        let conn = conn_handle.lookup_topic(topic, ordered).await?;
        trace!("create new producer");
        let sequence_id = conn.create_producer(name, id, topic, access_mode).await?;
        Ok(Self {
            topic: topic.to_string(),
            sequence_id,
            conn,
            id,
            conn_handle,
            ordered,
            name: name.to_string(),
            access_mode,
            retry_opts,
        })
    }

    /// use by user to send messages
    pub async fn send(&mut self, data: &[u8]) -> Result<()> {
        loop {
            match self
                .conn
                .publish(self.id, &self.topic, self.sequence_id + 1, data)
                .await
            {
                Ok(_) => {
                    self.sequence_id += 1;
                    return Ok(());
                }
                Err(connection::Error::Disconnect) => {
                    if let Err(e) = self.conn.close_producer(self.id).await {
                        warn!("client CLOSE_PRODUCER error: {e}")
                    }
                    (self.conn, self.sequence_id) = producer_reconnect(
                        &self.retry_opts,
                        self.ordered,
                        &self.conn_handle,
                        &self.name,
                        self.id,
                        &self.topic,
                        self.access_mode,
                    )
                    .await?;
                }
                Err(e) => return Err(e)?,
            }
        }
    }

    pub async fn send_batch<T, A>(&mut self, data: T) -> Result<()>
    where
        T: Borrow<[A]>,
        A: Borrow<[u8]>,
    {
        loop {
            match self
                .conn
                .publish_batch(self.id, &self.topic, self.sequence_id + 1, data.borrow())
                .await
            {
                Ok(_) => {
                    self.sequence_id += data.borrow().len() as u64;
                    return Ok(());
                }
                Err(connection::Error::Disconnect) => {
                    if let Err(e) = self.conn.close_producer(self.id).await {
                        warn!("client CLOSE_PRODUCER error: {e}")
                    }
                    (self.conn, self.sequence_id) = producer_reconnect(
                        &self.retry_opts,
                        self.ordered,
                        &self.conn_handle,
                        &self.name,
                        self.id,
                        &self.topic,
                        self.access_mode,
                    )
                    .await?;
                }
                Err(e) => return Err(e)?,
            }
        }
    }

    pub async fn close(self) -> Result<()> {
        self.conn.close_producer(self.id).await?;
        Ok(())
    }
}
