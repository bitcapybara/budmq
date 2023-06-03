use std::sync::Arc;

use bud_common::{protocol::ReturnCode, types::AccessMode};
use bytes::Bytes;
use log::warn;
use tokio::sync::{mpsc, oneshot};

use crate::{
    client::RetryOptions,
    connection::{self, Connection, ConnectionHandle},
    retry_op::producer_reconnect,
};

type Result<T> = std::result::Result<T, Error>;

#[derive(Debug)]
pub enum Error {
    FromServer(ReturnCode),
    Internal(String),
    Connection(connection::Error),
}

impl std::error::Error for Error {}

impl std::fmt::Display for Error {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Error::FromServer(code) => write!(f, "receive from server: {code}"),
            Error::Internal(e) => write!(f, "internal error: {e}"),
            Error::Connection(e) => write!(f, "connector error: {e}"),
        }
    }
}

impl From<connection::Error> for Error {
    fn from(e: connection::Error) -> Self {
        Self::Connection(e)
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
    pub res_tx: oneshot::Sender<connection::Result<ReturnCode>>,
}

pub struct Producer {
    id: u64,
    name: String,
    topic: String,
    access_mode: AccessMode,
    sequence_id: u64,
    conn: Arc<Connection>,
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
        let (conn, sequence_id) = match conn_handle.get_connection(ordered).await {
            Ok(conn) => match conn.create_producer(name, id, topic, access_mode).await {
                Ok(sequence_id) => (conn, sequence_id),
                Err(connection::Error::Disconnect) => {
                    if let Err(e) = conn.close_producer(id).await {
                        warn!("client CLOSE_PRODUCER error: {e}")
                    }
                    producer_reconnect(
                        &retry_opts,
                        ordered,
                        &conn_handle,
                        name,
                        id,
                        topic,
                        access_mode,
                    )
                    .await?
                }
                Err(e) => return Err(e.into()),
            },
            Err(connection::Error::Disconnect) => {
                producer_reconnect(
                    &retry_opts,
                    ordered,
                    &conn_handle,
                    name,
                    id,
                    topic,
                    access_mode,
                )
                .await?
            }
            Err(e) => return Err(e.into()),
        };
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
            self.sequence_id += 1;
            match self
                .conn
                .publish(self.id, &self.topic, self.sequence_id, data)
                .await
            {
                Ok(_) => return Ok(()),
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
