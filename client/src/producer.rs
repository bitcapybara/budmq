use std::sync::Arc;

use bud_common::{id::SerialId, protocol::ReturnCode, types::AccessMode};
use bytes::Bytes;
use log::warn;
use tokio::sync::{mpsc, oneshot};

use crate::connection::{self, Connection, ConnectionHandle};

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
    request_id: SerialId,
    sequence_id: u64,
    conn: Arc<Connection>,
    conn_handle: ConnectionHandle,
    ordered: bool,
}

impl Producer {
    pub async fn new(
        name: &str,
        topic: &str,
        access_mode: AccessMode,
        ordered: bool,
        conn_handle: ConnectionHandle,
    ) -> Result<Self> {
        let conn = conn_handle.get_connection(ordered).await?;
        let request_id = SerialId::new();
        // get seq_id from ProducerReceipt packet
        let (id, sequence_id) = conn.create_producer(name, topic, access_mode).await?;
        Ok(Self {
            topic: topic.to_string(),
            sequence_id,
            conn,
            id,
            request_id,
            conn_handle,
            ordered,
            name: name.to_string(),
            access_mode,
        })
    }

    /// use by user to send messages
    /// TODO send_timout()/send_async() method?
    pub async fn send(&mut self, data: &[u8]) -> Result<()> {
        loop {
            self.sequence_id += 1;
            match self.conn.publish(&self.topic, self.sequence_id, data).await {
                Ok(_) => return Ok(()),
                Err(connection::Error::Disconnect) => self.reconnect().await?,
                Err(e) => return Err(e)?,
            }
        }
    }

    pub async fn close(self) -> Result<()> {
        self.conn.close_producer(self.id).await?;
        Ok(())
    }

    async fn reconnect(&mut self) -> Result<()> {
        if let Err(e) = self.conn.close_producer(self.id).await {
            warn!("client CLOSE_PRODUCER error: {e}")
        }

        // TODO loop and retry
        self.conn = self.conn_handle.get_connection(self.ordered).await?;
        (self.id, self.sequence_id) = self
            .conn
            .create_producer(&self.name, &self.topic, self.access_mode)
            .await?;
        Ok(())
    }
}
