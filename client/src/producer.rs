use std::sync::Arc;

use bud_common::{
    id::{next_id, SerialId},
    protocol::{CloseProducer, CreateProducer, Packet, ProducerReceipt, Publish, ReturnCode},
};
use bytes::Bytes;
use tokio::sync::{mpsc, oneshot};

use crate::connection::{self, writer::OutgoingMessage, Connection, ConnectionHandle};

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
    request_id: SerialId,
    sequence_id: u64,
    tx: mpsc::UnboundedSender<OutgoingMessage>,
    conn: Arc<Connection>,
    conn_handle: ConnectionHandle,
    ordered: bool,
}

impl Producer {
    pub async fn new(
        topic: &str,
        name: &str,
        ordered: bool,
        conn_handle: ConnectionHandle,
    ) -> Result<Self> {
        let (tx, rx) = mpsc::unbounded_channel();
        let conn = conn_handle.get_connection(rx, ordered).await?;
        let request_id = SerialId::new();
        // get seq_id from ProducerReceipt packet
        let (res_tx, res_rx) = oneshot::channel();
        tx.send(OutgoingMessage {
            packet: Packet::CreateProducer(CreateProducer {
                request_id: request_id.next(),
                producer_name: name.to_string(),
                topic_name: topic.to_string(),
            }),
            res_tx,
        })?;
        let (id, sequence_id) = match res_rx.await {
            Ok(Some(Packet::ProducerReceipt(ProducerReceipt {
                producer_id,
                sequence_id,
                ..
            }))) => (producer_id, sequence_id),
            Ok(Some(_)) => return Err(Error::Internal("received unexpected packet".to_string())),
            Ok(None) => return Err(Error::Internal("response not found".to_string())),
            Err(e) => return Err(e)?,
        };
        Ok(Self {
            topic: topic.to_string(),
            sequence_id,
            tx,
            conn,
            id,
            request_id,
            conn_handle,
            ordered,
            name: name.to_string(),
        })
    }

    /// use by user to send messages
    /// TODO send_timout()/send_async() method?
    pub async fn send(&mut self, data: &[u8]) -> Result<()> {
        loop {
            self.sequence_id += 1;
            let (res_tx, res_rx) = oneshot::channel();
            match self.tx.send(OutgoingMessage {
                packet: Packet::Publish(Publish {
                    topic: self.topic.clone(),
                    sequence_id: self.sequence_id,
                    payload: Bytes::copy_from_slice(data),
                    request_id: next_id(),
                }),
                res_tx,
            }) {
                Ok(_) => match res_rx.await {
                    Ok(Some(Packet::Response(resp))) => {
                        if resp.code == ReturnCode::Success {
                            return Ok(());
                        } else {
                            return Err(Error::FromServer(resp.code));
                        }
                    }
                    Ok(Some(_)) | Ok(None) => {
                        return Err(Error::Internal("Unexpected response".to_string()))
                    }
                    Err(_) => self.reconnect(true).await?,
                },
                Err(_) => {
                    self.reconnect(false).await?;
                }
            }
        }
    }

    pub async fn reconnect(&mut self, close_producer: bool) -> Result<()> {
        if close_producer {
            let (res_tx, res_rx) = oneshot::channel();
            self.tx.send(OutgoingMessage {
                packet: Packet::CloseProducer(CloseProducer {
                    producer_id: self.id,
                }),
                res_tx,
            })?;
            res_rx.await.ok();
        }

        let (tx, rx) = mpsc::unbounded_channel();
        self.conn = self.conn_handle.get_connection(rx, self.ordered).await?;
        self.tx = tx.clone();
        (self.id, self.sequence_id) =
            create_producer(tx, self.request_id.next(), &self.topic, &self.name).await?;
        Ok(())
    }
}

pub async fn create_producer(
    tx: mpsc::UnboundedSender<OutgoingMessage>,
    request_id: u64,
    topic: &str,
    name: &str,
) -> Result<(u64, u64)> {
    let (res_tx, res_rx) = oneshot::channel();
    tx.send(OutgoingMessage {
        packet: Packet::CreateProducer(CreateProducer {
            request_id,
            producer_name: name.to_string(),
            topic_name: topic.to_string(),
        }),
        res_tx,
    })?;
    let (id, sequence_id) = match res_rx.await {
        Ok(Some(Packet::ProducerReceipt(ProducerReceipt {
            producer_id,
            sequence_id,
            ..
        }))) => (producer_id, sequence_id),
        Ok(Some(_)) => return Err(Error::Internal("received unexpected packet".to_string())),
        Ok(None) => return Err(Error::Internal("response not found".to_string())),
        Err(e) => return Err(e)?,
    };
    Ok((id, sequence_id))
}
