use std::sync::Arc;

use bud_common::{
    protocol::ReturnCode,
    types::{InitialPostion, MessageId, SubType},
};
use bytes::Bytes;
use chrono::{DateTime, Utc};
use log::warn;
use tokio::{
    select,
    sync::{mpsc, oneshot},
};
use tokio_util::sync::CancellationToken;

use crate::{
    client::RetryOptions,
    connection::{self, Connection, ConnectionHandle},
    retry_op::consumer_reconnect,
};

pub const CONSUME_CHANNEL_CAPACITY: u32 = 1000;

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
            Error::Connection(e) => write!(f, "connection error: {e}"),
        }
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

impl From<connection::Error> for Error {
    fn from(e: connection::Error) -> Self {
        Self::Connection(e)
    }
}

#[derive(Clone)]
pub struct SubscribeMessage {
    pub topic: String,
    pub sub_name: String,
    pub sub_type: SubType,
    pub initial_postion: InitialPostion,
}

pub struct ConsumeMessage {
    pub id: MessageId,
    pub payload: Bytes,
    pub produce_time: DateTime<Utc>,
    pub send_time: DateTime<Utc>,
    pub receive_time: DateTime<Utc>,
}

pub enum ConsumerEvent {
    Ack(MessageId),
    Unsubscribe,
    Close,
}

pub struct ConsumeEngine {
    id: u64,
    name: String,
    sub_message: SubscribeMessage,
    /// receive message from server
    server_rx: mpsc::UnboundedReceiver<ConsumeMessage>,
    /// send message to Consumer
    consumer_tx: mpsc::Sender<ConsumeMessage>,
    /// receive message from user, send to server
    event_rx: mpsc::Receiver<ConsumerEvent>,
    /// send message to server
    conn: Arc<Connection>,
    /// get new connection
    conn_handle: ConnectionHandle,
    /// remain permits
    remain_permits: u32,
    /// retry options
    retry_opts: Option<RetryOptions>,
    /// token to notify exit
    token: CancellationToken,
}

impl ConsumeEngine {
    #[allow(clippy::too_many_arguments)]
    async fn new(
        id: u64,
        name: &str,
        sub_message: &SubscribeMessage,
        event_rx: mpsc::Receiver<ConsumerEvent>,
        consumer_tx: mpsc::Sender<ConsumeMessage>,
        conn_handle: ConnectionHandle,
        retry_opts: Option<RetryOptions>,
        token: CancellationToken,
    ) -> Result<Self> {
        let conn = conn_handle.get_connection(false).await?;
        let (server_tx, server_rx) = mpsc::unbounded_channel();
        conn.subscribe(id, name, sub_message, server_tx).await?;
        conn.control_flow(id, CONSUME_CHANNEL_CAPACITY).await?;
        Ok(Self {
            id,
            sub_message: sub_message.clone(),
            consumer_tx,
            conn,
            conn_handle,
            remain_permits: 0,
            token,
            event_rx,
            server_rx,
            name: name.to_string(),
            retry_opts,
        })
    }

    async fn run(mut self) -> Result<()> {
        loop {
            if !self.conn.is_valid() && (self.conn.error().await).is_some() {
                if let Err(e) = self.conn.close_consumer(self.id).await {
                    warn!("client send CLOSE_CONSUMER packet error: {e}");
                }
                consumer_reconnect(
                    self.id,
                    &self.name,
                    &self.sub_message,
                    &self.retry_opts,
                    &self.conn_handle,
                )
                .await?;
                self.remain_permits = CONSUME_CHANNEL_CAPACITY;
            }

            if self.remain_permits < CONSUME_CHANNEL_CAPACITY / 2 {
                let permits = CONSUME_CHANNEL_CAPACITY - self.remain_permits;
                match self.conn.control_flow(self.id, permits).await {
                    Ok(_) => {}
                    Err(connection::Error::Disconnect) => {
                        if let Err(e) = self.conn.close_consumer(self.id).await {
                            warn!("client send CLOSE_CONSUMER packet error: {e}");
                        }
                        consumer_reconnect(
                            self.id,
                            &self.name,
                            &self.sub_message,
                            &self.retry_opts,
                            &self.conn_handle,
                        )
                        .await?;
                    }
                    Err(e) => return Err(e.into()),
                }
            }
            self.remain_permits = CONSUME_CHANNEL_CAPACITY;

            select! {
                res = self.server_rx.recv() => {
                    let Some(message) = res else {
                        return Ok(());
                    };
                    self.remain_permits -= 1;
                    self.consumer_tx.send(message).await?;
                }
                res = self.event_rx.recv() => {
                    let Some(event) = res else {
                        return Ok(());
                    };
                    match event {
                        ConsumerEvent::Ack(message_id) => {
                            self.conn.ack(self.id, &message_id).await?;
                        },
                        ConsumerEvent::Unsubscribe => {
                            self.conn.unsubscribe(self.id).await?;
                        },
                        ConsumerEvent::Close => {
                            self.conn.close_consumer(self.id).await?;
                            return Ok(())
                        }
                    }
                }
                _ = self.token.cancelled() => {
                    return Ok(())
                }
            }
        }
    }
}

pub struct Consumer {
    /// remaining space of the channel
    consumer_rx: mpsc::Receiver<ConsumeMessage>,
    /// send event to server
    event_tx: mpsc::Sender<ConsumerEvent>,
    /// retry options used in engine
    retry_opts: Option<RetryOptions>,
    /// token
    token: CancellationToken,
}

impl Consumer {
    pub async fn new(
        id: u64,
        name: &str,
        conn_handle: ConnectionHandle,
        sub_message: &SubscribeMessage,
        retry_opts: Option<RetryOptions>,
    ) -> Result<Self> {
        let (tx, rx) = mpsc::channel(CONSUME_CHANNEL_CAPACITY as usize);
        let (event_tx, event_rx) = mpsc::channel(1);
        let token = CancellationToken::new();
        let engine = ConsumeEngine::new(
            id,
            name,
            sub_message,
            event_rx,
            tx,
            conn_handle,
            retry_opts.clone(),
            token.clone(),
        )
        .await?;
        tokio::spawn(engine.run());
        Ok(Self {
            consumer_rx: rx,
            event_tx,
            token,
            retry_opts,
        })
    }

    pub async fn next(&mut self) -> Option<ConsumeMessage> {
        self.consumer_rx.recv().await
    }

    pub async fn ack(&self, message_id: &MessageId) -> Result<()> {
        self.event_tx.send(ConsumerEvent::Ack(*message_id)).await?;
        Ok(())
    }

    pub async fn unsubscribe(&self) -> Result<()> {
        self.event_tx.send(ConsumerEvent::Unsubscribe).await?;
        Ok(())
    }

    pub async fn close(self) -> Result<()> {
        self.event_tx.send(ConsumerEvent::Close).await?;
        self.token.cancel();
        Ok(())
    }
}
