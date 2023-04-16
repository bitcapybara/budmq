use std::{
    collections::HashMap,
    sync::{
        atomic::{AtomicU32, Ordering},
        Arc,
    },
};

use bytes::Bytes;
use libbud_common::{
    protocol::{ConsumeAck, ControlFlow, Packet, ReturnCode, Subscribe},
    subscription::{InitialPostion, SubType},
};
use tokio::sync::{mpsc, oneshot, RwLock};

use crate::connector::{self, ConsumerSender, OutgoingMessage};

pub const CONSUME_CHANNEL_CAPACITY: u32 = 1000;

type Result<T> = std::result::Result<T, Error>;

#[derive(Debug)]
pub enum Error {
    FromServer(ReturnCode),
    Internal(String),
    Connector(connector::Error),
}

impl std::error::Error for Error {}

impl std::fmt::Display for Error {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Error::FromServer(code) => write!(f, "receive from server: {code}"),
            Error::Internal(e) => write!(f, "internal error: {e}"),
            Error::Connector(e) => write!(f, "connector error: {e}"),
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

impl From<connector::Error> for Error {
    fn from(e: connector::Error) -> Self {
        Self::Connector(e)
    }
}

pub struct SubscribeMessage {
    pub topic: String,
    pub sub_name: String,
    pub sub_type: SubType,
    pub initial_postion: InitialPostion,
}

pub struct ConsumeMessage {
    pub payload: Bytes,
}

#[derive(Clone)]
pub struct Consumers(Arc<RwLock<HashMap<u64, ConsumerSender>>>);

impl Consumers {
    pub fn new() -> Self {
        Self(Arc::new(RwLock::new(HashMap::new())))
    }
    pub async fn add_consumer(&self, consumer_id: u64, consumer_sender: ConsumerSender) {
        let mut consumers = self.0.write().await;
        consumers.insert(consumer_id, consumer_sender);
    }

    pub async fn get_consumer(&self, consumer_id: u64) -> Option<ConsumerSender> {
        let consumers = self.0.read().await;
        consumers.get(&consumer_id).cloned()
    }
}

pub struct Consumer {
    pub id: u64,
    /// remaining space of the channel
    permits: Arc<AtomicU32>,
    server_tx: mpsc::UnboundedSender<OutgoingMessage>,
    consumer_rx: mpsc::UnboundedReceiver<ConsumeMessage>,
}

impl Consumer {
    pub async fn new(
        id: u64,
        permits: Arc<AtomicU32>,
        sub: SubscribeMessage,
        server_tx: mpsc::UnboundedSender<OutgoingMessage>,
        consumer_rx: mpsc::UnboundedReceiver<ConsumeMessage>,
    ) -> Result<Self> {
        // send permits packet on init
        let (permits_res_tx, permits_res_rx) = oneshot::channel();
        server_tx.send(OutgoingMessage {
            packet: Packet::ControlFlow(ControlFlow {
                consumer_id: id,
                permits: permits.load(Ordering::SeqCst),
            }),
            res_tx: permits_res_tx,
        })?;
        match permits_res_rx.await?? {
            ReturnCode::Success => {}
            code => return Err(Error::FromServer(code)),
        }
        // send subscribe message
        let (sub_res_tx, sub_res_rx) = oneshot::channel();
        server_tx.send(OutgoingMessage {
            packet: Packet::Subscribe(Subscribe {
                topic: sub.topic,
                sub_name: sub.sub_name,
                sub_type: sub.sub_type,
                consumer_id: id,
                initial_position: sub.initial_postion,
            }),
            res_tx: sub_res_tx,
        })?;
        match sub_res_rx.await?? {
            ReturnCode::Success => {}
            code => return Err(Error::FromServer(code)),
        }
        Ok(Self {
            id,
            permits,
            server_tx,
            consumer_rx,
        })
    }

    pub async fn next(&mut self) -> Option<ConsumeMessage> {
        let msg = self.consumer_rx.recv().await;
        if msg.is_some() {
            self.permits.fetch_add(1, Ordering::SeqCst);
        }
        msg
    }

    pub async fn ack(&self, message_id: u64) -> Result<()> {
        let (res_tx, res_rx) = oneshot::channel();
        self.server_tx.send(OutgoingMessage {
            packet: Packet::ConsumeAck(ConsumeAck {
                consumer_id: self.id,
                message_id,
            }),
            res_tx,
        })?;
        match res_rx.await?? {
            ReturnCode::Success => Ok(()),
            code => Err(Error::FromServer(code)),
        }
    }
}
