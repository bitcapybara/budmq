use std::{
    collections::HashMap,
    sync::{
        atomic::{AtomicU32, Ordering},
        Arc,
    },
};

use bytes::Bytes;
use libbud_common::protocol::{ControlFlow, Packet};
use tokio::sync::{mpsc, oneshot, RwLock};

use crate::connector::OutgoingMessage;

pub const CONSUME_CHANNEL_CAPACITY: u32 = 1000;

type Result<T> = std::result::Result<T, Error>;

#[derive(Debug)]
pub enum Error {}

impl std::error::Error for Error {}

impl std::fmt::Display for Error {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        todo!()
    }
}

impl<T> From<mpsc::error::SendError<T>> for Error {
    fn from(value: mpsc::error::SendError<T>) -> Self {
        todo!()
    }
}

impl From<oneshot::error::RecvError> for Error {
    fn from(value: oneshot::error::RecvError) -> Self {
        todo!()
    }
}

pub enum SubType {
    Exclusive,
    Shared,
}

pub struct Subscribe {
    topic: String,
    sub_name: String,
    sub_type: SubType,
}

pub struct ConsumeMessage {
    pub payload: Bytes,
}

#[derive(Clone)]
pub struct ConsumerSender {
    consumer_id: u64,
    permits: Arc<AtomicU32>,
    server_tx: mpsc::UnboundedSender<OutgoingMessage>,
    consumer_tx: mpsc::UnboundedSender<ConsumeMessage>,
}

impl ConsumerSender {
    pub fn new(
        consumer_id: u64,
        permits: Arc<AtomicU32>,
        server_tx: mpsc::UnboundedSender<OutgoingMessage>,
        consumer_tx: mpsc::UnboundedSender<ConsumeMessage>,
    ) -> Self {
        Self {
            permits,
            consumer_tx,
            server_tx,
            consumer_id,
        }
    }

    pub fn send(&self, message: ConsumeMessage) -> Result<()> {
        self.consumer_tx.send(message)?;
        let prev = self.permits.fetch_sub(1, Ordering::SeqCst);
        let (res_tx, res_rx) = oneshot::channel();
        if prev <= CONSUME_CHANNEL_CAPACITY / 2 {
            self.server_tx.send(OutgoingMessage {
                packet: Packet::ControlFlow(ControlFlow {
                    consumer_id: self.consumer_id,
                    permits: self.permits.load(Ordering::SeqCst),
                }),
                res_tx,
            })?;
        }
        Ok(())
    }
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
    permits: Arc<AtomicU32>,
    server_tx: mpsc::UnboundedSender<OutgoingMessage>,
    consumer_rx: mpsc::UnboundedReceiver<ConsumeMessage>,
}

impl Consumer {
    pub async fn new(
        id: u64,
        permits: Arc<AtomicU32>,
        sub: &Subscribe,
        server_tx: mpsc::UnboundedSender<OutgoingMessage>,
        consumer_rx: mpsc::UnboundedReceiver<ConsumeMessage>,
    ) -> Result<Self> {
        // send permits packet on init
        let (res_tx, res_rx) = oneshot::channel();
        server_tx.send(OutgoingMessage {
            packet: Packet::ControlFlow(ControlFlow {
                consumer_id: id,
                permits: permits.load(Ordering::SeqCst),
            }),
            res_tx,
        })?;
        let res = res_rx.await?;

        Ok(Self {
            id,
            permits,
            server_tx,
            consumer_rx,
        })
    }

    pub async fn next(&mut self) -> Option<ConsumeMessage> {
        // check permits and send
        let msg = self.consumer_rx.recv().await;
        if msg.is_some() {
            self.permits.fetch_add(1, Ordering::SeqCst);
        }
        msg
    }
}
