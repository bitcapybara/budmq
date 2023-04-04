mod cursor;
mod dispatcher;

use std::{collections::HashMap, fmt::Display};

use tokio::sync::{mpsc, oneshot};

use crate::{protocol::Subscribe, storage};

use self::dispatcher::{Dispatcher, Notify};

type Result<T> = std::result::Result<T, Error>;

#[derive(Debug)]
pub enum Error {
    SubscribeOnExclusive,
    SubTypeUnexpected,
    ReplyChannelClosed,
    SendOnDroppedChannel,
    Storage(storage::Error),
}

impl std::error::Error for Error {}

impl Display for Error {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Error::SubscribeOnExclusive => write!(f, "Subscribe on exclusive subscription"),
            Error::SubTypeUnexpected => write!(f, "Unexpected subscription type"),
            Error::ReplyChannelClosed => write!(f, "Wait reply on dropped channel"),
            Error::SendOnDroppedChannel => write!(f, "Send on dropped channel"),
            Error::Storage(e) => write!(f, "Storage error: {e}"),
        }
    }
}

impl From<oneshot::error::RecvError> for Error {
    fn from(_: oneshot::error::RecvError) -> Self {
        Self::ReplyChannelClosed
    }
}

impl<T> From<mpsc::error::SendError<T>> for Error {
    fn from(_: mpsc::error::SendError<T>) -> Self {
        Self::SendOnDroppedChannel
    }
}

impl From<storage::Error> for Error {
    fn from(e: storage::Error) -> Self {
        Self::Storage(e)
    }
}

#[derive(Debug, Clone, Copy)]
#[repr(u8)]
pub enum SubType {
    /// Each subscription is only allowed to contain one client
    Exclusive = 1,
    /// Each subscription allows multiple clients
    Shared,
}

#[derive(Debug, Clone, Copy)]
#[repr(u8)]
pub enum InitialPostion {
    Latest = 1,
    Earliest,
}

pub struct SendEvent {
    pub client_id: u64,
    pub topic_name: String,
    pub message_id: u64,
    pub consumer_id: u64,
    /// true if send to client successfully
    pub res_tx: oneshot::Sender<bool>,
}

#[derive(Debug, Clone)]
pub struct Consumer {
    client_id: u64,
    consumer_id: u64,
    permits: u32,
    topic_name: String,
    sub_name: String,
    sub_type: SubType,
    init_pos: InitialPostion,
}

impl Consumer {
    fn new(client_id: u64, consumer_id: u64, sub: &Subscribe) -> Self {
        Self {
            permits: 0,
            topic_name: sub.topic.clone(),
            sub_type: sub.sub_type,
            init_pos: sub.initial_position,
            client_id,
            consumer_id,
            sub_name: sub.sub_name.clone(),
        }
    }

    fn reduce_permit(&mut self) {
        self.permits -= 1;
    }
}

struct Consumers(Option<ConsumersType>);

impl Consumers {
    fn empty() -> Self {
        Self(None)
    }
    fn new(consumers: ConsumersType) -> Self {
        Self(Some(consumers))
    }
    fn from_consumer(consumer: Consumer) -> Self {
        let consumers_type = match consumer.sub_type {
            SubType::Exclusive => ConsumersType::Exclusive(consumer),
            SubType::Shared => {
                let mut map = HashMap::new();
                map.insert(consumer.client_id, consumer);
                ConsumersType::Shared(map)
            }
        };
        Self(Some(consumers_type))
    }
    fn set(&mut self, consumers: ConsumersType) {
        self.0 = Some(consumers)
    }
    fn clear(&mut self) {
        self.0.take();
    }
}

impl AsMut<Option<ConsumersType>> for Consumers {
    fn as_mut(&mut self) -> &mut Option<ConsumersType> {
        &mut self.0
    }
}

impl AsRef<Option<ConsumersType>> for Consumers {
    fn as_ref(&self) -> &Option<ConsumersType> {
        &self.0
    }
}

/// clients sub to this subscription
enum ConsumersType {
    Exclusive(Consumer),
    // key = client_id
    Shared(HashMap<u64, Consumer>),
}

impl From<Consumer> for ConsumersType {
    fn from(consumer: Consumer) -> Self {
        match consumer.sub_type {
            SubType::Exclusive => Self::Exclusive(consumer),
            SubType::Shared => {
                let mut consumers = HashMap::new();
                consumers.insert(consumer.client_id, consumer);
                Self::Shared(consumers)
            }
        }
    }
}

/// save cursor in persistent
/// save consumers in memory
pub struct Subscription {
    pub topic: String,
    pub name: String,
    dispatcher: Dispatcher,
    notify_tx: mpsc::UnboundedSender<Notify>,
}

impl Subscription {
    /// load from storage
    pub async fn new(
        topic: &str,
        sub_name: &str,
        send_tx: mpsc::UnboundedSender<SendEvent>,
    ) -> Result<Self> {
        let (notify_tx, notify_rx) = mpsc::unbounded_channel();
        let dispatcher = Dispatcher::new(sub_name).await?;
        tokio::spawn(dispatcher.clone().run(notify_rx, send_tx));
        Ok(Self {
            topic: topic.to_string(),
            name: sub_name.to_string(),
            dispatcher,
            notify_tx,
        })
    }

    pub async fn from_subscribe(
        client_id: u64,
        consumer_id: u64,
        sub: &Subscribe,
        send_tx: mpsc::UnboundedSender<SendEvent>,
    ) -> Result<Self> {
        // start dispatch
        let (notify_tx, notify_rx) = mpsc::unbounded_channel();
        let consumer = Consumer::new(client_id, consumer_id, sub);
        let dispatcher = Dispatcher::with_consumer(consumer).await?;
        tokio::spawn(dispatcher.clone().run(notify_rx, send_tx));
        Ok(Self {
            topic: sub.topic.clone(),
            name: sub.sub_name.clone(),
            notify_tx,
            dispatcher,
        })
    }

    pub async fn add_consumer(&self, client_id: u64, sub: &Subscribe) -> Result<()> {
        self.dispatcher
            .add_consumer(Consumer::new(client_id, sub.consumer_id, sub))
            .await
    }

    pub async fn del_consumer(&self, client_id: u64, consumer_id: u64) -> Result<()> {
        self.dispatcher.del_consumer(client_id, consumer_id).await;
        Ok(())
    }

    pub fn additional_permits(
        &self,
        client_id: u64,
        consumer_id: u64,
        add_permits: u32,
    ) -> Result<()> {
        self.notify_tx.send(Notify::AddPermits {
            client_id,
            consumer_id,
            add_permits,
        })?;
        Ok(())
    }

    pub fn message_notify(&self, message_id: u64) -> Result<()> {
        self.notify_tx.send(Notify::NewMessage(message_id))?;
        Ok(())
    }

    pub async fn consume_ack(&self, message_id: u64) -> Result<()> {
        self.dispatcher.consume_ack(message_id).await
    }

    pub async fn delete_position(&self) -> u64 {
        self.dispatcher.delete_position().await
    }
}
