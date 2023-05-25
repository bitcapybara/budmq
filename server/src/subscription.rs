mod cursor;
mod dispatcher;

use std::collections::HashMap;

use bud_common::{
    protocol::Subscribe,
    storage::Storage,
    types::{InitialPostion, MessageId, SubType},
};
use tokio::{
    sync::{mpsc, oneshot},
    task::JoinHandle,
};
use tokio_util::sync::CancellationToken;

use crate::storage;

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

impl std::fmt::Display for Error {
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

pub struct SendEvent {
    pub client_id: u64,
    pub topic_name: String,
    pub message_id: MessageId,
    pub consumer_id: u64,
    /// true if send to client successfully
    pub res_tx: oneshot::Sender<bool>,
}

impl std::fmt::Display for SendEvent {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            r#"{{ "client_id": {}, "consumer_id": {}, "topic": {}, "message_id": {:?} }}"#,
            self.client_id, self.consumer_id, self.topic_name, self.message_id
        )
    }
}

#[derive(Debug, Clone)]
pub struct Consumer {
    id: u64,
    client_id: u64,
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
            id: consumer_id,
            sub_name: sub.sub_name.clone(),
        }
    }

    fn reduce_permit(&mut self) {
        self.permits -= 1;
    }
}

struct TopicConsumers(Option<Consumers>);

impl TopicConsumers {
    fn empty() -> Self {
        Self(None)
    }
    fn new(consumers: Consumers) -> Self {
        Self(Some(consumers))
    }
    fn from_consumer(consumer: Consumer) -> Self {
        let consumers_type = match consumer.sub_type {
            SubType::Exclusive => Consumers::Exclusive(consumer),
            SubType::Shared => {
                let mut map = HashMap::new();
                map.insert(consumer.client_id, consumer);
                Consumers::Shared(map)
            }
        };
        Self(Some(consumers_type))
    }
    fn set(&mut self, consumers: Consumers) {
        self.0 = Some(consumers)
    }
    fn clear(&mut self) {
        self.0.take();
    }

    fn is_empty(&self) -> bool {
        self.0.is_none()
    }
}

impl AsMut<Option<Consumers>> for TopicConsumers {
    fn as_mut(&mut self) -> &mut Option<Consumers> {
        &mut self.0
    }
}

impl AsRef<Option<Consumers>> for TopicConsumers {
    fn as_ref(&self) -> &Option<Consumers> {
        &self.0
    }
}

/// clients sub to this subscription
enum Consumers {
    Exclusive(Consumer),
    // key = client_id
    Shared(HashMap<u64, Consumer>),
}

impl From<Consumer> for Consumers {
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
pub struct Subscription<S> {
    pub topic_id: u64,
    pub topic: String,
    pub name: String,
    dispatcher: Dispatcher<S>,
    handle: JoinHandle<Result<()>>,
    token: CancellationToken,
    notify_tx: mpsc::UnboundedSender<Notify>,
}

impl<S: Storage> Subscription<S> {
    /// load from storage
    pub async fn new(
        topic_id: u64,
        topic: &str,
        sub_name: &str,
        send_tx: mpsc::Sender<SendEvent>,
        storage: S,
        init_position: InitialPostion,
        token: CancellationToken,
    ) -> Result<Self> {
        let (notify_tx, notify_rx) = mpsc::unbounded_channel();
        let dispatcher = Dispatcher::new(
            topic_id,
            sub_name,
            storage,
            init_position,
            send_tx,
            token.child_token(),
        )
        .await?;
        let handle = tokio::spawn(dispatcher.clone().run(notify_rx));
        Ok(Self {
            topic: topic.to_string(),
            name: sub_name.to_string(),
            dispatcher,
            notify_tx,
            handle,
            token,
            topic_id,
        })
    }

    #[allow(clippy::too_many_arguments)]
    pub async fn from_subscribe(
        topic_id: u64,
        client_id: u64,
        consumer_id: u64,
        sub: &Subscribe,
        send_tx: mpsc::Sender<SendEvent>,
        storage: S,
        init_position: InitialPostion,
        token: CancellationToken,
    ) -> Result<Self> {
        // start dispatch
        let (notify_tx, notify_rx) = mpsc::unbounded_channel();
        let consumer = Consumer::new(client_id, consumer_id, sub);
        let dispatcher = Dispatcher::with_consumer(
            topic_id,
            consumer,
            storage,
            init_position,
            send_tx,
            token.child_token(),
        )
        .await?;
        let handle = tokio::spawn(dispatcher.clone().run(notify_rx));
        Ok(Self {
            topic: sub.topic.clone(),
            name: sub.sub_name.clone(),
            notify_tx,
            dispatcher,
            handle,
            token,
            topic_id,
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

    pub fn message_notify(&self) -> Result<()> {
        self.notify_tx.send(Notify::NewMessage)?;
        Ok(())
    }

    pub async fn consume_ack(&self, cursor_id: u64) -> Result<()> {
        self.dispatcher.consume_ack(cursor_id).await
    }

    pub async fn delete_position(&self) -> u64 {
        self.dispatcher.delete_position().await
    }
}

impl<S> Drop for Subscription<S> {
    fn drop(&mut self) {
        self.token.cancel();
    }
}
