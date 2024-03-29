mod cursor;
mod dispatcher;

use std::{borrow::Borrow, collections::HashMap};

use bud_common::{
    protocol::{ReturnCode, Subscribe},
    storage::{MessageStorage, MetaStorage},
    types::{InitialPostion, MessageId, SubType},
};
use log::error;
use tokio::sync::{
    mpsc::{self, error::TrySendError},
    oneshot,
};
use tokio_util::sync::CancellationToken;

use crate::storage;

use self::dispatcher::Dispatcher;

pub type Result<T> = std::result::Result<T, Error>;

#[derive(Debug, thiserror::Error)]
pub enum Error {
    /// error return to client
    #[error("Response error: {0}")]
    Response(ReturnCode),
    /// error from storage
    #[error("Storage error: {0}")]
    Storage(#[from] storage::Error),
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
    default_permits: u32,
    weight: u8,
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
            default_permits: sub.default_permits,
            weight: sub.weight.unwrap_or(5),
        }
    }

    fn reduce_permit(&mut self) {
        self.permits -= 1;
    }
}

struct SubscriptionConsumers(Option<Consumers>);

impl SubscriptionConsumers {
    fn empty() -> Self {
        Self(None)
    }
    fn new(consumers: Consumers) -> Self {
        Self(Some(consumers))
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

impl From<Consumer> for SubscriptionConsumers {
    fn from(consumer: Consumer) -> Self {
        SubscriptionConsumers(Some(consumer.into()))
    }
}

impl AsMut<Option<Consumers>> for SubscriptionConsumers {
    fn as_mut(&mut self) -> &mut Option<Consumers> {
        &mut self.0
    }
}

impl AsRef<Option<Consumers>> for SubscriptionConsumers {
    fn as_ref(&self) -> &Option<Consumers> {
        &self.0
    }
}

/// clients sub to this subscription
enum Consumers {
    Exclusive(Consumer),
    /// key1 = client_id, key2 = consumer_id
    Shared(HashMap<u64, HashMap<u64, Consumer>>),
}

impl From<Consumer> for Consumers {
    fn from(consumer: Consumer) -> Self {
        match consumer.sub_type {
            SubType::Exclusive => Self::Exclusive(consumer),
            SubType::Shared => {
                let client_id = consumer.client_id;
                let mut clients = HashMap::new();
                let mut consumers = HashMap::new();
                consumers.insert(consumer.id, consumer);
                clients.insert(client_id, consumers);
                Self::Shared(clients)
            }
        }
    }
}

/// save cursor in persistent
/// save consumers in memory
pub struct Subscription<S1, S2> {
    pub topic_id: u64,
    pub topic: String,
    pub name: String,
    dispatcher: Dispatcher<S1, S2>,
    token: CancellationToken,
    notify_tx: mpsc::Sender<()>,
}

impl<S1: MetaStorage, S2: MessageStorage> Subscription<S1, S2> {
    /// load from storage
    #[allow(clippy::too_many_arguments)]
    pub async fn new(
        topic_id: u64,
        topic: &str,
        sub_name: &str,
        send_tx: mpsc::Sender<SendEvent>,
        meta_storage: S1,
        message_storage: S2,
        init_position: InitialPostion,
        token: CancellationToken,
    ) -> Result<Self> {
        let (notify_tx, notify_rx) = mpsc::channel(1);
        let dispatcher = Dispatcher::new(
            topic_id,
            topic,
            sub_name,
            meta_storage,
            message_storage,
            init_position,
            send_tx,
            token.child_token(),
        )
        .await?;
        tokio::spawn(dispatcher.clone().run(notify_rx));
        Ok(Self {
            topic: topic.to_string(),
            name: sub_name.to_string(),
            dispatcher,
            notify_tx,
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
        meta_storage: S1,
        message_storage: S2,
        token: CancellationToken,
    ) -> Result<Self> {
        // start dispatch
        let (notify_tx, notify_rx) = mpsc::channel(1);
        let consumer = Consumer::new(client_id, consumer_id, sub);
        let dispatcher = Dispatcher::with_consumer(
            topic_id,
            &sub.topic,
            consumer,
            meta_storage,
            message_storage,
            sub.initial_position,
            send_tx,
            token.child_token(),
        )
        .await?;
        tokio::spawn(dispatcher.clone().run(notify_rx));
        Ok(Self {
            topic: sub.topic.clone(),
            name: sub.sub_name.clone(),
            notify_tx,
            dispatcher,
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

    pub async fn additional_permits(&self, client_id: u64, consumer_id: u64, add_permits: u32) {
        self.dispatcher
            .increase_consumer_permits(client_id, consumer_id, add_permits)
            .await;
        match self.notify_tx.try_send(()) {
            Ok(_) | Err(TrySendError::Full(_)) => {}
            Err(_) => {
                error!("Dispatcher task exited")
            }
        }
    }

    pub async fn add_message(&self, cursor_id: u64) -> Result<()> {
        self.dispatcher.update_message_id(cursor_id).await
    }

    pub fn message_notify(&self) {
        match self.notify_tx.try_send(()) {
            Ok(_) | Err(TrySendError::Full(_)) => {}
            Err(_) => {
                error!("Dispatcher task exited")
            }
        }
    }

    pub async fn consume_ack<T>(&self, cursor_ids: T) -> Result<()>
    where
        T: Borrow<[u64]>,
    {
        self.dispatcher.consume_ack(cursor_ids).await
    }
}

impl<S1, S2> Drop for Subscription<S1, S2> {
    fn drop(&mut self) {
        self.token.cancel();
    }
}
