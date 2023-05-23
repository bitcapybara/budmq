use std::collections::HashMap;

use bud_common::{
    protocol::{Publish, ReturnCode},
    storage::Storage,
    types::{AccessMode, InitialPostion, SubType},
};
use bytes::Bytes;

use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;

use crate::{
    storage::{self, TopicStorage},
    subscription::{self, SendEvent, Subscription},
};

type Result<T> = std::result::Result<T, Error>;

#[derive(Debug)]
pub enum Error {
    ReturnCode(ReturnCode),
    Storage(storage::Error),
    Subscription(subscription::Error),
}

impl std::error::Error for Error {}

impl std::fmt::Display for Error {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Error::Storage(e) => write!(f, "Storage error: {e}"),
            Error::Subscription(e) => write!(f, "Subscription error: {e}"),
            Error::ReturnCode(code) => write!(f, "{code}"),
        }
    }
}

impl From<storage::Error> for Error {
    fn from(e: storage::Error) -> Self {
        Self::Storage(e)
    }
}

impl From<subscription::Error> for Error {
    fn from(e: subscription::Error) -> Self {
        Self::Subscription(e)
    }
}

pub struct TopicMessage {
    /// topic name
    pub topic_name: String,
    /// topic curosr id, topic unique
    pub topic_cursor_id: u64,
    /// producer sequence id
    pub seq_id: u64,
    /// message payload
    pub payload: Bytes,
}

impl TopicMessage {
    pub fn new(topic: &str, cursor_id: u64, seq_id: u64, payload: Bytes) -> Self {
        Self {
            topic_name: topic.to_string(),
            topic_cursor_id: cursor_id,
            seq_id,
            payload,
        }
    }
}

#[derive(Clone)]
pub struct Producer {
    id: u64,
    name: String,
    access_mode: AccessMode,
    sequence_id: u64,
}

pub enum Producers {
    Exclusive(Producer),
    Shared(HashMap<u64, Producer>),
}

pub struct TopicProducers(Option<Producers>);

impl TopicProducers {
    fn add_producer(&mut self, producer: &Producer) -> Result<()> {
        match self.0.as_mut() {
            Some(producers) => match (producers, producer.access_mode) {
                (Producers::Exclusive(_), AccessMode::Exclusive) => todo!(),
                (Producers::Shared(_), AccessMode::Shared) => todo!(),
                _ => todo!(),
            },
            None => match producer.access_mode {
                AccessMode::Exclusive => self.0 = Some(Producers::Exclusive(producer.clone())),
                AccessMode::Shared => {
                    let mut map = HashMap::new();
                    map.insert(producer.id, producer.clone());
                    self.0 = Some(Producers::Shared(map))
                }
            },
        }
        Ok(())
    }
}

pub struct SubscriptionInfo {
    pub topic: String,
    pub name: String,
    pub sub_type: SubType,
    pub init_position: InitialPostion,
}

/// Save all messages associated with this topic in subscription
/// Save subscription associated with this topic in memory
pub struct Topic<S> {
    /// topic name
    pub name: String,
    /// producer message sequence id
    latest_seq_id: u64,
    /// all subscriptions in memory
    /// key = sub_name
    subscriptions: HashMap<String, Subscription<S>>,
    /// producer
    producers: TopicProducers,
    /// message storage
    storage: TopicStorage<S>,
    /// delete position
    delete_position: u64,
    /// latest cursor id
    latest_cursor_id: u64,
}

impl<S: Storage> Topic<S> {
    pub async fn new(
        topic: &str,
        send_tx: mpsc::UnboundedSender<SendEvent>,
        store: S,
        token: CancellationToken,
    ) -> Result<Self> {
        let storage = TopicStorage::new(topic, store.clone())?;
        let latest_seq_id = storage.get_latest_sequence_id().await?.unwrap_or_default();
        let latest_cursor_id = storage.get_latest_cursor_id().await?.unwrap_or_default();

        let loaded_subscriptions = storage.all_aubscriptions().await?;
        let mut delete_position = if loaded_subscriptions.is_empty() {
            0
        } else {
            u64::MAX
        };
        let mut subscriptions = HashMap::with_capacity(loaded_subscriptions.len());
        for sub in loaded_subscriptions {
            let subscription = Subscription::new(
                &sub.topic,
                &sub.name,
                send_tx.clone(),
                store.clone(),
                sub.init_position,
                token.clone(),
            )
            .await?;
            let sub_delete_pos = subscription.delete_position().await;
            if sub_delete_pos < delete_position {
                delete_position = sub_delete_pos;
            }
            subscriptions.insert(sub.name, subscription);
        }
        Ok(Self {
            name: topic.to_string(),
            latest_seq_id,
            subscriptions,
            storage,
            delete_position,
            latest_cursor_id,
            producers: TopicProducers(None),
        })
    }

    pub fn add_subscription(&mut self, sub: Subscription<S>) {
        self.subscriptions.insert(sub.name.clone(), sub);
    }

    pub fn del_subscription(&mut self, sub_name: &str) -> Option<Subscription<S>> {
        self.subscriptions.remove(sub_name)
    }

    pub fn get_subscription(&self, sub_name: &str) -> Option<&Subscription<S>> {
        self.subscriptions.get(sub_name)
    }

    /// save message in topic
    pub async fn add_message(&mut self, message: &Publish) -> Result<()> {
        if message.sequence_id <= self.latest_seq_id {
            return Err(Error::ReturnCode(ReturnCode::ProduceMessageDuplicated));
        }
        self.latest_cursor_id += 1;
        let topic_message = TopicMessage::new(
            &self.name,
            self.latest_cursor_id,
            message.sequence_id,
            message.payload.clone(),
        );
        let message_id = self.storage.add_message(&topic_message).await?;
        self.latest_seq_id = message.sequence_id;
        for sub in self.subscriptions.values() {
            sub.message_notify(message_id)?;
        }
        Ok(())
    }

    pub async fn get_message(&self, message_id: u64) -> Result<Option<TopicMessage>> {
        Ok(self.storage.get_message(message_id).await?)
    }

    pub async fn consume_ack(&mut self, sub_name: &str, message_id: u64) -> Result<()> {
        let Some(sp) = self.subscriptions.get(sub_name) else {
            return Ok(());
        };

        // ack
        let Some(cursor_id) = self.storage.get_message_cursor_id(message_id).await? else {
            return Ok(());
        };
        sp.consume_ack(cursor_id).await?;

        // remove acked messages
        const DELETE_BATCH: u64 = 100;
        let mut lowest_mark = u64::MAX;
        for sub in self.subscriptions.values() {
            let delete_position = sub.delete_position().await;
            if delete_position < lowest_mark {
                lowest_mark = delete_position;
            }
            if delete_position - self.delete_position <= DELETE_BATCH {
                break;
            }
        }

        if lowest_mark - self.delete_position > DELETE_BATCH {
            self.storage.delete_range(..lowest_mark).await?;
        }
        Ok(())
    }
}
