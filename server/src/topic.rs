use std::{collections::HashMap, fmt::Display};

use bud_common::{
    protocol::{Publish, ReturnCode},
    storage::Storage,
};
use bytes::Bytes;
use tokio::sync::mpsc;

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

impl Display for Error {
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

pub struct Message {
    /// producer sequence id
    pub seq_id: u64,
    /// message payload
    pub payload: Bytes,
}

impl Message {
    pub fn from_publish(publish: Publish) -> Self {
        Self {
            seq_id: publish.sequence_id,
            payload: publish.payload,
        }
    }
}

pub struct SubscriptionId {
    pub topic: String,
    pub name: String,
}

/// Save all messages associated with this topic in subscription
/// Save subscription associated with this topic in memory
pub struct Topic<S> {
    /// topic name
    pub name: String,
    /// producer message sequence id
    seq_id: u64,
    /// all subscriptions in memory
    /// key = sub_name
    subscriptions: HashMap<String, Subscription<S>>,
    /// message storage
    storage: TopicStorage<S>,
    /// delete position
    delete_position: u64,
}

impl<S: Storage> Topic<S> {
    pub async fn new(
        topic: &str,
        send_tx: mpsc::UnboundedSender<SendEvent>,
        store: S,
    ) -> Result<Self> {
        let storage = TopicStorage::new(topic, store.clone())?;
        let seq_id = storage.get_sequence_id().await?.unwrap_or_default();

        let loaded_subscriptions = storage.all_aubscriptions().await?;
        let mut delete_position = u64::MAX;
        let mut subscriptions = HashMap::with_capacity(loaded_subscriptions.len());
        for sub in loaded_subscriptions {
            let subscription =
                Subscription::new(&sub.topic, &sub.name, send_tx.clone(), store.clone()).await?;
            let sub_delete_pos = subscription.delete_position().await;
            if sub_delete_pos < delete_position {
                delete_position = sub_delete_pos;
            }
            subscriptions.insert(sub.name, subscription);
        }
        Ok(Self {
            name: topic.to_string(),
            seq_id,
            subscriptions,
            storage,
            delete_position,
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
    pub async fn add_message(&mut self, message: Message) -> Result<()> {
        if message.seq_id <= self.seq_id {
            return Err(Error::ReturnCode(ReturnCode::ProduceMessageDuplicated));
        }
        let message_id = self.storage.add_message(&message).await?;
        self.seq_id = message.seq_id;
        for sub in self.subscriptions.values() {
            sub.message_notify(message_id)?;
        }
        Ok(())
    }

    pub async fn get_message(&self, message_id: u64) -> Result<Option<Message>> {
        Ok(self.storage.get_message(message_id).await?)
    }

    pub async fn consume_ack(&mut self, sub_name: &str, message_id: u64) -> Result<()> {
        let Some(sp) = self.subscriptions.get(sub_name) else {
            return Ok(());
        };

        // ack
        sp.consume_ack(message_id).await?;

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
