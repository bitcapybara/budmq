use std::{collections::HashMap, fmt::Display};

use bytes::Bytes;

use crate::{
    protocol::Publish,
    storage::{self, TopicStorage},
    subscription::{self, Subscription},
};

type Result<T> = std::result::Result<T, Error>;

#[derive(Debug)]
pub enum Error {
    ProducerMessageDuplicated,
}

impl std::error::Error for Error {}

impl Display for Error {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        todo!()
    }
}

impl From<storage::Error> for Error {
    fn from(value: storage::Error) -> Self {
        todo!()
    }
}

impl From<subscription::Error> for Error {
    fn from(value: subscription::Error) -> Self {
        todo!()
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

/// Save all messages associated with this topic in subscription
/// Save subscription associated with this topic in memory
pub struct Topic {
    /// topic name
    pub name: String,
    /// producer message sequence id
    seq_id: u64,
    /// all subscriptions in memory
    /// key = sub_name
    subscriptions: HashMap<String, Subscription>,
    /// message storage
    storage: TopicStorage,
    /// delete position
    delete_position: u64,
}

impl Topic {
    pub fn new(topic: &str) -> Result<Self> {
        // TODO load from storage
        Ok(Self {
            name: topic.to_string(),
            seq_id: 0,
            subscriptions: HashMap::new(),
            storage: TopicStorage::new()?,
            delete_position: 0,
        })
    }

    pub fn add_subscription(&mut self, sub: Subscription) {
        self.subscriptions.insert(sub.name.clone(), sub);
    }

    pub fn del_subscription(&mut self, sub_name: &str) -> Option<Subscription> {
        self.subscriptions.remove(sub_name)
    }

    pub fn get_subscription(&self, sub_name: &str) -> Option<&Subscription> {
        self.subscriptions.get(sub_name)
    }

    /// save message in topic
    pub async fn add_message(&mut self, message: Message) -> Result<()> {
        if message.seq_id <= self.seq_id {
            return Err(Error::ProducerMessageDuplicated);
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
