use std::{collections::HashMap, fmt::Display};

use bytes::Bytes;

use crate::{
    protocol::Publish,
    storage::{self, TopicStorage},
    subscription::{self, Subscription},
};

type Result<T> = std::result::Result<T, Error>;

#[derive(Debug)]
pub enum Error {}

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
    pub seq_id: u64,
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
    name: String,
    seq_id: u64,
    /// all subscriptions in memory
    subscriptions: HashMap<String, Subscription>,
    /// message storage
    storage: TopicStorage,
}

impl Topic {
    pub fn new(topic: &str) -> Self {
        Self {
            name: topic.to_string(),
            seq_id: 0,
            subscriptions: HashMap::new(),
            storage: TopicStorage::new(),
        }
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

    pub fn get_mut_subscription(&self, sub_name: &str) -> Option<&mut Subscription> {
        self.subscriptions.get_mut(sub_name)
    }

    /// save message in topic
    pub fn add_message(&mut self, message: Message) -> Result<()> {
        let message_id = self.storage.add_message(message)?;
        for (_, sub) in self.subscriptions {
            sub.message_notify(message_id)?;
        }
        Ok(())
    }
}
