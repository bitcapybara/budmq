use std::fmt::Display;

use bytes::Bytes;

use crate::{protocol::Publish, subscription::Subscription};

type Result<T> = std::result::Result<T, Error>;

#[derive(Debug)]
pub enum Error {}

impl std::error::Error for Error {}

impl Display for Error {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
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
    topic: String,
    seq_id: u64,
}

impl Topic {
    pub fn new(topic: &str) -> Self {
        Self {
            topic: topic.to_string(),
            seq_id: 0,
        }
    }

    pub fn add_subscription(&mut self, sub: Subscription) {
        todo!()
    }

    pub fn del_subscription(&mut self, sub_id: &str) -> Option<Subscription> {
        todo!()
    }

    pub fn get_subscription(&self, sub_id: &str) -> Option<&Subscription> {
        todo!()
    }

    pub fn get_mut_subscription(&self, sub_id: &str) -> Option<&mut Subscription> {
        todo!()
    }

    /// save message in topic
    pub fn add_message(&mut self, msg: Message) {
        // notify_tx send latest message id to subscriptions
        todo!()
    }
}
