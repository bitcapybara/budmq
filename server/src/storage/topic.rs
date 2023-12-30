use std::sync::atomic::AtomicU64;

use bud_common::{
    storage::{MessageStorage, MetaStorage},
    types::{MessageId, SubscriptionInfo, TopicMessage},
};

use super::{Error, Result};

pub struct TopicStorage<M, S> {
    topic_name: String,
    meta_storage: M,
    message_storage: S,
    counter: AtomicU64,
}

impl<M: MetaStorage, S: MessageStorage> TopicStorage<M, S> {
    const TOPIC_KEY: &'static [u8] = "TOPIC".as_bytes();
    const PRODUCER_SEQUENCE_ID_KEY: &'static str = "PRODUCER_SEQUENCE_ID";
    const SUBSCRIPTION_NAMES_KEY: &'static str = "SUBSCRIPTION_NAMES";
    const SUBSCRIPTION_KEY: &'static str = "SUBSCRIPTION";
    const LATEST_CURSOR_ID_KEY: &'static str = "LATEST_CURSOR_ID";
    const MESSAGE_KEY: &'static str = "MESSAGE";

    pub fn new(topic_name: &str, meta_storage: M, message_storage: S) -> Result<Self> {
        Ok(Self {
            topic_name: topic_name.to_string(),
            meta_storage,
            counter: AtomicU64::new(1),
            message_storage,
        })
    }

    pub fn inner_meta(&self) -> M {
        self.meta_storage.clone()
    }

    pub fn inner_message(&self) -> S {
        self.message_storage.clone()
    }

    pub async fn add_subscription(&self, sub: &SubscriptionInfo) -> Result<()> {
        self.meta_storage
            .add_subscription(sub)
            .await
            .map_err(|e| Error::Storage(e.to_string()))?;
        Ok(())
    }

    pub async fn del_subscription(&self, sub_name: &str) -> Result<()> {
        self.meta_storage
            .del_subscription(&self.topic_name, sub_name)
            .await
            .map_err(|e| Error::Storage(e.to_string()))?;
        Ok(())
    }

    pub async fn all_aubscriptions(&self) -> Result<Vec<SubscriptionInfo>> {
        self.meta_storage
            .all_subscription(&self.topic_name)
            .await
            .map_err(|e| Error::Storage(e.to_string()))
    }

    pub async fn add_message(&self, message: &TopicMessage) -> Result<()> {
        self.message_storage
            .put_message(message)
            .await
            .map_err(|e| Error::Storage(e.to_string()))
    }

    pub async fn get_message(&self, message_id: &MessageId) -> Result<Option<TopicMessage>> {
        self.message_storage
            .get_message(message_id)
            .await
            .map_err(|e| Error::Storage(e.to_string()))
    }

    pub async fn del_message(&self, message_id: &MessageId) -> Result<()> {
        self.message_storage
            .del_message(message_id)
            .await
            .map_err(|e| Error::Storage(e.to_string()))
    }

    pub async fn get_sequence_id(&self, producer_name: &str) -> Result<Option<u64>> {
        let key = format!("{}-{}", Self::PRODUCER_SEQUENCE_ID_KEY, producer_name);
        let key = self.key(&key);
        self.meta_storage
            .get_u64(&key)
            .await
            .map_err(|e| Error::Storage(e.to_string()))
    }

    pub async fn set_sequence_id(&self, producer_name: &str, seq_id: u64) -> Result<()> {
        let key = format!("{}-{}", Self::PRODUCER_SEQUENCE_ID_KEY, producer_name);
        let key = self.key(&key);
        self.meta_storage
            .put_u64(&key, seq_id)
            .await
            .map_err(|e| Error::Storage(e.to_string()))
    }

    pub async fn get_new_cursor_id(&self) -> Result<u64> {
        let key = self.key(Self::LATEST_CURSOR_ID_KEY);
        self.meta_storage
            .inc_u64(&key, 1)
            .await
            .map_err(|e| Error::Storage(e.to_string()))
    }

    fn key(&self, s: &str) -> String {
        format!("TOPIC-{}-{}", self.topic_name, s)
    }
}
