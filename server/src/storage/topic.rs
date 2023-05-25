use std::{
    ops::RangeBounds,
    sync::atomic::{self, AtomicU64},
};

use bud_common::{storage::Storage, types::MessageId};

use crate::topic::{SubscriptionInfo, TopicMessage};

use super::{get_range, Codec, Result};

pub struct TopicStorage<S> {
    topic_name: String,
    storage: S,
    counter: AtomicU64,
}

impl<S: Storage> TopicStorage<S> {
    const TOPIC_KEY: &[u8] = "TOPIC".as_bytes();
    const PRODUCER_SEQUENCE_ID_KEY: &str = "PRODUCER_SEQUENCE_ID";
    const MAX_SUBSCRIPTION_ID_KEY: &[u8] = "SUBSCRIPTION".as_bytes();
    const LATEST_CURSOR_ID_KEY: &[u8] = "LATEST_CURSOR_ID".as_bytes();

    pub fn new(topic_name: &str, storage: S) -> Result<Self> {
        Ok(Self {
            topic_name: topic_name.to_string(),
            storage,
            counter: AtomicU64::new(1),
        })
    }

    pub async fn add_subscription(&self, sub: &SubscriptionInfo) -> Result<()> {
        let mut id_key = self.key(Self::MAX_SUBSCRIPTION_ID_KEY);
        let id = self
            .storage
            .get_u64(&id_key)
            .await?
            .map(|id| id + 1)
            .unwrap_or_default();
        id_key.extend_from_slice(&id.to_be_bytes());
        self.storage.put(&id_key, &sub.encode()).await?;
        Ok(())
    }

    pub async fn all_aubscriptions(&self) -> Result<Vec<SubscriptionInfo>> {
        let id_key = self.key(Self::MAX_SUBSCRIPTION_ID_KEY);
        let Some(max_id)= self.storage.get_u64(&id_key).await? else {
            return Ok(vec![]);
        };

        let mut subs = Vec::with_capacity(max_id as usize + 1);
        for i in 0..=max_id {
            let mut key = id_key.clone();
            key.extend_from_slice(&i.to_be_bytes());
            let Some(sub) = self.storage.get(&key).await? else {
                continue;
            };
            subs.push(SubscriptionInfo::decode(&sub)?);
        }
        Ok(subs)
    }

    pub async fn add_message(&self, message: &TopicMessage) -> Result<()> {
        let msg_id = self.counter.fetch_add(1, atomic::Ordering::SeqCst);
        let key = self.key(msg_id.to_be_bytes().as_slice());
        let value = message.encode();
        self.storage.put(&key, &value).await?;
        Ok(())
    }

    pub async fn get_message(&self, message_id: &MessageId) -> Result<Option<TopicMessage>> {
        let key = self.key(message_id.encode().as_slice());
        self.storage
            .get(&key)
            .await?
            .map(|b| TopicMessage::decode(&b))
            .transpose()
    }

    pub async fn delete_range<R>(&self, range: R) -> Result<()>
    where
        R: RangeBounds<u64>,
    {
        for i in get_range(range)? {
            self.storage
                .del(&self.key(i.to_be_bytes().as_slice()))
                .await?;
        }
        Ok(())
    }

    pub async fn get_sequence_id(&self, producer_name: &str) -> Result<Option<u64>> {
        let key = format!("{}-{}", Self::PRODUCER_SEQUENCE_ID_KEY, producer_name);
        let key = self.key(key.as_bytes());
        Ok(self.storage.get_u64(&key).await?)
    }

    pub async fn set_sequence_id(&self, producer_name: &str, seq_id: u64) -> Result<()> {
        let key = format!("{}-{}", Self::PRODUCER_SEQUENCE_ID_KEY, producer_name);
        let key = self.key(key.as_bytes());
        Ok(self
            .storage
            .put(&key, seq_id.to_be_bytes().as_slice())
            .await?)
    }

    pub async fn get_new_cursor_id(&self) -> Result<u64> {
        let key = self.key(Self::LATEST_CURSOR_ID_KEY);
        Ok(self.storage.fetch_add(&key, 1).await?)
    }

    fn key(&self, bytes: &[u8]) -> Vec<u8> {
        let mut key = format!("TOPIC-{}", self.topic_name).as_bytes().to_vec();
        key.extend_from_slice(bytes);
        key
    }
}
