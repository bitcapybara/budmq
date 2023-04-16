use std::{
    ops::RangeBounds,
    sync::atomic::{self, AtomicU64},
};

use libbud_common::storage::Storage;

use crate::topic::{Message, SubscriptionId};

use super::{get_range, Codec, Result};

pub struct TopicStorage<S> {
    topic_name: String,
    storage: S,
    counter: AtomicU64,
}

impl<S: Storage> TopicStorage<S> {
    const TOPIC_KEY: &[u8] = "TOPIC".as_bytes();
    const SEQUENCE_ID_KEY: &[u8] = "SEQUENCE_ID".as_bytes();
    const SUBSCRIPTION_KEY: &[u8] = "SUBSCRIPTION".as_bytes();

    pub fn new(topic_name: &str, storage: S) -> Result<Self> {
        Ok(Self {
            topic_name: topic_name.to_string(),
            storage,
            counter: AtomicU64::default(),
        })
    }

    pub async fn add_subscription(&self, sub: &SubscriptionId) -> Result<()> {
        let mut id_key = self.key(Self::SUBSCRIPTION_KEY);
        let id = self
            .storage
            .get_u64(&id_key)
            .await?
            .map(|id| id + 1)
            .unwrap_or_default();
        id_key.extend_from_slice(&id.to_be_bytes());
        self.storage.put(&id_key, &sub.to_vec()).await?;
        Ok(())
    }

    pub async fn all_aubscriptions(&self) -> Result<Vec<SubscriptionId>> {
        let id_key = self.key(Self::SUBSCRIPTION_KEY);
        let Some(max_id )= self.storage.get_u64(&id_key).await? else {
            return Ok(vec![]);
        };

        let mut subs = Vec::with_capacity(max_id as usize + 1);
        for i in 0..=max_id {
            let mut key = id_key.clone();
            key.extend_from_slice(&i.to_be_bytes());
            let Some(sub) = self.storage.get(&key).await? else {
                continue;
            };
            subs.push(SubscriptionId::from_bytes(&sub)?);
        }
        Ok(subs)
    }

    pub async fn add_message(&self, message: &Message) -> Result<u64> {
        let msg_id = self.counter.fetch_add(1, atomic::Ordering::SeqCst);
        let key = self.key(msg_id.to_be_bytes().as_slice());
        let value = message.to_vec();
        self.storage.put(&key, &value).await?;
        self.set_sequence_id(message.seq_id).await?;
        Ok(msg_id)
    }

    pub async fn get_message(&self, message_id: u64) -> Result<Option<Message>> {
        let key = self.key(message_id.to_be_bytes().as_slice());
        self.storage
            .get(&key)
            .await?
            .map(|b| Message::from_bytes(&b))
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

    pub async fn get_sequence_id(&self) -> Result<Option<u64>> {
        let key = self.key(Self::SEQUENCE_ID_KEY);
        Ok(self.storage.get_u64(&key).await?)
    }

    pub async fn set_sequence_id(&self, seq_id: u64) -> Result<()> {
        let key = self.key(Self::SEQUENCE_ID_KEY);
        self.storage
            .put(&key, seq_id.to_be_bytes().as_slice())
            .await?;
        Ok(())
    }

    fn key(&self, bytes: &[u8]) -> Vec<u8> {
        let mut key = format!("TOPIC-{}", self.topic_name).as_bytes().to_vec();
        key.extend_from_slice(bytes);
        key
    }
}