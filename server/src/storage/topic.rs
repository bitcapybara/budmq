use std::{ops::RangeBounds, sync::atomic::AtomicU64};

use bud_common::{
    codec::Codec,
    storage::Storage,
    types::{MessageId, SubscriptionInfo, TopicMessage},
};
use bytes::{Bytes, BytesMut};

use super::{get_range, Result};

pub struct TopicStorage<S> {
    topic_name: String,
    storage: S,
    counter: AtomicU64,
}

impl<S: Storage> TopicStorage<S> {
    const TOPIC_KEY: &[u8] = "TOPIC".as_bytes();
    const PRODUCER_SEQUENCE_ID_KEY: &str = "PRODUCER_SEQUENCE_ID";
    const MAX_SUBSCRIPTION_ID_KEY: &str = "MAX_SUBSCRIPTION_ID";
    const SUBSCRIPTION_KEY: &str = "SUBSCRIPTION";
    const LATEST_CURSOR_ID_KEY: &str = "LATEST_CURSOR_ID";
    const MESSAGE_KEY: &str = "MESSAGE";

    pub fn new(topic_name: &str, storage: S) -> Result<Self> {
        Ok(Self {
            topic_name: topic_name.to_string(),
            storage,
            counter: AtomicU64::new(1),
        })
    }

    pub fn inner(&self) -> S {
        self.storage.clone()
    }

    pub async fn add_subscription(&self, sub: &SubscriptionInfo) -> Result<()> {
        let id_key = self.key(Self::MAX_SUBSCRIPTION_ID_KEY);
        let id = self.storage.inc_u64(&id_key, 1).await?;
        let id_key = self.key(&format!("{}-{}", Self::SUBSCRIPTION_KEY, id));
        let mut buf = BytesMut::new();
        sub.encode(&mut buf);
        self.storage.put(&id_key, &buf).await?;
        Ok(())
    }

    pub async fn all_aubscriptions(&self) -> Result<Vec<SubscriptionInfo>> {
        let id_key = self.key(Self::MAX_SUBSCRIPTION_ID_KEY);
        let Some(max_id)= self.storage.get_u64(&id_key).await? else {
            return Ok(vec![]);
        };

        let mut subs = Vec::with_capacity(max_id as usize + 1);
        for i in 0..=max_id {
            let key = self.key(&format!("{}-{}", Self::SUBSCRIPTION_KEY, i));
            let Some(sub) = self.storage.get(&key).await? else {
                continue;
            };
            let mut buf = Bytes::copy_from_slice(&sub);
            subs.push(SubscriptionInfo::decode(&mut buf)?);
        }
        Ok(subs)
    }

    pub async fn add_message(&self, message: &TopicMessage) -> Result<()> {
        let msg_id = message.message_id;
        let key = self.key(&format!(
            "{}-{}-{}",
            Self::MESSAGE_KEY,
            msg_id.topic_id,
            msg_id.cursor_id
        ));
        let mut buf = BytesMut::new();
        message.encode(&mut buf);
        self.storage.put(&key, &buf).await?;
        Ok(())
    }

    pub async fn get_message(&self, message_id: &MessageId) -> Result<Option<TopicMessage>> {
        let key = self.key(&format!(
            "{}-{}-{}",
            Self::MESSAGE_KEY,
            message_id.topic_id,
            message_id.cursor_id
        ));
        Ok(self
            .storage
            .get(&key)
            .await?
            .map(|b| {
                let mut buf = Bytes::copy_from_slice(&b);
                TopicMessage::decode(&mut buf)
            })
            .transpose()?)
    }

    pub async fn delete_range<R>(&self, topic_id: u64, cursor_range: R) -> Result<()>
    where
        R: RangeBounds<u64>,
    {
        for i in get_range(cursor_range)? {
            self.storage
                .del(&self.key(&format!("{}-{}-{}", Self::MESSAGE_KEY, topic_id, i)))
                .await?;
        }
        Ok(())
    }

    pub async fn get_sequence_id(&self, producer_name: &str) -> Result<Option<u64>> {
        let key = format!("{}-{}", Self::PRODUCER_SEQUENCE_ID_KEY, producer_name);
        let key = self.key(&key);
        Ok(self.storage.get_u64(&key).await?)
    }

    pub async fn set_sequence_id(&self, producer_name: &str, seq_id: u64) -> Result<()> {
        let key = format!("{}-{}", Self::PRODUCER_SEQUENCE_ID_KEY, producer_name);
        let key = self.key(&key);
        Ok(self.storage.set_u64(&key, seq_id).await?)
    }

    pub async fn get_new_cursor_id(&self) -> Result<u64> {
        let key = self.key(Self::LATEST_CURSOR_ID_KEY);
        Ok(self.storage.inc_u64(&key, 1).await?)
    }

    fn key(&self, s: &str) -> String {
        format!("TOPIC-{}-{}", self.topic_name, s)
    }
}
