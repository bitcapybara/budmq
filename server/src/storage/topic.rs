use std::{
    ops::{Bound, RangeBounds, RangeInclusive},
    sync::atomic::AtomicU64,
};

use bud_common::{
    codec::Codec,
    storage::{MessageStorage, MetaStorage},
    types::{MessageId, SubscriptionInfo, TopicMessage},
};
use bytes::{Bytes, BytesMut};
use log::warn;

use super::{Error, Result};

pub struct TopicStorage<M, S> {
    topic_name: String,
    meta_storage: M,
    message_storage: S,
    counter: AtomicU64,
}

impl<M: MetaStorage, S: MessageStorage> TopicStorage<M, S> {
    const TOPIC_KEY: &[u8] = "TOPIC".as_bytes();
    const PRODUCER_SEQUENCE_ID_KEY: &str = "PRODUCER_SEQUENCE_ID";
    const MAX_SUBSCRIPTION_ID_KEY: &str = "MAX_SUBSCRIPTION_ID";
    const SUBSCRIPTION_KEY: &str = "SUBSCRIPTION";
    const LATEST_CURSOR_ID_KEY: &str = "LATEST_CURSOR_ID";
    const MESSAGE_KEY: &str = "MESSAGE";

    pub fn new(topic_name: &str, meta_storage: M, message_storage: S) -> Result<Self> {
        Ok(Self {
            topic_name: topic_name.to_string(),
            meta_storage,
            counter: AtomicU64::new(1),
            message_storage,
        })
    }

    pub fn inner(&self) -> M {
        self.meta_storage.clone()
    }

    pub async fn add_subscription(&self, sub: &SubscriptionInfo) -> Result<()> {
        let id_key = self.key(Self::MAX_SUBSCRIPTION_ID_KEY);
        let id = self
            .meta_storage
            .inc_u64(&id_key, 1)
            .await
            .map_err(|e| Error::Storage(e.to_string()))?;
        let id_key = self.key(&format!("{}-{}", Self::SUBSCRIPTION_KEY, id));
        let mut buf = BytesMut::new();
        sub.encode(&mut buf);
        self.meta_storage
            .put(&id_key, &buf)
            .await
            .map_err(|e| Error::Storage(e.to_string()))?;
        Ok(())
    }

    pub async fn del_subscription(&self, _sub_name: &str) -> Result<()> {
        todo!()
    }

    pub async fn all_aubscriptions(&self) -> Result<Vec<SubscriptionInfo>> {
        let id_key = self.key(Self::MAX_SUBSCRIPTION_ID_KEY);
        let max_id = self
            .meta_storage
            .get_u64(&id_key)
            .await
            .map_err(|e| Error::Storage(e.to_string()))?;
        let Some(max_id)= max_id else {
            return Ok(vec![]);
        };

        let mut subs = Vec::with_capacity(max_id as usize + 1);
        for i in 0..=max_id {
            let key = self.key(&format!("{}-{}", Self::SUBSCRIPTION_KEY, i));
            let sub = self
                .meta_storage
                .get(&key)
                .await
                .map_err(|e| Error::Storage(e.to_string()))?;
            let Some(sub) = sub else {
                continue;
            };
            let mut buf = Bytes::copy_from_slice(&sub);
            subs.push(SubscriptionInfo::decode(&mut buf)?);
        }
        Ok(subs)
    }

    pub async fn add_message(&self, message: &TopicMessage) -> Result<()> {
        self.message_storage
            .put_message(message)
            .await
            .map_err(|e| Error::Storage(e.to_string()))?;
        Ok(())
    }

    pub async fn get_message(&self, message_id: &MessageId) -> Result<Option<TopicMessage>> {
        self.message_storage
            .get_message(message_id)
            .await
            .map_err(|e| Error::Storage(e.to_string()))
    }

    pub async fn delete_range<R>(&self, topic_id: u64, cursor_range: R) -> Result<()>
    where
        R: RangeBounds<u64>,
    {
        let Some(range) = Self::get_range(cursor_range) else {
            warn!("delete message with invalid range");
            return Ok(());
        };
        for i in range {
            self.message_storage
                .del_message(&MessageId {
                    topic_id,
                    cursor_id: i,
                })
                .await
                .map_err(|e| Error::Storage(e.to_string()))?;
        }
        Ok(())
    }

    fn get_range<R>(range: R) -> Option<RangeInclusive<u64>>
    where
        R: RangeBounds<u64>,
    {
        let start = match range.start_bound() {
            Bound::Included(&i) => i,
            Bound::Excluded(&u64::MAX) => return None,
            Bound::Excluded(&i) => i + 1,
            Bound::Unbounded => 0,
        };
        let end = match range.end_bound() {
            Bound::Included(&i) => i,
            Bound::Excluded(&0) => return None,
            Bound::Excluded(&i) => i - 1,
            Bound::Unbounded => u64::MAX,
        };
        if end < start {
            return None;
        }

        Some(start..=end)
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
            .set_u64(&key, seq_id)
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
