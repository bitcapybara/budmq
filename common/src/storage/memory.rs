//! Memory Storage
//! Only used for standalone, non-durable server

use std::{array, collections::HashMap, sync::Arc};

use async_trait::async_trait;
use tokio::sync::RwLock;

use crate::types::{BrokerAddress, MessageId, SubscriptionInfo, TopicMessage};

use super::{MessageStorage, MetaStorage};

pub type Result<T> = std::result::Result<T, Error>;

#[derive(Debug, thiserror::Error)]
pub enum Error {
    /// error when decode slice to u64
    #[error("Decode slice error: {0}")]
    DecodeSlice(#[from] array::TryFromSliceError),
}

#[derive(Debug, Clone)]
pub struct MemoryDB {
    /// str_key -> bytes_value
    metas: Arc<RwLock<HashMap<String, Vec<u8>>>>,
    /// broker
    broker_addr: Arc<RwLock<Option<BrokerAddress>>>,
    /// topic_name -> sub_name -> subscription
    subs: Arc<RwLock<HashMap<String, HashMap<String, SubscriptionInfo>>>>,
    /// cursor
    cursor: Arc<RwLock<Option<Vec<u8>>>>,
    /// topic_id -> cursor_id -> message
    messages: Arc<RwLock<HashMap<u64, HashMap<u64, TopicMessage>>>>,
}

impl MemoryDB {
    pub fn new() -> Self {
        Self {
            metas: Arc::new(RwLock::new(HashMap::new())),
            messages: Arc::new(RwLock::new(HashMap::new())),
            broker_addr: Arc::new(RwLock::new(None)),
            subs: Arc::new(RwLock::new(HashMap::new())),
            cursor: Arc::new(RwLock::new(None)),
        }
    }

    async fn put(&self, k: &str, v: &[u8]) -> Result<()> {
        let mut inner = self.metas.write().await;
        inner.insert(k.to_string(), v.to_vec());
        Ok(())
    }

    async fn get(&self, k: &str) -> Result<Option<Vec<u8>>> {
        let inner = self.metas.read().await;
        Ok(inner.get(k).cloned())
    }

    async fn del(&self, k: &str) -> Result<()> {
        let mut inner = self.metas.write().await;
        inner.remove(k);
        Ok(())
    }
}

impl Default for MemoryDB {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait]
impl MetaStorage for MemoryDB {
    type Error = Error;

    async fn register_topic(&self, _topic_name: &str, broker_addr: &BrokerAddress) -> Result<()> {
        let mut broker = self.broker_addr.write().await;
        *broker = Some(broker_addr.clone());
        Ok(())
    }
    async fn unregister_topic(&self, _topic_name: &str, broker_addr: &BrokerAddress) -> Result<()> {
        let mut broker = self.broker_addr.write().await;
        let Some(addr) = broker.as_ref() else {
            return Ok(());
        };
        if addr.socket_addr == broker_addr.socket_addr {
            *broker = None;
        }
        Ok(())
    }

    async fn get_topic_owner(&self, _topic_name: &str) -> Result<Option<BrokerAddress>> {
        let broker = self.broker_addr.read().await;
        Ok((*broker).clone())
    }

    async fn add_subscription(&self, info: &SubscriptionInfo) -> Result<()> {
        let mut topic = self.subs.write().await;
        match topic.get_mut(&info.topic) {
            Some(subs) => {
                subs.insert(info.name.clone(), info.clone());
            }
            None => {
                let mut subs = HashMap::new();
                subs.insert(info.name.clone(), info.clone());
                topic.insert(info.topic.clone(), subs);
            }
        }
        Ok(())
    }

    async fn all_subscription(&self, topic_name: &str) -> Result<Vec<SubscriptionInfo>> {
        let topic = self.subs.read().await;
        Ok(topic
            .get(topic_name)
            .map(|subs| subs.values().cloned().collect())
            .unwrap_or_default())
    }

    async fn del_subscription(&self, topic_name: &str, name: &str) -> Result<()> {
        let mut topic = self.subs.write().await;
        if let Some(subs) = topic.get_mut(topic_name) {
            subs.remove(name);
        }
        Ok(())
    }

    async fn get_u64(&self, k: &str) -> Result<Option<u64>> {
        Ok(self
            .get(k)
            .await?
            .map(|b| b.as_slice().try_into())
            .transpose()?
            .map(u64::from_be_bytes))
    }

    async fn put_u64(&self, k: &str, v: u64) -> Result<()> {
        Ok(self.put(k, v.to_be_bytes().as_slice()).await?)
    }

    async fn inc_u64(&self, k: &str, v: u64) -> Result<u64> {
        let mut inner = self.metas.write().await;
        match inner.remove(k) {
            Some(value) => {
                let prev = u64::from_be_bytes(value.as_slice().try_into()?);
                inner.insert(k.to_string(), (prev + v).to_be_bytes().to_vec());
                Ok(prev)
            }
            None => {
                let prev = 0;
                inner.insert(k.to_string(), v.to_be_bytes().to_vec());
                Ok(prev)
            }
        }
    }
}

#[async_trait]
impl MessageStorage for MemoryDB {
    type Error = Error;

    async fn put_message(&self, msg: &TopicMessage) -> Result<()> {
        let MessageId {
            topic_id,
            cursor_id,
        } = msg.message_id;
        let mut messages = self.messages.write().await;
        match messages.get_mut(&topic_id) {
            Some(msgs) => {
                msgs.insert(cursor_id, msg.clone());
            }
            None => {
                let mut map = HashMap::new();
                map.insert(cursor_id, msg.clone());
                messages.insert(topic_id, map);
            }
        }
        Ok(())
    }

    async fn get_message(&self, id: &MessageId) -> Result<Option<TopicMessage>> {
        let messages = self.messages.read().await;
        let msg = messages
            .get(&id.topic_id)
            .and_then(|msgs| msgs.get(&id.cursor_id))
            .cloned();
        Ok(msg)
    }
    async fn del_message(&self, id: &MessageId) -> Result<()> {
        let mut messages = self.messages.write().await;
        if let Some(msgs) = messages.get_mut(&id.topic_id) {
            msgs.remove(&id.cursor_id);
        }
        Ok(())
    }

    async fn save_cursor(&self, _topic_name: &str, _sub_name: &str, _bytes: &[u8]) -> Result<()> {
        Ok(())
    }

    async fn load_cursor(&self, _topic_name: &str, _sub_name: &str) -> Result<Option<Vec<u8>>> {
        Ok(None)
    }
}
