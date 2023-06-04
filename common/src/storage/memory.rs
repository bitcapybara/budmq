use std::{collections::HashMap, sync::Arc};

use async_trait::async_trait;
use tokio::sync::RwLock;

use crate::types::{MessageId, TopicMessage};

use super::{MessageStorage, MetaStorage, Result};

#[derive(Debug, Clone)]
pub struct MemoryStorage {
    /// str_key -> bytes_value
    metas: Arc<RwLock<HashMap<String, Vec<u8>>>>,

    /// topic_id -> cursor_id -> message
    messages: Arc<RwLock<HashMap<u64, HashMap<u64, TopicMessage>>>>,
}

impl MemoryStorage {
    pub fn new() -> Self {
        Self {
            metas: Arc::new(RwLock::new(HashMap::new())),
            messages: Arc::new(RwLock::new(HashMap::new())),
        }
    }
}

impl Default for MemoryStorage {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait]
impl MetaStorage for MemoryStorage {
    async fn create(_id: &str) -> Result<Self> {
        Ok(Self::default())
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
impl MessageStorage for MemoryStorage {
    async fn put_message(&self, msg: TopicMessage) -> Result<()> {
        let MessageId {
            topic_id,
            cursor_id,
        } = msg.message_id;
        let mut messages = self.messages.write().await;
        match messages.get_mut(&topic_id) {
            Some(msgs) => {
                msgs.insert(cursor_id, msg);
            }
            None => {
                let mut map = HashMap::new();
                map.insert(cursor_id, msg);
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
}
