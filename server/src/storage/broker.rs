use std::net::SocketAddr;

use bud_common::storage::MetaStorage;

use super::{Error, Result};

#[derive(Clone)]
pub struct BrokerStorage<M> {
    storage: M,
}

impl<M: MetaStorage> BrokerStorage<M> {
    const MAX_TOPIC_ID_KEY: &str = "MAX_TOPIC_ID";
    const TOPIC_ID_KEY: &str = "TOPIC_ID";

    pub fn new(storage: M) -> Self {
        Self { storage }
    }

    pub fn inner(&self) -> M {
        self.storage.clone()
    }

    pub async fn get_or_create_topic_id(&self, topic_name: &str) -> Result<u64> {
        if let Some(id) = self.get_topic_id(topic_name).await? {
            return Ok(id);
        }
        let new_id = self
            .storage
            .inc_u64(Self::MAX_TOPIC_ID_KEY, 1)
            .await
            .map_err(|e| Error::Storage(e.to_string()))?;
        self.set_topic_id(topic_name, new_id).await?;
        Ok(new_id)
    }

    async fn get_topic_id(&self, topic_name: &str) -> Result<Option<u64>> {
        let key = format!("{}-{}", Self::TOPIC_ID_KEY, topic_name);
        self.storage
            .get_u64(&key)
            .await
            .map_err(|e| Error::Storage(e.to_string()))
    }

    async fn set_topic_id(&self, topic_name: &str, topic_id: u64) -> Result<()> {
        let key = format!("{}-{}", Self::TOPIC_ID_KEY, topic_name);
        self.storage
            .put_u64(&key, topic_id)
            .await
            .map_err(|e| Error::Storage(e.to_string()))
    }

    pub async fn get_topic_broker_addr(&self, topic_name: &str) -> Result<Option<SocketAddr>> {
        self.storage
            .get_topic_owner(topic_name)
            .await
            .map_err(|e| Error::Storage(e.to_string()))
    }
}
