use bud_common::storage::Storage;

use super::Result;

#[derive(Clone)]
pub struct BrokerStorage<S> {
    storage: S,
}

impl<S: Storage> BrokerStorage<S> {
    const MAX_TOPIC_ID_KEY: &[u8] = "MAX_TOPIC_ID".as_bytes();
    const TOPIC_ID_KEY: &str = "TOPIC_ID";
    pub fn new(storage: S) -> Self {
        Self { storage }
    }

    pub async fn get_or_create_topic_id(&self, topic_name: &str) -> Result<u64> {
        if let Some(id) = self.get_topic_id(topic_name).await? {
            return Ok(id);
        }
        let new_id = self.storage.atomic_add(Self::MAX_TOPIC_ID_KEY, 1).await?;
        self.set_topic_id(topic_name, new_id).await?;
        Ok(new_id)
    }

    async fn get_topic_id(&self, topic_name: &str) -> Result<Option<u64>> {
        let key = format!("{}-{}", Self::TOPIC_ID_KEY, topic_name);
        self.storage
            .get_u64(key.as_bytes())
            .await
            .map_err(|e| e.into())
    }

    async fn set_topic_id(&self, topic_name: &str, topic_id: u64) -> Result<()> {
        let key = format!("{}-{}", Self::TOPIC_ID_KEY, topic_name);
        Ok(self
            .storage
            .put(key.as_bytes(), topic_id.to_be_bytes().as_slice())
            .await?)
    }
}
