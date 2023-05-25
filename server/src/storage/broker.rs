use bud_common::storage::Storage;

use super::Result;

#[derive(Clone)]
pub struct BrokerStorage<S> {
    storage: S,
}

impl<S: Storage> BrokerStorage<S> {
    const MAX_TOPIC_ID_KEY: &[u8] = "MAX_TOPIC_ID".as_bytes();
    pub fn new(storage: S) -> Self {
        Self { storage }
    }

    pub fn inner(&self) -> S {
        self.storage.clone()
    }

    pub async fn get_new_topic_id(&self) -> Result<u64> {
        Ok(self.storage.fetch_add(Self::MAX_TOPIC_ID_KEY, 1).await?)
    }
}
