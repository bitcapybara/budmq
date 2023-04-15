use std::{collections::HashMap, sync::Arc};

use async_trait::async_trait;
use tokio::sync::RwLock;

use super::{Result, Storage};

#[derive(Debug, Clone)]
pub struct MemoryStorage {
    inner: Arc<RwLock<HashMap<Vec<u8>, Vec<u8>>>>,
}

impl MemoryStorage {
    pub fn new() -> Self {
        Self {
            inner: Arc::new(RwLock::new(HashMap::new())),
        }
    }
}

#[async_trait]
impl Storage for MemoryStorage {
    async fn put(&self, k: &[u8], v: &[u8]) -> Result<()> {
        let mut inner = self.inner.write().await;
        inner.insert(k.to_vec(), v.to_vec());
        Ok(())
    }

    async fn get(&self, k: &[u8]) -> Result<Option<Vec<u8>>> {
        let inner = self.inner.read().await;
        Ok(inner.get(k).cloned())
    }

    async fn del(&self, k: &[u8]) -> Result<()> {
        let mut inner = self.inner.write().await;
        inner.remove(k);
        Ok(())
    }
}
