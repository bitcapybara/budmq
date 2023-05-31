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

impl Default for MemoryStorage {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait]
impl Storage for MemoryStorage {
    async fn create(_id: &str) -> Result<Self> {
        Ok(Self::default())
    }

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

    async fn atomic_add(&self, k: &[u8], v: u64) -> Result<u64> {
        let mut inner = self.inner.write().await;
        match inner.remove(k) {
            Some(value) => {
                let prev = u64::from_be_bytes(value.as_slice().try_into()?);
                inner.insert(k.to_vec(), (prev + v).to_be_bytes().to_vec());
                Ok(prev)
            }
            None => {
                let prev = 0;
                inner.insert(k.to_vec(), v.to_be_bytes().to_vec());
                Ok(prev)
            }
        }
    }
}
