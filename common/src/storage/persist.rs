use async_trait::async_trait;
use bonsaidb::{
    core::keyvalue::{AsyncKeyValue, Numeric, Value},
    local::{
        config::{Builder, StorageConfiguration},
        AsyncDatabase,
    },
};

use super::{MetaStorage, Result};

#[derive(Clone)]
pub struct PersistStorage {
    inner: AsyncDatabase,
}

#[async_trait]
impl MetaStorage for PersistStorage {
    async fn create(id: &str) -> Result<Self> {
        let config = StorageConfiguration::new(format!("data/{id}.db"));
        let inner = AsyncDatabase::open::<()>(config).await?;
        Ok(Self { inner })
    }

    async fn put(&self, k: &str, v: &[u8]) -> Result<()> {
        self.inner.set_binary_key(k, v).await?;
        Ok(())
    }

    async fn get(&self, k: &str) -> Result<Option<Vec<u8>>> {
        let Some(v) = self.inner.get_key(k).await? else {
            return Ok(None);
        };

        match v {
            Value::Bytes(b) => Ok(Some(b.to_vec())),
            Value::Numeric(Numeric::UnsignedInteger(n)) => Ok(Some(n.to_be_bytes().to_vec())),
            _ => Ok(None),
        }
    }

    async fn del(&self, k: &str) -> Result<()> {
        self.inner.delete_key(k).await?;
        Ok(())
    }

    async fn get_u64(&self, k: &str) -> Result<Option<u64>> {
        let Some(v) = self.inner.get_key(k).await? else {
            return Ok(None)
        };
        match v {
            Value::Numeric(Numeric::UnsignedInteger(n)) => Ok(Some(n)),
            _ => Ok(None),
        }
    }

    async fn set_u64(&self, k: &str, v: u64) -> Result<()> {
        self.inner.set_numeric_key(k, v).await?;
        Ok(())
    }

    async fn inc_u64(&self, k: &str, v: u64) -> Result<u64> {
        let v = self.inner.increment_key_by(k, v).await?;
        Ok(v)
    }
}
