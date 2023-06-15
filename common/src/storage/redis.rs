use async_trait::async_trait;
use redis::Client;

use super::MetaStorage;

pub type Result<T> = std::result::Result<T, Error>;

#[derive(Debug, thiserror::Error)]
pub enum Error {
    /// error from redis
    #[error("Redis error: {0}")]
    Redis(#[from] redis::RedisError),
}

#[derive(Clone)]
pub struct Redis {
    client: Client,
}

impl Redis {
    fn new(client: Client) -> Self {
        Self { client }
    }
}

#[async_trait]
impl MetaStorage for Redis {
    type Error = Error;

    async fn put(&self, k: &str, v: &[u8]) -> Result<()> {
        let mut conn = self.client.get_async_connection().await?;
        redis::Cmd::set(k, v.to_vec())
            .query_async(&mut conn)
            .await?;
        Ok(())
    }

    async fn get(&self, k: &str) -> Result<Option<Vec<u8>>> {
        let mut conn = self.client.get_async_connection().await?;
        Ok(redis::Cmd::get(k)
            .query_async::<_, Option<Vec<u8>>>(&mut conn)
            .await?)
    }

    async fn del(&self, k: &str) -> Result<()> {
        let mut conn = self.client.get_async_connection().await?;
        redis::Cmd::del(k).query_async(&mut conn).await?;
        Ok(())
    }

    async fn get_u64(&self, k: &str) -> Result<Option<u64>> {
        let mut conn = self.client.get_async_connection().await?;
        Ok(redis::Cmd::get(k)
            .query_async::<_, Option<u64>>(&mut conn)
            .await?)
    }

    async fn put_u64(&self, k: &str, v: u64) -> Result<()> {
        let mut conn = self.client.get_async_connection().await?;
        Ok(redis::Cmd::set(k, v).query_async(&mut conn).await?)
    }

    async fn inc_u64(&self, k: &str, v: u64) -> Result<u64> {
        let mut conn = self.client.get_async_connection().await?;
        Ok(redis::Cmd::incr(k, v)
            .query_async::<_, u64>(&mut conn)
            .await?)
    }
}
