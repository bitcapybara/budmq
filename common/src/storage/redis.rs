use std::{net::SocketAddr, time::Duration};

use async_trait::async_trait;
use log::error;
use redis::Client;
use tokio::{select, time};
use tokio_util::sync::CancellationToken;

use crate::types::SubscriptionInfo;

use super::MetaStorage;

pub type Result<T> = std::result::Result<T, Error>;

#[derive(Debug, thiserror::Error)]
pub enum Error {
    /// error from redis
    #[error("Redis error: {0}")]
    Redis(#[from] redis::RedisError),
    /// key already set
    #[error("Redis key already exists when SET NX")]
    KeyExists,
}

#[derive(Clone)]
pub struct Redis {
    client: Client,
    token: CancellationToken,
}

impl Drop for Redis {
    fn drop(&mut self) {
        self.token.cancel();
    }
}

impl Redis {
    fn new(client: Client) -> Self {
        Self {
            client,
            token: CancellationToken::new(),
        }
    }

    async fn register_broker(&self, id: &str, addr: &SocketAddr) -> Result<()> {
        let mut conn = self.client.get_async_connection().await?;
        // SET NX EX
        let res: Option<String> = redis::Cmd::new()
            .arg("SET")
            .arg(format!("BUDMQ_BROKER_ONLINE:{id}"))
            .arg(addr.to_string())
            .arg("NX")
            .arg("EX")
            .arg(10)
            .query_async(&mut conn)
            .await?;
        if res.is_none() {
            return Err(Error::KeyExists);
        }
        // spawn SET EX
        let client = self.client.clone();
        let id = id.to_string();
        let addr = *addr;
        let token = self.token.child_token();
        tokio::spawn(async move {
            loop {
                select! {
                    _ = time::sleep(Duration::from_secs(5)) => {
                        let mut conn = match client.get_async_connection().await {
                            Ok(conn) => conn,
                            Err(e) => {
                                error!("Redis get_async_connection error: {e}");
                                return;
                            }
                        };
                        if let Err(e) = redis::Cmd::new()
                            .arg("SET")
                            .arg(format!("BUDMQ_BROKER_ONLINE:{id}"))
                            .arg(addr.to_string())
                            .arg("EX")
                            .arg(10)
                            .query_async::<_, String>(&mut conn)
                            .await
                        {
                            error!("Redis SET EX error: {e}");
                        };
                    }
                    _ = token.cancelled() => {
                        return
                    }
                }
            }
        });
        Ok(())
    }

    async fn all_brokers(&self) -> Result<Vec<SocketAddr>> {
        // SCAN
        Ok(vec![])
    }

    async fn register_topic(&self, _topic_id: u64, _broker_id: &str) -> Result<()> {
        // SET EX NX
        // spawn SET EX
        Ok(())
    }

    async fn get_topic_owner(&self, _topic_id: u64) -> Result<Option<SocketAddr>> {
        // GET
        Ok(None)
    }

    async fn add_subscription(&self, _info: &SubscriptionInfo) -> Result<()> {
        // HSET
        Ok(())
    }

    async fn all_subscription(&self) -> Result<Vec<SubscriptionInfo>> {
        // HSCAN
        Ok(vec![])
    }

    async fn del_subscription(&self, _topic_name: &str, _name: &str) -> Result<()> {
        // HDEL
        Ok(())
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
        Ok(redis::Cmd::get(k).query_async(&mut conn).await?)
    }

    async fn del(&self, k: &str) -> Result<()> {
        let mut conn = self.client.get_async_connection().await?;
        redis::Cmd::del(k).query_async(&mut conn).await?;
        Ok(())
    }

    async fn get_u64(&self, k: &str) -> Result<Option<u64>> {
        let mut conn = self.client.get_async_connection().await?;
        Ok(redis::Cmd::get(k).query_async(&mut conn).await?)
    }

    async fn put_u64(&self, k: &str, v: u64) -> Result<()> {
        let mut conn = self.client.get_async_connection().await?;
        Ok(redis::Cmd::set(k, v).query_async(&mut conn).await?)
    }

    async fn inc_u64(&self, k: &str, v: u64) -> Result<u64> {
        let mut conn = self.client.get_async_connection().await?;
        Ok(redis::Cmd::incr(k, v).query_async(&mut conn).await?)
    }
}
