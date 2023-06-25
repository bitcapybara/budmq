use std::{
    net::{AddrParseError, SocketAddr},
    time::Duration,
};

use async_trait::async_trait;
use log::error;
use redis::{AsyncCommands, Client};
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
    #[error("Parse broker addr error: {0}")]
    ParseAddr(#[from] AddrParseError),
    #[error("Json codec error: {0}")]
    Json(#[from] serde_json::Error),
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
}

#[async_trait]
impl MetaStorage for Redis {
    type Error = Error;

    async fn register_broker(&self, id: &str, addr: &SocketAddr) -> Result<()> {
        let mut conn = self.client.get_async_connection().await?;
        // SET NX EX
        let res: Option<String> = redis::Cmd::new()
            .arg("SET")
            .arg(format!("BUDMQ_BROKER_ONLINE:{id}")) // key
            .arg(addr.to_string()) // value
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
        let mut conn = self.client.get_async_connection().await?;
        let keys = {
            let mut brokers = conn
                .scan_match::<_, String>("BUDMQ_BROKER_ONLINE:*")
                .await?;
            let mut keys = vec![];
            while let Some(broker) = brokers.next_item().await {
                keys.push(broker);
            }
            keys
        };
        let mut res = vec![];
        for key in keys {
            let addr: String = redis::Cmd::get(key).query_async(&mut conn).await?;
            res.push(addr.parse()?)
        }
        Ok(res)
    }

    async fn register_topic(&self, topic_id: u64, broker_id: &str) -> Result<()> {
        let mut conn = self.client.get_async_connection().await?;
        // SET EX NX
        let res: Option<String> = redis::Cmd::new()
            .arg("SET")
            .arg(format!("BUDMQ_TOPIC_BROKER:{topic_id}")) // key
            .arg(broker_id) // value
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
        let token = self.token.child_token();
        let topic_id = topic_id;
        let broker_id = broker_id.to_owned();
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
                            .arg(format!("BUDMQ_TOPIC_BROKER:{topic_id}")) // key
                            .arg(&broker_id) // value
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

    async fn get_topic_owner(&self, topic_id: u64) -> Result<Option<SocketAddr>> {
        let mut conn = self.client.get_async_connection().await?;
        // GET broker id
        let broker_id: String = {
            match redis::Cmd::get(format!("BUDMQ_TOPIC_BROKER:{topic_id}"))
                .query_async(&mut conn)
                .await?
            {
                Some(broker_id) => broker_id,
                None => return Ok(None),
            }
        };
        // GET broker addr
        let addr: String = {
            match redis::Cmd::get(format!("BUDMQ_BROKER_ONLINE:{broker_id}"))
                .query_async(&mut conn)
                .await?
            {
                Some(addr) => addr,
                None => return Ok(None),
            }
        };
        Ok(Some(addr.parse()?))
    }

    async fn add_subscription(&self, info: &SubscriptionInfo) -> Result<()> {
        let mut conn = self.client.get_async_connection().await?;
        // HSET
        let content = serde_json::to_string(info)?;
        redis::Cmd::hset(
            format!("BUDMQ_SUBSCRIPTIONS:{}", &info.topic),
            &info.name,
            content,
        )
        .query_async(&mut conn)
        .await?;
        Ok(())
    }

    async fn all_subscription(&self, topic_name: &str) -> Result<Vec<SubscriptionInfo>> {
        let mut conn = self.client.get_async_connection().await?;
        // HSCAN
        let mut subs = conn
            .hscan::<_, String>(format!("BUDMQ_SUBSCRIPTIONS:{topic_name}"))
            .await?;
        let mut res = vec![];
        let mut is_val = false;
        while let Some(content) = subs.next_item().await {
            if is_val {
                let sub: SubscriptionInfo = serde_json::from_str(&content)?;
                res.push(sub);
            } else {
                is_val = true
            }
        }
        Ok(res)
    }

    async fn del_subscription(&self, topic_name: &str, name: &str) -> Result<()> {
        let mut conn = self.client.get_async_connection().await?;
        // HDEL
        redis::Cmd::hdel(format!("BUDMQ_SUBSCRIPTIONS:{topic_name}"), name)
            .query_async(&mut conn)
            .await?;
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
