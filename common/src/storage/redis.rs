use std::time::Duration;

use async_trait::async_trait;
use log::error;
use redis::{aio, AsyncCommands, Client};
use tokio::{select, time};
use tokio_util::sync::CancellationToken;

use crate::{
    error::WrapError,
    types::{BrokerAddress, SubscriptionInfo},
    wrap_error_impl,
};

use super::MetaStorage;

const KEY_PREFIX: &str = "BUDMQ";

pub type Result<T> = std::result::Result<T, Error>;

#[derive(Debug, thiserror::Error)]
pub enum Error {
    /// error from redis
    #[error("Redis error: {0}")]
    Redis(String),
    #[error("Parse broker addr error: {0}")]
    ParseAddr(String),
    #[error("Json codec error: {0}")]
    Json(String),
}

wrap_error_impl!(redis::RedisError, Error::Redis);
wrap_error_impl!(std::net::AddrParseError, Error::ParseAddr);
wrap_error_impl!(serde_json::Error, Error::Json);

pub struct Redis {
    client: Client,
    token: CancellationToken,
}

impl Clone for Redis {
    fn clone(&self) -> Self {
        Self {
            client: self.client.clone(),
            token: CancellationToken::new(),
        }
    }
}

impl Drop for Redis {
    fn drop(&mut self) {
        self.token.cancel();
    }
}

impl Redis {
    pub fn new(client: Client) -> Self {
        Self {
            client,
            token: CancellationToken::new(),
        }
    }

    async fn get_async_connection(&self) -> Result<aio::Connection> {
        self.client
            .get_async_connection()
            .await
            .wrap("get async connection error")
    }
}

#[async_trait]
impl MetaStorage for Redis {
    type Error = Error;

    async fn register_topic(&self, topic_name: &str, broker_addr: &BrokerAddress) -> Result<()> {
        let mut conn = self.get_async_connection().await?;
        // SET EX NX
        let broker_addr_str =
            serde_json::to_string(broker_addr).wrap("marshal broker addr to json error")?;
        let res: Option<String> = redis::Cmd::new()
            .arg("SET")
            .arg(format!("{KEY_PREFIX}:TOPIC_BROKER:{topic_name}")) // key
            .arg(&broker_addr_str) // value
            .arg("NX")
            .arg("EX")
            .arg(10)
            .query_async(&mut conn)
            .await
            .with_wrap(|| format!("broker {broker_addr} register topic {topic_name} error"))?;
        if res.is_none() {
            return Ok(());
        }
        // spawn SET EX
        let client = self.client.clone();
        let token = self.token.child_token();
        let topic_name = topic_name.to_string();
        tokio::spawn(async move {
            let mut interval = time::interval(Duration::from_secs(5));
            interval.set_missed_tick_behavior(time::MissedTickBehavior::Skip);
            interval.tick().await;
            loop {
                select! {
                    _ = interval.tick() => {
                        let mut conn = match client.get_async_connection().await {
                            Ok(conn) => conn,
                            Err(e) => {
                                error!("Redis get_async_connection error: {e}");
                                return;
                            }
                        };
                        if let Err(e) = redis::Cmd::new()
                            .arg("SET")
                            .arg(format!("{KEY_PREFIX}:TOPIC_BROKER:{topic_name}")) // key
                            .arg(&broker_addr_str) // value
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
    async fn unregister_topic(&self, topic_name: &str, broker_addr: &BrokerAddress) -> Result<()> {
        let Some(addr) = self.get_topic_owner(topic_name).await? else {
            return Ok(());
        };
        if addr.socket_addr != broker_addr.socket_addr {
            return Ok(());
        }
        let mut conn = self.get_async_connection().await?;
        redis::Cmd::del(format!("{KEY_PREFIX}:TOPIC_BROKER:{topic_name}"))
            .query_async(&mut conn)
            .await
            .with_wrap(|| format!("broker unregister topic {topic_name} error"))?;
        Ok(())
    }

    async fn get_topic_owner(&self, topic_name: &str) -> Result<Option<BrokerAddress>> {
        let mut conn = self.get_async_connection().await?;
        // GET broker id
        let addr: String = {
            match redis::Cmd::get(format!("{KEY_PREFIX}:TOPIC_BROKER:{topic_name}"))
                .query_async(&mut conn)
                .await
                .with_wrap(|| format!("get topic {topic_name} owner error"))?
            {
                Some(addr) => addr,
                None => return Ok(None),
            }
        };
        Ok(Some(
            serde_json::from_str(&addr).wrap("unmarshal broker addr error")?,
        ))
    }

    async fn add_subscription(&self, info: &SubscriptionInfo) -> Result<()> {
        let mut conn = self.get_async_connection().await?;
        // HSET
        let content = serde_json::to_string(info).wrap("marshal subscription info error")?;
        redis::Cmd::hset(
            format!("{KEY_PREFIX}:SUBSCRIPTIONS:{}", &info.topic),
            &info.name,
            content,
        )
        .query_async(&mut conn)
        .await
        .with_wrap(|| format!("hset topic {} subscription {} error", info.topic, info.name))?;
        Ok(())
    }

    async fn all_subscription(&self, topic_name: &str) -> Result<Vec<SubscriptionInfo>> {
        let mut conn = self.get_async_connection().await?;
        // HSCAN
        let mut subs = conn
            .hscan::<_, String>(format!("{KEY_PREFIX}:SUBSCRIPTIONS:{topic_name}"))
            .await
            .with_wrap(|| format!("hscan all subscriptions in topic {topic_name} error"))?;
        let mut res = vec![];
        let mut is_val = false;
        while let Some(content) = subs.next_item().await {
            if is_val {
                let sub: SubscriptionInfo =
                    serde_json::from_str(&content).wrap("unmarshal subscription error")?;
                res.push(sub);
            } else {
                is_val = true
            }
        }
        Ok(res)
    }

    async fn del_subscription(&self, topic_name: &str, name: &str) -> Result<()> {
        let mut conn = self.get_async_connection().await?;
        // HDEL
        redis::Cmd::hdel(format!("{KEY_PREFIX}:SUBSCRIPTIONS:{topic_name}"), name)
            .query_async(&mut conn)
            .await
            .with_wrap(|| format!("hdel topic {topic_name} subscription {name} error"))?;
        Ok(())
    }

    async fn get_u64(&self, k: &str) -> Result<Option<u64>> {
        let mut conn = self.get_async_connection().await?;
        Ok(redis::Cmd::get(format!("{KEY_PREFIX}:{k}"))
            .query_async(&mut conn)
            .await
            .with_wrap(|| format!("get {k} value error"))?)
    }

    async fn put_u64(&self, k: &str, v: u64) -> Result<()> {
        let mut conn = self.get_async_connection().await?;
        Ok(redis::Cmd::set(format!("{KEY_PREFIX}:{k}"), v)
            .query_async(&mut conn)
            .await
            .with_wrap(|| format!("put {k} value error"))?)
    }

    async fn inc_u64(&self, k: &str, v: u64) -> Result<u64> {
        let mut conn = self.get_async_connection().await?;
        Ok(redis::Cmd::incr(format!("{KEY_PREFIX}:{k}"), v)
            .query_async(&mut conn)
            .await
            .with_wrap(|| format!("inc {k} {v} error"))?)
    }
}
