use std::net::SocketAddr;

use async_trait::async_trait;
use bonsaidb::{
    core::keyvalue::{AsyncKeyValue, Numeric, Value},
    local::{
        config::{Builder, StorageConfiguration},
        AsyncDatabase,
    },
};
use bytes::{Bytes, BytesMut};

use crate::{
    codec::{self, Codec},
    types::{MessageId, SubscriptionInfo, TopicMessage},
};

use super::{MessageStorage, MetaStorage};

const CURRENT_BROKER_KEY: &str = "CURRENT_BROKER";
const BROKER_TOPIC_KEY: &str = "BROKER_TOPIC";
const SUBSCRIPTION_NAMES_KEY: &str = "SUBSCRIPTION_NAMES";
const SUBSCRIPTION_KEY: &str = "SUBSCRIPTION";
const CURSOR_KEY: &str = "CURSOR";

pub type Result<T> = std::result::Result<T, Error>;

#[derive(Debug, thiserror::Error)]
pub enum Error {
    /// error from bonsaidb
    #[error("Persistent database error: {0}")]
    BonsaiDB(String),
    /// data structure codec error
    #[error("Protocol codec error: {0}")]
    Codec(#[from] codec::Error),
}

impl From<bonsaidb::local::Error> for Error {
    fn from(e: bonsaidb::local::Error) -> Self {
        Self::BonsaiDB(e.to_string())
    }
}

impl From<bonsaidb::core::Error> for Error {
    fn from(e: bonsaidb::core::Error) -> Self {
        Self::BonsaiDB(e.to_string())
    }
}

#[derive(Clone)]
pub struct BonsaiDB {
    metas: AsyncDatabase,
    messages: AsyncDatabase,
}

impl BonsaiDB {
    async fn new() -> Result<Self> {
        Ok(Self {
            metas: AsyncDatabase::open::<()>(StorageConfiguration::new("data/meta.db")).await?,
            messages: AsyncDatabase::open::<()>(StorageConfiguration::new("data/message.db"))
                .await?,
        })
    }

    async fn put(&self, k: &str, v: &[u8]) -> Result<()> {
        self.metas.set_binary_key(k, v).await?;
        Ok(())
    }

    async fn get(&self, k: &str) -> Result<Option<Vec<u8>>> {
        let Some(v) = self.metas.get_key(k).await? else {
            return Ok(None);
        };

        match v {
            Value::Bytes(b) => Ok(Some(b.to_vec())),
            Value::Numeric(Numeric::UnsignedInteger(n)) => Ok(Some(n.to_be_bytes().to_vec())),
            _ => Ok(None),
        }
    }

    async fn del(&self, k: &str) -> Result<()> {
        self.metas.delete_key(k).await?;
        Ok(())
    }
}

#[async_trait]
impl MetaStorage for BonsaiDB {
    type Error = Error;

    async fn register_topic(&self, topic_name: &str, broker_addr: &SocketAddr) -> Result<()> {
        let key = format!("{}-{}", BROKER_TOPIC_KEY, topic_name);
        self.metas.set_key(key, &broker_addr).await?;
        Ok(())
    }

    async fn get_topic_owner(&self, topic_name: &str) -> Result<Option<SocketAddr>> {
        let key = format!("{}-{}", BROKER_TOPIC_KEY, topic_name);
        Ok(self.metas.get_key(key).into().await?)
    }

    async fn add_subscription(&self, info: &SubscriptionInfo) -> Result<()> {
        let id_key = topic_key(&info.topic, SUBSCRIPTION_NAMES_KEY);
        let names = self.get(&id_key).await?;
        match names {
            Some(names) => {
                let mut names = {
                    let mut buf = Bytes::copy_from_slice(&names);
                    String::decode(&mut buf)?
                };
                names.push(',');
                names.push_str(&info.name);
                let mut buf = BytesMut::new();
                names.encode(&mut buf);
                self.put(SUBSCRIPTION_NAMES_KEY, &buf).await?;
            }
            None => {
                let mut buf = BytesMut::new();
                info.name.clone().encode(&mut buf);
                self.put(SUBSCRIPTION_NAMES_KEY, &buf).await?;
            }
        }
        let id_key = topic_key(&info.topic, &format!("{}-{}", SUBSCRIPTION_KEY, &info.name));
        let mut buf = BytesMut::new();
        info.encode(&mut buf);
        self.put(&id_key, &buf).await?;
        Ok(())
    }

    async fn all_subscription(&self, topic_name: &str) -> Result<Vec<SubscriptionInfo>> {
        let id_key = topic_key(topic_name, SUBSCRIPTION_NAMES_KEY);
        let names = self.get(&id_key).await?;
        let Some(names) = names else {
            return Ok(vec![])
        };

        let names = {
            let mut buf = Bytes::copy_from_slice(&names);
            String::decode(&mut buf)?
                .split(',')
                .map(|s| s.to_string())
                .collect::<Vec<String>>()
        };
        let mut subs = Vec::with_capacity(names.len());
        for i in names {
            let key = topic_key(topic_name, &format!("{}-{}", SUBSCRIPTION_KEY, i));
            let sub = self.get(&key).await?;
            let Some(sub) = sub else {
                continue;
            };
            let mut buf = Bytes::copy_from_slice(&sub);
            subs.push(SubscriptionInfo::decode(&mut buf)?);
        }
        Ok(subs)
    }

    async fn del_subscription(&self, topic_name: &str, name: &str) -> Result<()> {
        let id_key = topic_key(topic_name, SUBSCRIPTION_NAMES_KEY);
        let names = self.get(&id_key).await?;
        let Some(names) = names else {
            return Ok(())
        };
        let names = {
            let mut buf = Bytes::copy_from_slice(&names);
            String::decode(&mut buf)?
                .split(',')
                .filter_map(|s| if s != name { Some(s.to_string()) } else { None })
                .collect::<Vec<String>>()
                .join(",")
        };
        let mut buf = BytesMut::new();
        names.encode(&mut buf);
        self.put(SUBSCRIPTION_NAMES_KEY, &buf).await?;

        self.del(&format!("{}-{}", SUBSCRIPTION_KEY, &name)).await?;
        Ok(())
    }

    async fn get_u64(&self, k: &str) -> Result<Option<u64>> {
        let Some(v) = self.metas.get_key(k).await? else {
            return Ok(None)
        };
        match v {
            Value::Numeric(Numeric::UnsignedInteger(n)) => Ok(Some(n)),
            _ => Ok(None),
        }
    }

    async fn put_u64(&self, k: &str, v: u64) -> Result<()> {
        self.metas.set_numeric_key(k, v).await?;
        Ok(())
    }

    async fn inc_u64(&self, k: &str, v: u64) -> Result<u64> {
        let v = self.metas.increment_key_by(k, v).await?;
        Ok(v)
    }
}

#[async_trait]
impl MessageStorage for BonsaiDB {
    type Error = Error;

    async fn put_message(&self, msg: &TopicMessage) -> Result<()> {
        let MessageId {
            topic_id,
            cursor_id,
        } = msg.message_id;
        let db = self.messages.with_key_namespace(&ns(topic_id));
        let mut bytes = BytesMut::new();
        msg.encode(&mut bytes);
        db.set_binary_key(cursor_id.to_string(), &bytes).await?;
        Ok(())
    }

    async fn get_message(&self, id: &MessageId) -> Result<Option<TopicMessage>> {
        let MessageId {
            topic_id,
            cursor_id,
        } = id;
        let db = self.messages.with_key_namespace(&ns(*topic_id));
        Ok(db
            .get_key(cursor_id.to_string())
            .await?
            .and_then(|v| match v {
                Value::Bytes(b) => {
                    let mut buf = Bytes::copy_from_slice(&b);
                    Some(TopicMessage::decode(&mut buf))
                }
                Value::Numeric(_) => None,
            })
            .transpose()?)
    }

    async fn del_message(&self, id: &MessageId) -> Result<()> {
        let MessageId {
            topic_id,
            cursor_id,
        } = id;
        let db = self.messages.with_key_namespace(&ns(*topic_id));
        db.delete_key(cursor_id.to_string()).await?;
        Ok(())
    }

    async fn save_cursor(&self, topic_name: &str, sub_name: &str, bytes: &[u8]) -> Result<()> {
        let key = format!("{}-{}-{}", CURSOR_KEY, topic_name, sub_name);
        self.put(&key, bytes).await?;
        Ok(())
    }

    async fn load_cursor(&self, topic_name: &str, sub_name: &str) -> Result<Option<Vec<u8>>> {
        let key = format!("{}-{}-{}", CURSOR_KEY, topic_name, sub_name);
        self.get(&key).await
    }
}

fn ns(topic_id: u64) -> String {
    format!("topic_{topic_id}")
}

fn topic_key(topic_name: &str, s: &str) -> String {
    format!("TOPIC-{}-{}", topic_name, s)
}
