use std::{collections::HashMap, sync::Arc};

use async_trait::async_trait;
use bonsaidb::{
    core::keyvalue::{AsyncKeyValue, Numeric, Value},
    local::{
        config::{Builder, StorageConfiguration},
        AsyncDatabase,
    },
};
use bytes::{Bytes, BytesMut};
use tokio::sync::RwLock;

use crate::{
    codec::{self, Codec},
    types::{MessageId, TopicMessage},
};

use super::{MessageStorage, MetaStorage};

pub type Result<T> = std::result::Result<T, Error>;

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("Persistent database error: {0}")]
    BonsaiDB(String),
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
    /// topic_id -> database(cursor_id -> message)
    message_dbs: Arc<RwLock<HashMap<u64, AsyncDatabase>>>,
}

impl BonsaiDB {
    async fn new() -> Result<Self> {
        let config = StorageConfiguration::new("data/meta.db");
        Ok(Self {
            metas: AsyncDatabase::open::<()>(config).await?,
            message_dbs: Arc::new(RwLock::new(HashMap::new())),
        })
    }

    async fn get_db(&self, topic_id: u64) -> Result<AsyncDatabase> {
        let mut dbs = self.message_dbs.write().await;
        let db = match dbs.get(&topic_id) {
            Some(db) => db.clone(),
            None => {
                let db = new_database(topic_id).await?;
                dbs.insert(topic_id, db.clone());
                db
            }
        };
        Ok(db)
    }
}

#[async_trait]
impl MetaStorage for BonsaiDB {
    type Error = Error;

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

    async fn get_u64(&self, k: &str) -> Result<Option<u64>> {
        let Some(v) = self.metas.get_key(k).await? else {
            return Ok(None)
        };
        match v {
            Value::Numeric(Numeric::UnsignedInteger(n)) => Ok(Some(n)),
            _ => Ok(None),
        }
    }

    async fn set_u64(&self, k: &str, v: u64) -> Result<()> {
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
        let db = self.get_db(topic_id).await?;
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
        let db = self.get_db(*topic_id).await?;
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
        let db = self.get_db(*topic_id).await?;
        db.delete_key(cursor_id.to_string()).await?;
        Ok(())
    }
}

async fn new_database(topic_id: u64) -> Result<AsyncDatabase> {
    let config = StorageConfiguration::new(format!("data/{topic_id}.db"));
    Ok(AsyncDatabase::open::<()>(config).await?)
}
