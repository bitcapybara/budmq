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
    codec::Codec,
    types::{MessageId, TopicMessage},
};

use super::{MessageStorage, MetaStorage, Result};

#[derive(Clone)]
pub struct PersistStorage {
    metas: AsyncDatabase,
    /// topic_id -> database(cursor_id -> message)
    message_dbs: Arc<RwLock<HashMap<u64, AsyncDatabase>>>,
}

#[async_trait]
impl MetaStorage for PersistStorage {
    async fn create(id: &str) -> Result<Self> {
        let config = StorageConfiguration::new(format!("data/{id}.db"));
        let metas = AsyncDatabase::open::<()>(config).await?;
        Ok(Self {
            metas,
            message_dbs: Arc::new(RwLock::new(HashMap::new())),
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
impl MessageStorage for PersistStorage {
    async fn put_message(&self, msg: TopicMessage) -> Result<()> {
        let MessageId {
            topic_id,
            cursor_id,
        } = msg.message_id;
        let mut messages = self.message_dbs.write().await;
        let db = match messages.get(&topic_id) {
            Some(db) => db,
            None => {
                let db = new_database(topic_id).await?;
                messages.entry(topic_id).or_insert(db)
            }
        };

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
        let mut dbs = self.message_dbs.write().await;
        let db = match dbs.get(topic_id) {
            Some(db) => db,
            None => {
                let db = new_database(*topic_id).await?;
                dbs.entry(*topic_id).or_insert(db)
            }
        };
        get_message(db, *cursor_id).await
    }

    async fn del_message(&self, id: &MessageId) -> Result<()> {
        let MessageId {
            topic_id,
            cursor_id,
        } = id;
        let mut dbs = self.message_dbs.write().await;
        let db = match dbs.get(topic_id) {
            Some(db) => db,
            None => {
                let db = new_database(*topic_id).await?;
                dbs.entry(*topic_id).or_insert(db)
            }
        };
        db.delete_key(cursor_id.to_string()).await?;
        Ok(())
    }
}

async fn new_database(topic_id: u64) -> Result<AsyncDatabase> {
    let config = StorageConfiguration::new(format!("data/{topic_id}.db"));
    Ok(AsyncDatabase::open::<()>(config).await?)
}

async fn get_message(db: &AsyncDatabase, cursor_id: u64) -> Result<Option<TopicMessage>> {
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
