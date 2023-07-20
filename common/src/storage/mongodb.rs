use std::{collections::HashMap, sync::Arc};

use async_trait::async_trait;
use bytes::Bytes;
use mongodb::{
    bson::{doc, spec::BinarySubtype, Binary, Bson, DateTime},
    options::ReplaceOptions,
    Collection, Database,
};
use tokio::sync::RwLock;

use crate::{
    error::WrapError,
    types::{MessageId, TopicMessage},
};

use super::MessageStorage;

pub type Result<T> = std::result::Result<T, Error>;

#[derive(Debug, thiserror::Error)]
pub enum Error {
    /// error from mongodb
    #[error("MongoDB error: {0}")]
    MongoDB(String),
}

impl<T> WrapError<T, Error> for std::result::Result<T, mongodb::error::Error> {
    fn wrap<C>(self, context: C) -> std::result::Result<T, Error>
    where
        C: std::fmt::Display + Send + Sync + 'static,
    {
        self.map_err(|e| Error::MongoDB(format!("{e}: {context}")))
    }

    fn with_wrap<F, C>(self, context: F) -> std::result::Result<T, Error>
    where
        F: FnOnce() -> C,
        C: std::fmt::Display + Send + Sync + 'static,
    {
        self.map_err(|e| Error::MongoDB(format!("{e}: {}", context())))
    }
}

#[derive(Debug, serde::Serialize, serde::Deserialize)]
pub struct MongoMessageId {
    pub topic_id: u64,
    pub cursor_id: u64,
}

impl From<MessageId> for MongoMessageId {
    fn from(m: MessageId) -> Self {
        Self {
            topic_id: m.topic_id,
            cursor_id: m.cursor_id,
        }
    }
}

impl From<MongoMessageId> for MessageId {
    fn from(m: MongoMessageId) -> Self {
        Self {
            topic_id: m.topic_id,
            cursor_id: m.cursor_id,
        }
    }
}

#[derive(Debug, serde::Serialize, serde::Deserialize)]
pub struct MongoTopicMessage {
    pub message_id: MongoMessageId,
    pub topic_name: String,
    pub sequence_id: u64,
    pub payload: Vec<u8>,
    pub produce_time: DateTime,
}

impl From<&TopicMessage> for MongoTopicMessage {
    fn from(m: &TopicMessage) -> Self {
        Self {
            message_id: m.message_id.into(),
            topic_name: m.topic_name.clone(),
            sequence_id: m.sequence_id,
            payload: m.payload.to_vec(),
            produce_time: m.produce_time.into(),
        }
    }
}

impl From<MongoTopicMessage> for TopicMessage {
    fn from(m: MongoTopicMessage) -> Self {
        Self {
            message_id: m.message_id.into(),
            topic_name: m.topic_name,
            sequence_id: m.sequence_id,
            payload: Bytes::copy_from_slice(&m.payload),
            produce_time: m.produce_time.into(),
        }
    }
}

#[derive(Debug, serde::Serialize, serde::Deserialize)]
pub struct MongoCursor {
    pub topic_name: String,
    pub sub_name: String,
    pub bytes: Vec<u8>,
}

#[derive(Debug, Clone)]
pub struct MongoDB {
    database: Database,
    /// topic_id -> collection
    message_dbs: Arc<RwLock<HashMap<u64, Collection<MongoTopicMessage>>>>,
    /// cursor collection
    cursor: Collection<MongoCursor>,
}

impl MongoDB {
    pub fn new(database: Database) -> Self {
        let cursor = database.collection("corsor");
        Self {
            database,
            message_dbs: Arc::new(RwLock::new(HashMap::new())),
            cursor,
        }
    }

    async fn get_collection(&self, topic_id: u64) -> Collection<MongoTopicMessage> {
        let mut dbs = self.message_dbs.write().await;
        match dbs.get(&topic_id) {
            Some(c) => c.clone(),
            None => {
                let collection = self.database.collection(&format!("topic_{topic_id}"));
                dbs.insert(topic_id, collection.clone());
                collection
            }
        }
    }
}

fn binary(bytes: &[u8]) -> Bson {
    Bson::Binary(Binary {
        subtype: BinarySubtype::Generic,
        bytes: bytes.to_vec(),
    })
}

#[async_trait]
impl MessageStorage for MongoDB {
    type Error = Error;

    async fn put_message(&self, msg: &TopicMessage) -> Result<()> {
        let MessageId {
            topic_id,
            cursor_id,
        } = msg.message_id;
        let collection = self.get_collection(topic_id).await;
        let mongo_msg: MongoTopicMessage = msg.into();
        let filter = doc! {
            "message_id": {
                "topic_id": topic_id as i64,
                "cursor_id": cursor_id as i64
            },
        };
        let opts = ReplaceOptions::builder().upsert(Some(true)).build();
        collection
            .replace_one(filter, mongo_msg, Some(opts))
            .await
            .with_wrap(|| format!("put message {} error", msg.message_id))?;
        Ok(())
    }

    async fn get_message(&self, id: &MessageId) -> Result<Option<TopicMessage>> {
        let MessageId {
            topic_id,
            cursor_id,
        } = id;
        let collection = self.get_collection(*topic_id).await;
        let filter = doc! {
            "message_id": {
                "topic_id": *topic_id as i64,
                "cursor_id": *cursor_id as i64
            },
        };
        Ok(collection
            .find_one(Some(filter), None)
            .await
            .with_wrap(|| format!("get message {id} error"))?
            .map(|m| m.into()))
    }

    async fn del_message(&self, id: &MessageId) -> Result<()> {
        let MessageId {
            topic_id,
            cursor_id,
        } = id;
        let collection = self.get_collection(*topic_id).await;
        let filter = doc! {
            "message_id": {
                "topic_id": *topic_id as i64,
                "cursor_id": *cursor_id as i64
            },
        };
        collection
            .delete_one(filter, None)
            .await
            .with_wrap(|| format!("del message {id} error"))?;
        Ok(())
    }

    async fn save_cursor(&self, topic_name: &str, sub_name: &str, bytes: &[u8]) -> Result<()> {
        let filter = doc! {
            "topic_name": topic_name.to_string(),
            "sub_name": sub_name.to_string()
        };
        let cursor = MongoCursor {
            topic_name: topic_name.to_string(),
            sub_name: sub_name.to_string(),
            bytes: bytes.to_vec(),
        };
        let opts = ReplaceOptions::builder().upsert(Some(true)).build();
        self.cursor
            .replace_one(filter, cursor, Some(opts))
            .await
            .with_wrap(|| format!("save cursor {topic_name} error"))?;
        Ok(())
    }

    async fn load_cursor(&self, topic_name: &str, sub_name: &str) -> Result<Option<Vec<u8>>> {
        let filter = doc! {
            "topic_name": topic_name,
            "sub_name": sub_name,
        };
        let bytes = self
            .cursor
            .find_one(Some(filter), None)
            .await
            .with_wrap(|| format!("load cursor {topic_name} error"))?
            .map(|c| c.bytes);
        Ok(bytes)
    }
}
