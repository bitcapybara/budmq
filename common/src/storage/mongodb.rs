use mongodb::bson::DateTime;

use crate::types::{MessageId, TopicMessage};

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

#[derive(Debug, serde::Serialize, serde::Deserialize)]
pub struct MongoTopicMessage {
    pub message_id: MongoMessageId,
    pub topic_name: String,
    pub seq_id: u64,
    pub payload: Vec<u8>,
    pub produce_time: DateTime,
}

impl From<TopicMessage> for MongoTopicMessage {
    fn from(m: TopicMessage) -> Self {
        Self {
            message_id: m.message_id.into(),
            topic_name: m.topic_name,
            seq_id: m.seq_id,
            payload: m.payload.to_vec(),
            produce_time: m.produce_time.into(),
        }
    }
}
