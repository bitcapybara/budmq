use bytes::Bytes;
use chrono::{DateTime, Utc};

#[derive(Debug, PartialEq, Clone, bud_derive::PacketCodec)]
pub struct Publish {
    pub producer_id: u64,
    /// pub subject
    pub topic: String,
    /// Ensure that the message sent by the producer is unique within this topic
    pub sequence_id: u64,
    /// message content
    pub payload: Bytes,
    /// produce time
    pub produce_time: DateTime<Utc>,
}

#[derive(Debug, PartialEq, Clone, bud_derive::PacketCodec)]
pub struct PublishBatch {
    pub producer_id: u64,
    /// pub subject
    pub topic: String,
    /// Ensure that the message sent by the producer is unique within this topic
    /// indicate the max seq-id of payload
    pub sequence_id: u64,
    /// message content
    pub payloads: Vec<Bytes>,
    /// produce time
    pub produce_time: DateTime<Utc>,
}
