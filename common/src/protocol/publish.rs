use bytes::Bytes;
use chrono::{DateTime, Utc};

#[derive(Debug, PartialEq, Clone, bud_derive::PacketCodec)]
pub struct Publish {
    pub producer_id: u64,
    /// pub subject
    pub topic: String,
    /// Ensure that the message sent by the producer is unique within this topic
    /// indicate the first seq-id of payloads
    pub start_seq_id: u64,
    /// message content
    pub payloads: Vec<Bytes>,
    /// produce time
    pub produce_time: DateTime<Utc>,
}
