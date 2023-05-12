#[derive(Debug, PartialEq, Clone, bud_derive::Codec)]
pub struct Publish {
    pub request_id: u64,
    /// pub subject
    pub topic: String,
    /// Ensure that the message sent by the producer is unique within this topic
    pub sequence_id: u64,
    /// message content
    pub payload: Bytes,
}
