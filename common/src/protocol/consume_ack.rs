#[derive(Debug, PartialEq, Clone, bud_derive::Codec)]
pub struct ConsumeAck {
    pub request_id: u64,
    pub consumer_id: u64,
    /// which message to ack
    pub message_id: u64,
}
