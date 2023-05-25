use crate::types::MessageId;

#[derive(Debug, PartialEq, Clone, bud_derive::Codec)]
pub struct Send {
    pub request_id: u64,
    pub message_id: MessageId,
    pub consumer_id: u64,
    pub payload: bytes::Bytes,
}
