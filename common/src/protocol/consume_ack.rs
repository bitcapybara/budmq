use crate::types::MessageId;

#[derive(Debug, PartialEq, Clone, bud_derive::PacketCodec)]
pub struct ConsumeAck {
    pub consumer_id: u64,
    pub message_ids: Vec<MessageId>,
}
