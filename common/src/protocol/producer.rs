use crate::types::AccessMode;

#[derive(Debug, PartialEq, Clone, bud_derive::PacketCodec)]
pub struct CreateProducer {
    pub request_id: u64,
    pub producer_name: String,
    pub producer_id: u64,
    pub topic_name: String,
    pub access_mode: AccessMode,
}

#[derive(Debug, PartialEq, Clone, bud_derive::PacketCodec)]
pub struct ProducerReceipt {
    pub request_id: u64,
    pub producer_id: u64,
    pub sequence_id: u64,
}

#[derive(Debug, PartialEq, Clone, bud_derive::PacketCodec)]
pub struct CloseProducer {
    pub producer_id: u64,
}
