#[derive(Debug, PartialEq, Clone, bud_derive::Codec)]
pub struct Producer {
    pub request_id: u64,
    pub producer_name: String,
    pub topic_name: String,
}

#[derive(Debug, PartialEq, Clone, bud_derive::Codec)]
pub struct ProducerReceipt {
    pub request_id: u64,
    pub producer_id: u64,
    pub sequence_id: u64,
}

#[derive(Debug, PartialEq, Clone, bud_derive::Codec)]
pub struct CloseProducer {
    pub producer_id: u64,
}
