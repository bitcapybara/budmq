#[derive(Debug, PartialEq, Clone, bud_derive::Codec)]
pub struct ProducerReceipt {
    pub request_id: u64,
    pub producer_id: u64,
    pub sequence_id: u64,
}
