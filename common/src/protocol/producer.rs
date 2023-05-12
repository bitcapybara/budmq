#[derive(Debug, PartialEq, Clone, bud_derive::Codec)]
pub struct Producer {
    pub request_id: u64,
    pub producer_name: String,
    pub topic_name: String,
}
