#[derive(Debug, PartialEq, Clone, bud_derive::PacketCodec)]
pub struct LookupTopic {
    pub topic_name: String,
}

#[derive(Debug, PartialEq, Clone, bud_derive::PacketCodec)]
pub struct LookupTopicResponse {
    pub broker_addr: String,
    pub server_name: String,
}
