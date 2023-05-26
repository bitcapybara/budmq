#[derive(Debug, PartialEq, Clone, bud_derive::PacketCodec)]
pub struct Unsubscribe {
    pub request_id: u64,
    /// consumer id
    pub consumer_id: u64,
}
