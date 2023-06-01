#[derive(Debug, PartialEq, Clone, bud_derive::PacketCodec)]
pub struct Unsubscribe {
    /// consumer id
    pub consumer_id: u64,
}
