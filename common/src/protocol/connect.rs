#[derive(Debug, Clone, Copy, PartialEq, bud_derive::PacketCodec)]
pub struct Connect {
    pub request_id: u64,
    /// keepalive(ms)
    pub keepalive: u16,
}
