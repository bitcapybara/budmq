#[derive(Debug, Clone, Copy, PartialEq, bud_derive::PacketCodec)]
pub struct Connect {
    /// keepalive(ms)
    pub keepalive: u16,
}
