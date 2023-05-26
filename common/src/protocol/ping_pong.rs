#[derive(Debug, PartialEq, Clone, bud_derive::PacketCodec)]
pub struct Ping {
    pub request_id: u64,
}

#[derive(Debug, PartialEq, Clone, bud_derive::PacketCodec)]
pub struct Pong {
    pub request_id: u64,
}
