#[derive(Debug, PartialEq, Clone, bud_derive::Codec)]
pub struct Ping {
    pub request_id: u64,
}

#[derive(Debug, PartialEq, Clone, bud_derive::Codec)]
pub struct Pong {
    pub request_id: u64,
}
