#[derive(Debug, Clone, Copy, PartialEq, codec_derive::Codec)]
pub struct Connect {
    pub request_id: u64,
    /// keepalive(ms)
    pub keepalive: u16,
}
