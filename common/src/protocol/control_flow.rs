#[derive(Debug, PartialEq, Clone, codec_derive::Codec)]
pub struct ControlFlow {
    pub request_id: u64,
    pub consumer_id: u64,
    /// number of additional messages requested
    pub permits: u32,
}
