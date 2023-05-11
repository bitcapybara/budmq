#[derive(Debug, PartialEq, Clone, codec_derive::Codec)]
pub struct Send {
    pub request_id: u64,
    pub message_id: u64,
    pub consumer_id: u64,
    pub payload: Bytes,
}
