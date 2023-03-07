use super::{Codec, Result};

#[derive(Debug, Clone, Copy)]
pub enum ReturnCode {
    Success = 0,
    AlreadyConnected = 1,
}

pub struct Ack {
    pub request_id: u64,
    pub return_code: ReturnCode,
}

impl Codec for Ack {
    fn decode(buf: &mut bytes::BytesMut) -> Result<Self> {
        todo!()
    }

    fn encode(self, buf: &mut bytes::BytesMut) -> Result<()> {
        todo!()
    }
}
