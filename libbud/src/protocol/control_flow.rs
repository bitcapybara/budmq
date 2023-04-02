use super::{Codec, Result};

pub struct ControlFlow {
    pub consumer_id: u64,
    /// number of additional messages requested
    pub permits: u32,
}

impl Codec for ControlFlow {
    fn decode(buf: bytes::Bytes) -> Result<Self> {
        todo!()
    }

    fn encode(&self, buf: &mut bytes::BytesMut) -> Result<()> {
        todo!()
    }
}
