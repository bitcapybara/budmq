use super::{Codec, Result};

#[derive(Debug, Clone, Copy)]
pub struct Connect {
    /// keepalive(ms)
    pub keepalive: u16,
}

impl Codec for Connect {
    fn decode(buf: bytes::Bytes) -> Result<Self> {
        todo!()
    }

    fn encode(&self, buf: &mut bytes::BytesMut) -> Result<()> {
        todo!()
    }
}
