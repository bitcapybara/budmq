use bytes::Bytes;

use super::{Codec, Result};

pub struct Send {
    pub consumer_id: u64,
    pub payload: Bytes,
}

impl Codec for Send {
    fn decode(buf: bytes::Bytes) -> Result<Self> {
        todo!()
    }

    fn encode(&self, buf: &mut bytes::BytesMut) -> Result<()> {
        todo!()
    }

    fn header(&self) -> super::Header {
        todo!()
    }
}
