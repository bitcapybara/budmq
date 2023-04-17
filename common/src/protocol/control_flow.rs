use bytes::{Buf, BufMut};

use super::{assert_len, Codec, Header, PacketType, Result};

pub struct ControlFlow {
    pub consumer_id: u64,
    /// number of additional messages requested
    pub permits: u32,
}

impl Codec for ControlFlow {
    fn decode(mut buf: bytes::Bytes) -> Result<Self> {
        assert_len(&buf, 8)?;
        let consumer_id = buf.get_u64();
        assert_len(&buf, 4)?;
        let permits = buf.get_u32();
        Ok(Self {
            consumer_id,
            permits,
        })
    }

    fn encode(&self, buf: &mut bytes::BytesMut) -> Result<()> {
        buf.put_u64(self.consumer_id);
        buf.put_u32(self.permits);
        Ok(())
    }

    fn header(&self) -> Header {
        Header::new(PacketType::ControlFlow, 8 + 4)
    }
}
