use bytes::BufMut;

use super::{get_u32, get_u64, Codec, Header, PacketType, Result};

pub struct ControlFlow {
    pub consumer_id: u64,
    /// number of additional messages requested
    pub permits: u32,
}

impl Codec for ControlFlow {
    fn decode(mut buf: bytes::Bytes) -> Result<Self> {
        let consumer_id = get_u64(&mut buf)?;
        let permits = get_u32(&mut buf)?;
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
