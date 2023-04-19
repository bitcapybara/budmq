use bytes::BufMut;

use super::{get_u64, Codec, Header, PacketType, Result};

pub struct Unsubscribe {
    /// consumer id
    pub consumer_id: u64,
}

impl Codec for Unsubscribe {
    fn decode(mut buf: bytes::Bytes) -> Result<Self> {
        let consumer_id = get_u64(&mut buf)?;
        Ok(Self { consumer_id })
    }

    fn encode(&self, buf: &mut bytes::BytesMut) -> Result<()> {
        buf.put_u64(self.consumer_id);
        Ok(())
    }

    fn header(&self) -> Header {
        Header::new(PacketType::Unsubscribe, 8)
    }
}
