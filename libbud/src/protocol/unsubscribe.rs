use bytes::{Buf, BufMut};

use super::{assert_len, Codec, Header, PacketType, Result};

pub struct Unsubscribe {
    /// consumer id
    pub consumer_id: u64,
}

impl Codec for Unsubscribe {
    fn decode(mut buf: bytes::Bytes) -> Result<Self> {
        assert_len(&buf, 8)?;
        let consumer_id = buf.get_u64();
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
