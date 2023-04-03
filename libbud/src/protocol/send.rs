use bytes::{Buf, BufMut, Bytes};

use super::{assert_len, read_bytes, write_bytes, Codec, Header, PacketType, Result};

pub struct Send {
    pub consumer_id: u64,
    pub payload: Bytes,
}

impl Codec for Send {
    fn decode(mut buf: bytes::Bytes) -> Result<Self> {
        assert_len(&buf, 8)?;
        let consumer_id = buf.get_u64();
        let payload = read_bytes(&mut buf)?;
        Ok(Self {
            consumer_id,
            payload,
        })
    }

    fn encode(&self, buf: &mut bytes::BytesMut) -> Result<()> {
        buf.put_u64(self.consumer_id);
        write_bytes(buf, &self.payload);
        Ok(())
    }

    fn header(&self) -> Header {
        Header::new(PacketType::Send, 8 + self.payload.len())
    }
}
