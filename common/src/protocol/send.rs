use bytes::{BufMut, Bytes};

use super::{get_u64, read_bytes, write_bytes, Codec, Header, PacketType, Result};

pub struct Send {
    pub message_id: u64,
    pub consumer_id: u64,
    pub payload: Bytes,
}

impl Codec for Send {
    fn decode(mut buf: bytes::Bytes) -> Result<Self> {
        let message_id = get_u64(&mut buf)?;
        let consumer_id = get_u64(&mut buf)?;
        let payload = read_bytes(&mut buf)?;
        Ok(Self {
            message_id,
            consumer_id,
            payload,
        })
    }

    fn encode(&self, buf: &mut bytes::BytesMut) -> Result<()> {
        buf.put_u64(self.message_id);
        buf.put_u64(self.consumer_id);
        write_bytes(buf, &self.payload);
        Ok(())
    }

    fn header(&self) -> Header {
        Header::new(PacketType::Send, 8 + 8 + self.payload.len())
    }
}
