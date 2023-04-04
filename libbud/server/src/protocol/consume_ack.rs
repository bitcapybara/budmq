use bytes::{Buf, BufMut};

use super::{assert_len, Codec, Header, PacketType, Result};

pub struct ConsumeAck {
    pub consumer_id: u64,
    /// which message to ack
    pub message_id: u64,
}

impl Codec for ConsumeAck {
    fn decode(mut buf: bytes::Bytes) -> Result<Self> {
        assert_len(&buf, 8)?;
        let consumer_id = buf.get_u64();
        assert_len(&buf, 8)?;
        let message_id = buf.get_u64();
        Ok(Self {
            consumer_id,
            message_id,
        })
    }

    fn encode(&self, buf: &mut bytes::BytesMut) -> Result<()> {
        buf.put_u64(self.consumer_id);
        buf.put_u64(self.message_id);
        Ok(())
    }

    fn header(&self) -> Header {
        Header::new(PacketType::ConsumeAck, 8 + 8)
    }
}
