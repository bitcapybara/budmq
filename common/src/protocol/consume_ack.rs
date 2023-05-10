use bytes::BufMut;

use super::{get_u64, Codec, Header, PacketType, Result};

#[derive(Debug, PartialEq, Clone)]
pub struct ConsumeAck {
    pub request_id: u64,
    pub consumer_id: u64,
    /// which message to ack
    pub message_id: u64,
}

impl Codec for ConsumeAck {
    fn decode(mut buf: bytes::Bytes) -> Result<Self> {
        let request_id = get_u64(&mut buf)?;
        let consumer_id = get_u64(&mut buf)?;
        let message_id = get_u64(&mut buf)?;
        Ok(Self {
            request_id,
            consumer_id,
            message_id,
        })
    }

    fn encode(&self, buf: &mut bytes::BytesMut) -> Result<()> {
        buf.put_u64(self.request_id);
        buf.put_u64(self.consumer_id);
        buf.put_u64(self.message_id);
        Ok(())
    }

    fn header(&self) -> Header {
        Header::new(PacketType::ConsumeAck, 8 + 8 + 8)
    }
}
