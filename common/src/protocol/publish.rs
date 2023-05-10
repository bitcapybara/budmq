use bytes::{BufMut, Bytes};

use super::{
    get_u64, read_bytes, read_string, write_bytes, write_string, Codec, Header, PacketType, Result,
};

#[derive(Debug, PartialEq, Clone)]
pub struct Publish {
    pub request_id: u64,
    /// pub subject
    pub topic: String,
    /// Ensure that the message sent by the producer is unique within this topic
    pub sequence_id: u64,
    /// message content
    pub payload: Bytes,
}

impl Codec for Publish {
    fn decode(mut buf: bytes::Bytes) -> Result<Self> {
        let request_id = get_u64(&mut buf)?;
        let topic = read_string(&mut buf)?;
        let sequence_id = get_u64(&mut buf)?;
        let payload = read_bytes(&mut buf)?;
        Ok(Self {
            request_id,
            topic,
            sequence_id,
            payload,
        })
    }

    fn encode(&self, buf: &mut bytes::BytesMut) -> Result<()> {
        buf.put_u64(self.request_id);
        write_string(buf, &self.topic);
        buf.put_u64(self.sequence_id);
        write_bytes(buf, &self.payload);
        Ok(())
    }

    fn header(&self) -> Header {
        Header::new(
            PacketType::Publish,
            8 + self.topic.len() + 2 + 8 + self.payload.len() + 2,
        )
    }
}
