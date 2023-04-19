use bytes::{BufMut, Bytes};

use super::{
    get_u64, read_bytes, read_string, write_bytes, write_string, Codec, Header, PacketType, Result,
};

pub struct Publish {
    /// pub subject
    pub topic: String,
    /// Ensure that the message sent by the producer is unique within this topic
    pub sequence_id: u64,
    /// message content
    pub payload: Bytes,
}

impl Codec for Publish {
    fn decode(mut buf: bytes::Bytes) -> Result<Self> {
        let topic = read_string(&mut buf)?;
        let sequence_id = get_u64(&mut buf)?;
        let payload = read_bytes(&mut buf)?;
        Ok(Self {
            topic,
            sequence_id,
            payload,
        })
    }

    fn encode(&self, buf: &mut bytes::BytesMut) -> Result<()> {
        write_string(buf, &self.topic);
        buf.put_u64(self.sequence_id);
        write_bytes(buf, &self.payload);
        Ok(())
    }

    fn header(&self) -> Header {
        Header::new(
            PacketType::Publish,
            self.topic.len() + 8 + self.payload.len(),
        )
    }
}
