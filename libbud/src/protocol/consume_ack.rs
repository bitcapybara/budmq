use super::{Codec, Result};

pub struct ConsumeAck {
    pub consumer_id: u64,
    /// which message to ack
    pub message_id: u64,
}

impl Codec for ConsumeAck {
    fn decode(buf: bytes::Bytes) -> Result<Self> {
        todo!()
    }

    fn encode(&self, buf: &mut bytes::BytesMut) -> Result<()> {
        todo!()
    }
}
