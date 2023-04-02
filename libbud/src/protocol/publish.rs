use bytes::Bytes;

use super::{Codec, Result};

pub struct Publish {
    /// pub subject
    pub topic: String,
    /// Ensure that the message sent by the producer is unique within this topic
    pub sequence_id: u64,
    /// message content
    pub payload: Bytes,
}

impl Codec for Publish {
    fn decode(buf: bytes::Bytes) -> Result<Self> {
        todo!()
    }

    fn encode(&self, buf: &mut bytes::BytesMut) -> Result<()> {
        todo!()
    }
}
