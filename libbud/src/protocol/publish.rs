use bytes::Bytes;

use super::{Codec, Packet, Result, ReturnCode};

pub struct Publish {
    request_id: u64,
    /// Ensure that the message sent by the producer is unique
    sequence_id: u64,
    /// message content
    payload: Bytes,
}

impl Publish {
    pub fn ack(&self, return_code: ReturnCode) -> Packet {
        Packet::ack(self.request_id, return_code)
    }
}

impl Codec for Publish {
    fn decode(buf: &mut bytes::BytesMut) -> Result<Self> {
        todo!()
    }

    fn encode(self, buf: &mut bytes::BytesMut) -> Result<()> {
        todo!()
    }
}
