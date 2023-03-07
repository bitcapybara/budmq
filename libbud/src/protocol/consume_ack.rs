use super::{Codec, Result};

pub struct ConsumeAck {
    request_id: u64,
    /// which message to ack
    message_id: u64,
}

impl Codec for ConsumeAck {
    fn decode(buf: &mut bytes::BytesMut) -> Result<Self> {
        todo!()
    }

    fn encode(self, buf: &mut bytes::BytesMut) -> Result<()> {
        todo!()
    }
}
