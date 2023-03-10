use super::{Codec, Packet, Result, ReturnCode};

pub struct Unsubscribe {
    /// consumer id
    pub consumer_id: String,
}

impl Codec for Unsubscribe {
    fn decode(buf: &mut bytes::BytesMut) -> Result<Self> {
        todo!()
    }

    fn encode(self, buf: &mut bytes::BytesMut) -> Result<()> {
        todo!()
    }
}
