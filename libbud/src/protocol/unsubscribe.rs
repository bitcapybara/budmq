use super::{Codec, Packet, Result, ReturnCode};

pub struct Unsubscribe {
    /// subscription id
    pub sub_id: String,
}

impl Codec for Unsubscribe {
    fn decode(buf: &mut bytes::BytesMut) -> Result<Self> {
        todo!()
    }

    fn encode(self, buf: &mut bytes::BytesMut) -> Result<()> {
        todo!()
    }
}
