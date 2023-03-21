use super::{Codec, Result};

pub struct Unsubscribe {
    /// consumer id
    pub consumer_id: u64,
}

impl Codec for Unsubscribe {
    fn decode(buf: &mut bytes::BytesMut) -> Result<Self> {
        todo!()
    }

    fn encode(self, buf: &mut bytes::BytesMut) -> Result<()> {
        todo!()
    }
}
