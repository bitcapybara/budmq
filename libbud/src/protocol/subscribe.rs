use super::{Codec, Packet, Result, ReturnCode};

pub enum SubType {
    /// Each subscription is only allowed to contain one client
    Exclusive,
    /// Each subscription allows multiple clients
    Shared,
}

pub struct Subscribe {
    /// subscribe topic
    pub topic: String,
    /// subscription id
    pub sub_id: String,
    /// subscribe type
    pub sub_type: SubType,
}

impl Codec for Subscribe {
    fn decode(buf: &mut bytes::BytesMut) -> Result<Self> {
        todo!()
    }

    fn encode(self, buf: &mut bytes::BytesMut) -> Result<()> {
        todo!()
    }
}
