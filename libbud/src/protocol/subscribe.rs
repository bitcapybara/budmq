use super::{Ack, Codec, Packet, Result, ReturnCode};

pub enum SubType {
    /// Each subscription is only allowed to contain one client
    Exclusive,
    /// Each subscription allows multiple clients
    Shared,
}

pub struct Subscribe {
    /// genereted by client, unique within connection
    pub request_id: u64,
    /// subscribe topic
    pub topic: String,
    /// subscription id
    pub sub_id: String,
    /// subscribe type
    pub sub_type: SubType,
}

impl Subscribe {
    pub fn ack(&self, return_code: ReturnCode) -> Packet {
        Packet::ack(self.request_id, return_code)
    }
}

impl Codec for Subscribe {
    fn decode(buf: &mut bytes::BytesMut) -> Result<Self> {
        todo!()
    }

    fn encode(self, buf: &mut bytes::BytesMut) -> Result<()> {
        todo!()
    }
}
