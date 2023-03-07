use super::{Codec, Packet, Result, ReturnCode};

pub struct Unsubscribe {
    pub request_id: u64,
    pub sub_id: String,
}

impl Unsubscribe {
    pub fn ack(&self, return_code: ReturnCode) -> Packet {
        Packet::ack(self.request_id, return_code)
    }
}

impl Codec for Unsubscribe {
    fn decode(buf: &mut bytes::BytesMut) -> Result<Self> {
        todo!()
    }

    fn encode(self, buf: &mut bytes::BytesMut) -> Result<()> {
        todo!()
    }
}
