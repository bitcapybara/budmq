use super::{Codec, Packet, Result, ReturnCode};

pub struct ControlFlow {
    pub request_id: u64,
    /// number of additional messages requested
    pub permits: u32,
}

impl ControlFlow {
    pub fn ack(&self, return_code: ReturnCode) -> Packet {
        Packet::ack(self.request_id, return_code)
    }
}

impl Codec for ControlFlow {
    fn decode(buf: &mut bytes::BytesMut) -> Result<Self> {
        todo!()
    }

    fn encode(self, buf: &mut bytes::BytesMut) -> Result<()> {
        todo!()
    }
}
