use super::{Ack, Codec, Packet, Result, ReturnCode};

#[derive(Debug, Clone, Copy)]
pub struct Connect {
    request_id: u64,
    /// keepalive(ms)
    pub keepalive: u16,
}

impl Connect {
    pub fn ack(&self, return_code: ReturnCode) -> Packet {
        Packet::ack(self.request_id, return_code)
    }
}

impl Codec for Connect {
    fn decode(buf: &mut bytes::BytesMut) -> Result<Self> {
        todo!()
    }

    fn encode(self, buf: &mut bytes::BytesMut) -> Result<()> {
        todo!()
    }
}
