use bytes::BufMut;

use super::{get_u16, get_u64, Codec, Header, PacketType, Result};

#[derive(Debug, Clone, Copy, PartialEq)]
pub struct Connect {
    pub request_id: u64,
    /// keepalive(ms)
    pub keepalive: u16,
}

impl Codec for Connect {
    fn decode(mut buf: bytes::Bytes) -> Result<Self> {
        let request_id = get_u64(&mut buf)?;
        let keepalive = get_u16(&mut buf)?;
        Ok(Self {
            request_id,
            keepalive,
        })
    }

    fn encode(&self, buf: &mut bytes::BytesMut) -> Result<()> {
        buf.put_u64(self.request_id);
        buf.put_u16(self.keepalive);
        Ok(())
    }

    fn header(&self) -> Header {
        Header {
            type_byte: PacketType::Connect as u8,
            remain_len: 8 + 2,
        }
    }
}
