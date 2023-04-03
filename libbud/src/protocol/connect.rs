use bytes::{Buf, BufMut};

use super::{assert_len, Codec, Header, PacketType, Result};

#[derive(Debug, Clone, Copy)]
pub struct Connect {
    /// keepalive(ms)
    pub keepalive: u16,
}

impl Codec for Connect {
    fn decode(mut buf: bytes::Bytes) -> Result<Self> {
        assert_len(&buf, 2)?;
        let keepalive = buf.get_u16();
        Ok(Self { keepalive })
    }

    fn encode(&self, buf: &mut bytes::BytesMut) -> Result<()> {
        buf.put_u16(self.keepalive);
        Ok(())
    }

    fn header(&self) -> Header {
        Header {
            type_byte: PacketType::Connect as u8,
            remain_len: 2,
        }
    }
}
