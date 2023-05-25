use bytes::{BufMut, Bytes, BytesMut};

use crate::protocol::{self, get_u64};

#[derive(Debug, Clone, Copy, PartialEq)]
#[repr(u8)]
pub enum SubType {
    /// Each subscription is only allowed to contain one client
    Exclusive = 1,
    /// Each subscription allows multiple clients
    Shared,
}

#[derive(Debug, Clone, Copy, PartialEq)]
#[repr(u8)]
pub enum InitialPostion {
    Latest = 1,
    Earliest,
}

#[derive(Debug, Clone, Copy, PartialEq)]
#[repr(u8)]
pub enum AccessMode {
    Exclusive = 1,
    Shared,
}

#[derive(Debug, Clone, Copy, PartialEq)]
pub struct MessageId {
    pub topic_id: u64,
    pub cursor_id: u64,
}

impl MessageId {
    pub fn encode(&self) -> Vec<u8> {
        let mut buf = BytesMut::with_capacity(8 + 8);
        buf.put_u64(self.topic_id);
        buf.put_u64(self.cursor_id);
        buf.to_vec()
    }

    pub fn decode(bytes: &[u8]) -> protocol::Result<Self> {
        let mut buf = Bytes::copy_from_slice(bytes);
        let topic_id = get_u64(&mut buf)?;
        let cursor_id = get_u64(&mut buf)?;
        Ok(Self {
            topic_id,
            cursor_id,
        })
    }
}
