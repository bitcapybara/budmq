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
    pub fn encode(&self, buf: &mut BytesMut) {
        buf.put_u64(self.topic_id);
        buf.put_u64(self.cursor_id);
    }

    pub fn decode(buf: &mut Bytes) -> protocol::Result<Self> {
        let topic_id = get_u64(buf)?;
        let cursor_id = get_u64(buf)?;
        Ok(Self {
            topic_id,
            cursor_id,
        })
    }
}
