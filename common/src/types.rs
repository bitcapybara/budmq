#[derive(Debug, Clone, Copy, PartialEq, bud_derive::Codec)]
#[repr(u8)]
pub enum SubType {
    /// Each subscription is only allowed to contain one client
    Exclusive = 1,
    /// Each subscription allows multiple clients
    Shared,
}

impl TryFrom<u8> for SubType {
    type Error = crate::protocol::Error;

    fn try_from(value: u8) -> crate::protocol::Result<Self> {
        Ok(match value {
            1 => Self::Exclusive,
            2 => Self::Shared,
            _ => return Err(Self::Error::UnsupportedSubType),
        })
    }
}

#[derive(Debug, Clone, Copy, PartialEq, bud_derive::Codec)]
#[repr(u8)]
pub enum InitialPostion {
    Latest = 1,
    Earliest,
}

impl TryFrom<u8> for InitialPostion {
    type Error = crate::protocol::Error;

    fn try_from(value: u8) -> crate::protocol::Result<Self> {
        Ok(match value {
            1 => Self::Latest,
            2 => Self::Earliest,
            _ => return Err(Self::Error::UnsupportedInitPosition),
        })
    }
}

#[derive(Debug, Clone, Copy, PartialEq, bud_derive::Codec)]
#[repr(u8)]
pub enum AccessMode {
    Exclusive = 1,
    Shared,
}

impl TryFrom<u8> for AccessMode {
    type Error = crate::protocol::Error;

    fn try_from(value: u8) -> crate::protocol::Result<Self> {
        Ok(match value {
            1 => Self::Exclusive,
            2 => Self::Shared,
            _ => return Err(Self::Error::UnsupportedAccessMode),
        })
    }
}

#[derive(Debug, Clone, Copy, PartialOrd, PartialEq, bud_derive::Codec)]
pub struct MessageId {
    pub topic_id: u64,
    pub cursor_id: u64,
}

impl MessageId {
    pub fn new(topic_id: u64, cursor_id: u64) -> Self {
        Self {
            topic_id,
            cursor_id,
        }
    }
}
