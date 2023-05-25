pub(crate) mod broker;
pub(crate) mod cursor;
pub(crate) mod topic;

use std::{
    array, io,
    ops::{Bound, RangeBounds, RangeInclusive},
    string,
};

use bud_common::{
    protocol::{self, get_u64, get_u8, read_bytes, read_string, write_bytes, write_string},
    storage,
    types::MessageId,
};
use bytes::{BufMut, Bytes, BytesMut};

use crate::topic::{SubscriptionInfo, TopicMessage};

pub type Result<T> = std::result::Result<T, Error>;

#[derive(Debug)]
pub enum Error {
    InvalidRange,
    Io(io::Error),
    DecodeSlice(array::TryFromSliceError),
    DecodeString(string::FromUtf8Error),
    Storage(storage::Error),
    Protocol(protocol::Error),
}

impl std::error::Error for Error {}

impl std::fmt::Display for Error {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Error::InvalidRange => write!(f, "invalid range"),
            Error::Io(e) => write!(f, "io error: {e}"),
            Error::DecodeSlice(e) => write!(f, "decode slice error: {e}"),
            Error::DecodeString(e) => write!(f, "decode string error: {e}"),
            Error::Storage(e) => write!(f, "storage error: {e}"),
            Error::Protocol(e) => write!(f, "decode protocol error: {e}"),
        }
    }
}

impl From<io::Error> for Error {
    fn from(e: io::Error) -> Self {
        Self::Io(e)
    }
}

impl From<storage::Error> for Error {
    fn from(e: storage::Error) -> Self {
        Self::Storage(e)
    }
}

impl From<array::TryFromSliceError> for Error {
    fn from(e: array::TryFromSliceError) -> Self {
        Self::DecodeSlice(e)
    }
}

impl From<string::FromUtf8Error> for Error {
    fn from(e: string::FromUtf8Error) -> Self {
        Self::DecodeString(e)
    }
}

impl From<protocol::Error> for Error {
    fn from(e: protocol::Error) -> Self {
        Self::Protocol(e)
    }
}

trait Codec {
    fn encode(&self) -> Vec<u8>;
    fn decode(bytes: &[u8]) -> Result<Self>
    where
        Self: Sized;
}

impl Codec for TopicMessage {
    fn encode(&self) -> Vec<u8> {
        // topic_name_len + topic_name + cursor_id + seq_id + payload_len + payload
        let buf_len = 8 + 8 + 2 + self.topic_name.len() + 8 + 8 + 2 + self.payload.len();
        let mut buf = BytesMut::with_capacity(buf_len);
        write_bytes(&mut buf, self.message_id.encode().as_slice());
        write_string(&mut buf, &self.topic_name);
        buf.put_u64(self.seq_id);
        write_bytes(&mut buf, &self.payload);
        buf.to_vec()
    }

    fn decode(bytes: &[u8]) -> Result<Self> {
        let mut buf = Bytes::copy_from_slice(bytes);
        let message_id = MessageId::decode(&read_bytes(&mut buf)?)?;
        let topic_name = read_string(&mut buf)?;
        let seq_id = get_u64(&mut buf)?;
        let payload = read_bytes(&mut buf)?;
        Ok(Self {
            seq_id,
            payload,
            topic_name,
            message_id,
        })
    }
}

impl Codec for SubscriptionInfo {
    fn encode(&self) -> Vec<u8> {
        // topic_len + topic + name_len + name + sub_type + init_position
        let buf_len = 2 + self.topic.len() + 2 + self.name.len() + 1 + 1;
        let mut buf = BytesMut::with_capacity(buf_len);
        write_string(&mut buf, &self.topic);
        write_string(&mut buf, &self.name);
        buf.put_u8(self.sub_type as u8);
        buf.put_u8(self.init_position as u8);
        buf.to_vec()
    }

    fn decode(bytes: &[u8]) -> Result<Self> {
        let mut buf = Bytes::copy_from_slice(bytes);
        let topic = read_string(&mut buf)?;
        let name = read_string(&mut buf)?;
        let sub_type = get_u8(&mut buf)?.try_into()?;
        let init_position = get_u8(&mut buf)?.try_into()?;
        Ok(Self {
            topic,
            name,
            sub_type,
            init_position,
        })
    }
}

fn get_range<R>(range: R) -> Result<RangeInclusive<u64>>
where
    R: RangeBounds<u64>,
{
    let start = match range.start_bound() {
        Bound::Included(&i) => i,
        Bound::Excluded(&u64::MAX) => return Err(Error::InvalidRange),
        Bound::Excluded(&i) => i + 1,
        Bound::Unbounded => 0,
    };
    let end = match range.end_bound() {
        Bound::Included(&i) => i,
        Bound::Excluded(&0) => return Err(Error::InvalidRange),
        Bound::Excluded(&i) => i - 1,
        Bound::Unbounded => u64::MAX,
    };
    if end < start {
        return Err(Error::InvalidRange);
    }

    Ok(start..=end)
}
