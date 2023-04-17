mod cursor;
mod topic;

use std::{
    array, io,
    ops::{Bound, RangeBounds, RangeInclusive},
    string,
};

use bud_common::storage;
use bytes::{Buf, Bytes};

use crate::topic::{Message, SubscriptionId};

pub(crate) use cursor::CursorStorage;
pub(crate) use topic::TopicStorage;

pub type Result<T> = std::result::Result<T, Error>;

#[derive(Debug)]
pub enum Error {
    InvalidRange,
    Io(io::Error),
    DecodeSlice(array::TryFromSliceError),
    DecodeString(string::FromUtf8Error),
    Storage(storage::Error),
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
trait Codec {
    fn to_vec(&self) -> Vec<u8>;
    fn from_bytes(bytes: &[u8]) -> Result<Self>
    where
        Self: Sized;
}

impl Codec for Message {
    fn to_vec(&self) -> Vec<u8> {
        let mut bytes = Vec::with_capacity(8 + self.payload.len());
        bytes.extend_from_slice(&self.seq_id.to_be_bytes());
        bytes.extend_from_slice(&self.payload);
        bytes
    }

    fn from_bytes(bytes: &[u8]) -> Result<Self> {
        let (seq_id_bytes, payload_bytes) = bytes.split_at(8);
        let seq_id = u64::from_be_bytes(seq_id_bytes.try_into()?);
        let payload = Bytes::copy_from_slice(payload_bytes);
        Ok(Self { seq_id, payload })
    }
}

impl Codec for SubscriptionId {
    fn to_vec(&self) -> Vec<u8> {
        let mut bytes = Vec::with_capacity(2 + self.topic.len() + 2 + self.name.len());
        bytes.extend_from_slice(&self.topic.len().to_be_bytes());
        bytes.extend_from_slice(self.topic.as_bytes());
        bytes.extend_from_slice(&self.name.len().to_be_bytes());
        bytes.extend_from_slice(self.name.as_bytes());
        bytes
    }

    fn from_bytes(bytes: &[u8]) -> Result<Self> {
        let mut bytes = Bytes::copy_from_slice(bytes);
        let topic = read_string(&mut bytes)?;
        let name = read_string(&mut bytes)?;
        Ok(Self { topic, name })
    }
}

fn read_string(buf: &mut Bytes) -> Result<String> {
    let len = buf.get_u16() as usize;
    let bytes = buf.split_to(len);
    Ok(String::from_utf8(bytes.to_vec())?)
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
