pub(crate) mod broker;
pub(crate) mod cursor;
pub(crate) mod topic;

use std::{
    array, io,
    ops::{Bound, RangeBounds, RangeInclusive},
    string,
};

use bud_common::{protocol, storage};

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
