pub(crate) mod broker;
pub(crate) mod cursor;
pub(crate) mod topic;

use std::{
    array, io,
    ops::{Bound, RangeBounds, RangeInclusive},
    string,
};

use bud_common::{codec, storage};

pub type Result<T> = std::result::Result<T, Error>;

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("Invalid range")]
    InvalidRange,
    #[error("I/O error: {0}")]
    Io(#[from] io::Error),
    #[error("Decode slice error: {0}")]
    DecodeSlice(#[from] array::TryFromSliceError),
    #[error("Decode string error: {0}")]
    DecodeString(#[from] string::FromUtf8Error),
    #[error("Storage error: {0}")]
    Storage(#[from] storage::Error),
    #[error("Protocol Codec error: {0}")]
    Codec(#[from] codec::Error),
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
