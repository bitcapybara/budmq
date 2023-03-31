#[cfg(feature = "memory")]
mod memory;
#[cfg(feature = "persist")]
mod persist;

use std::ops::{Bound, RangeBounds, RangeInclusive};

#[cfg(feature = "memory")]
pub use memory::{CursorStorage, Error, Result, TopicStorage};

#[cfg(feature = "persist")]
pub use persist::{CursorStorage, Error, Result, TopicStorage};

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
