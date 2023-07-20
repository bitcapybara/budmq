pub(crate) mod broker;
pub(crate) mod cursor;
pub(crate) mod topic;

use std::io;

use bud_common::{codec, wrap_error_impl};

use crate::error::WrapError;

pub type Result<T> = std::result::Result<T, Error>;

#[derive(Debug, thiserror::Error)]
pub enum Error {
    /// roaring map seriealize/deserialize
    #[error("I/O error: {0}")]
    Io(String),
    /// storage error from custom storage impls
    #[error("Storage error: {0}")]
    Storage(String),
    /// codec error in storage serialize/deserialize
    #[error("Protocol Codec error: {0}")]
    Codec(#[from] codec::Error),
}

wrap_error_impl!(io::Error, Error::Io);
