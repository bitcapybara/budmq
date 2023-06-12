pub(crate) mod broker;
pub(crate) mod cursor;
pub(crate) mod topic;

use std::{array, io, string};

use bud_common::codec;

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
    Storage(String),
    #[error("Protocol Codec error: {0}")]
    Codec(#[from] codec::Error),
}
