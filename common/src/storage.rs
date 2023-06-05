use std::array;

use async_trait::async_trait;

use crate::{
    codec,
    types::{MessageId, TopicMessage},
};

pub mod memory;
pub mod persist;

pub type Result<T> = std::result::Result<T, Error>;

#[derive(Debug)]
pub enum Error {
    DecodeSlice(array::TryFromSliceError),
    PersistDb(String),
    Codec(codec::Error),
}

impl std::error::Error for Error {}

impl std::fmt::Display for Error {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Error::DecodeSlice(e) => write!(f, "decode from slice error: {e}"),
            Error::PersistDb(e) => write!(f, "persistent db error: {e}"),
            Error::Codec(e) => write!(f, "protocol codec error: {e}"),
        }
    }
}

impl From<array::TryFromSliceError> for Error {
    fn from(e: array::TryFromSliceError) -> Self {
        Self::DecodeSlice(e)
    }
}
impl From<bonsaidb::local::Error> for Error {
    fn from(e: bonsaidb::local::Error) -> Self {
        Self::PersistDb(e.to_string())
    }
}

impl From<bonsaidb::core::Error> for Error {
    fn from(e: bonsaidb::core::Error) -> Self {
        Self::PersistDb(e.to_string())
    }
}

impl From<codec::Error> for Error {
    fn from(e: codec::Error) -> Self {
        Self::Codec(e)
    }
}

#[async_trait]
pub trait MetaStorage: Clone + Send + Sync + 'static {
    async fn put(&self, k: &str, v: &[u8]) -> Result<()>;

    async fn get(&self, k: &str) -> Result<Option<Vec<u8>>>;

    async fn del(&self, k: &str) -> Result<()>;

    async fn get_u64(&self, k: &str) -> Result<Option<u64>> {
        Ok(self
            .get(k)
            .await?
            .map(|b| b.as_slice().try_into())
            .transpose()?
            .map(u64::from_be_bytes))
    }

    async fn set_u64(&self, k: &str, v: u64) -> Result<()> {
        Ok(self.put(k, v.to_be_bytes().as_slice()).await?)
    }

    async fn inc_u64(&self, k: &str, v: u64) -> Result<u64>;
}

#[async_trait]
pub trait MessageStorage: Clone + Send + Sync + 'static {
    async fn put_message(&self, msg: &TopicMessage) -> Result<()>;
    async fn get_message(&self, id: &MessageId) -> Result<Option<TopicMessage>>;
    async fn del_message(&self, id: &MessageId) -> Result<()>;
}
