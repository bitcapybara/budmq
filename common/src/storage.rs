use std::array;

use async_trait::async_trait;

use crate::{
    codec,
    types::{MessageId, TopicMessage},
};

pub mod memory;
pub mod persist;

pub type Result<T> = std::result::Result<T, Error>;

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("Decode slice error: {0}")]
    DecodeSlice(#[from] array::TryFromSliceError),
    #[error("Persistent database error: {0}")]
    PersistDb(String),
    #[error("Protocol codec error: {0}")]
    Codec(#[from] codec::Error),
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
