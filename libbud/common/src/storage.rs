use std::array;

use async_trait::async_trait;

mod memory;

pub type Result<T> = std::result::Result<T, Error>;

#[derive(Debug)]
pub enum Error {}

impl std::error::Error for Error {}

impl std::fmt::Display for Error {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        todo!()
    }
}

impl From<array::TryFromSliceError> for Error {
    fn from(value: array::TryFromSliceError) -> Self {
        todo!()
    }
}

#[async_trait]
pub trait Storage: Clone {
    async fn put(&self, k: &[u8], v: &[u8]) -> Result<()>;

    async fn get(&self, k: &[u8]) -> Result<Option<Vec<u8>>>;

    async fn del(&self, k: &[u8]) -> Result<()>;

    async fn get_u64(&self, key: &[u8]) -> Result<Option<u64>> {
        Ok(self
            .get(key)
            .await?
            .map(|b| b.as_slice().try_into())
            .transpose()?
            .map(u64::from_be_bytes))
    }
}
