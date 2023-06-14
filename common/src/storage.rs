use async_trait::async_trait;

use crate::types::{MessageId, TopicMessage};

#[cfg(feature = "bonsaidb")]
pub mod bonsaidb;
pub mod memory;
#[cfg(feature = "mongodb")]
pub mod mongodb;
#[cfg(feature = "redis")]
pub mod redis;

#[async_trait]
pub trait MetaStorage: Clone + Send + Sync + 'static {
    type Error: std::error::Error;

    async fn put(&self, k: &str, v: &[u8]) -> Result<(), Self::Error>;

    async fn get(&self, k: &str) -> Result<Option<Vec<u8>>, Self::Error>;

    async fn del(&self, k: &str) -> Result<(), Self::Error>;

    async fn get_u64(&self, k: &str) -> Result<Option<u64>, Self::Error>;

    async fn set_u64(&self, k: &str, v: u64) -> Result<(), Self::Error>;

    async fn inc_u64(&self, k: &str, v: u64) -> Result<u64, Self::Error>;
}

#[async_trait]
pub trait MessageStorage: Clone + Send + Sync + 'static {
    type Error: std::error::Error;

    async fn put_message(&self, msg: &TopicMessage) -> Result<(), Self::Error>;

    async fn get_message(&self, id: &MessageId) -> Result<Option<TopicMessage>, Self::Error>;

    async fn del_message(&self, id: &MessageId) -> Result<(), Self::Error>;
}
