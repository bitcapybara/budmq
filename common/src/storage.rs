use std::net::SocketAddr;

use async_trait::async_trait;

use crate::types::{MessageId, SubscriptionInfo, TopicMessage};

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

    async fn register_topic(&self, topic_name: &str, broker_id: &str) -> Result<(), Self::Error>;

    async fn get_topic_owner(&self, topic_name: &str) -> Result<Option<SocketAddr>, Self::Error>;

    async fn add_subscription(&self, info: &SubscriptionInfo) -> Result<(), Self::Error>;

    async fn all_subscription(
        &self,
        topic_name: &str,
    ) -> Result<Vec<SubscriptionInfo>, Self::Error>;

    async fn del_subscription(&self, topic_name: &str, name: &str) -> Result<(), Self::Error>;

    async fn get_u64(&self, k: &str) -> Result<Option<u64>, Self::Error>;

    async fn put_u64(&self, k: &str, v: u64) -> Result<(), Self::Error>;

    async fn inc_u64(&self, k: &str, v: u64) -> Result<u64, Self::Error>;
}

#[async_trait]
pub trait MessageStorage: Clone + Send + Sync + 'static {
    type Error: std::error::Error;

    async fn put_message(&self, msg: &TopicMessage) -> Result<(), Self::Error>;

    async fn get_message(&self, id: &MessageId) -> Result<Option<TopicMessage>, Self::Error>;

    async fn del_message(&self, id: &MessageId) -> Result<(), Self::Error>;

    async fn save_cursor(
        &self,
        topic_name: &str,
        sub_name: &str,
        bytes: &[u8],
    ) -> Result<(), Self::Error>;

    async fn load_cursor(
        &self,
        topic_name: &str,
        sub_name: &str,
    ) -> Result<Option<Vec<u8>>, Self::Error>;
}
