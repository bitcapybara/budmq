use std::net::SocketAddr;

use bytes::Bytes;
use chrono::{DateTime, Utc};

#[derive(
    Debug, Clone, Copy, PartialEq, serde::Serialize, serde::Deserialize, bud_derive::Codec,
)]
#[repr(u8)]
pub enum SubType {
    /// Each subscription is only allowed to contain one client
    Exclusive = 1,
    /// Each subscription allows multiple clients
    Shared,
}

impl TryFrom<u8> for SubType {
    type Error = crate::codec::Error;

    fn try_from(value: u8) -> crate::codec::Result<Self> {
        Ok(match value {
            1 => Self::Exclusive,
            2 => Self::Shared,
            _ => return Err(Self::Error::Malformed),
        })
    }
}

impl Default for SubType {
    fn default() -> Self {
        Self::Shared
    }
}

#[derive(
    Debug, Clone, Copy, PartialEq, serde::Serialize, serde::Deserialize, bud_derive::Codec,
)]
#[repr(u8)]
pub enum InitialPostion {
    Latest = 1,
    Earliest,
}

impl TryFrom<u8> for InitialPostion {
    type Error = crate::codec::Error;

    fn try_from(value: u8) -> crate::codec::Result<Self> {
        Ok(match value {
            1 => Self::Latest,
            2 => Self::Earliest,
            _ => return Err(Self::Error::Malformed),
        })
    }
}

impl Default for InitialPostion {
    fn default() -> Self {
        Self::Latest
    }
}

#[derive(Debug, Clone, Copy, PartialEq, bud_derive::Codec)]
#[repr(u8)]
pub enum AccessMode {
    Exclusive = 1,
    Shared,
}

impl TryFrom<u8> for AccessMode {
    type Error = crate::codec::Error;

    fn try_from(value: u8) -> crate::codec::Result<Self> {
        Ok(match value {
            1 => Self::Exclusive,
            2 => Self::Shared,
            _ => return Err(Self::Error::Malformed),
        })
    }
}

impl Default for AccessMode {
    fn default() -> Self {
        Self::Shared
    }
}

#[derive(Debug, Clone, Copy, PartialOrd, PartialEq, bud_derive::Codec)]
pub struct MessageId {
    pub topic_id: u64,
    pub cursor_id: u64,
}

impl MessageId {
    pub fn new(topic_id: u64, cursor_id: u64) -> Self {
        Self {
            topic_id,
            cursor_id,
        }
    }
}

impl std::fmt::Display for MessageId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}::{}", self.topic_id, self.cursor_id)
    }
}

#[derive(Debug, Clone, bud_derive::Codec)]
pub struct TopicMessage {
    /// message id
    pub message_id: MessageId,
    /// topic name
    pub topic_name: String,
    /// producer sequence id
    pub sequence_id: u64,
    /// message payload
    pub payload: Bytes,
    /// produce time
    pub produce_time: DateTime<Utc>,
}

impl TopicMessage {
    pub fn new(
        topic: &str,
        message_id: MessageId,
        seq_id: u64,
        payload: Bytes,
        produce_time: DateTime<Utc>,
    ) -> Self {
        Self {
            topic_name: topic.to_string(),
            sequence_id: seq_id,
            payload,
            message_id,
            produce_time,
        }
    }
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize, bud_derive::Codec)]
pub struct SubscriptionInfo {
    pub topic: String,
    pub name: String,
    pub sub_type: SubType,
    pub init_position: InitialPostion,
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct BrokerAddress {
    pub socket_addr: SocketAddr,
    pub server_name: String,
}

impl std::fmt::Display for BrokerAddress {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{} -> {}", self.socket_addr, self.server_name)
    }
}
