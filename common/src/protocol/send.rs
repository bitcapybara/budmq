use bytes::Bytes;
use chrono::{DateTime, Utc};

use crate::types::MessageId;

#[derive(Debug, PartialEq, Clone, bud_derive::PacketCodec)]
pub struct Send {
    pub message_id: MessageId,
    pub consumer_id: u64,
    pub payload: Bytes,
    pub produce_time: DateTime<Utc>,
    pub send_time: DateTime<Utc>,
}
