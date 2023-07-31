use crate::types::{InitialPostion, SubType};

/// Each consumer corresponds to a subscription
#[derive(Debug, PartialEq, Clone, bud_derive::PacketCodec)]
pub struct Subscribe {
    /// consumer name
    pub consumer_name: String,
    /// consumer_id, unique within one connection
    pub consumer_id: u64,
    /// subscribe topic
    pub topic: String,
    /// subscription id
    pub sub_name: String,
    /// subscribe type
    pub sub_type: SubType,
    /// consume init position
    pub initial_position: InitialPostion,
    /// default permits
    pub default_permits: u32,
    /// weight
    pub weight: Option<u8>,
}

#[derive(Debug, PartialEq, Clone, bud_derive::PacketCodec)]
pub struct CloseConsumer {
    pub consumer_id: u64,
}
