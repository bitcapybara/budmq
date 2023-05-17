use crate::subscription::{InitialPostion, SubType};

/// Each consumer corresponds to a subscription
#[derive(Debug, PartialEq, Clone, bud_derive::Codec)]
pub struct Subscribe {
    pub request_id: u64,
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
}

#[derive(Debug, PartialEq, Clone, bud_derive::Codec)]
pub struct CloseConsumer {
    pub consumer_id: u64,
}
