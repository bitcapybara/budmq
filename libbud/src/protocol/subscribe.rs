use crate::subscription::SubType;

use super::{Codec, Result};

/// Each consumer corresponds to a subscription
pub struct Subscribe {
    /// consumer_id, unique within one connection
    pub consumer_id: u64,
    /// subscribe topic
    pub topic: String,
    /// subscription id
    pub sub_name: String,
    /// subscribe type
    pub sub_type: SubType,
}

impl Codec for Subscribe {
    fn decode(buf: &mut bytes::BytesMut) -> Result<Self> {
        todo!()
    }

    fn encode(self, buf: &mut bytes::BytesMut) -> Result<()> {
        todo!()
    }
}
