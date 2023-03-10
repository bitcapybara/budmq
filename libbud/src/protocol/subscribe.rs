use crate::subscription::SubType;

use super::{Codec, Result};

pub struct Subscribe {
    /// consumer_id
    pub consumer_id: u64,
    /// subscribe topic
    pub topic: String,
    /// subscription id
    pub sub_id: String,
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
