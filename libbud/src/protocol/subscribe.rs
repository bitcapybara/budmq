use crate::subscription::SubType;

use super::{Codec, Result};

pub struct Subscribe {
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
