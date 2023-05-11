use bytes::BufMut;

use crate::subscription::{InitialPostion, SubType};

use super::{get_u64, get_u8, read_string, write_string, Codec, Header, PacketType, Result};

/// Each consumer corresponds to a subscription
#[derive(Debug, PartialEq, Clone)]
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

impl Codec for Subscribe {
    fn decode(mut buf: bytes::Bytes) -> Result<Self> {
        let request_id = get_u64(&mut buf)?;
        let consumer_id = get_u64(&mut buf)?;
        let topic = read_string(&mut buf)?;
        let sub_name = read_string(&mut buf)?;
        let sub_type = get_u8(&mut buf)?.try_into()?;
        let initial_position = get_u8(&mut buf)?.try_into()?;

        Ok(Self {
            request_id,
            consumer_id,
            topic,
            sub_name,
            sub_type,
            initial_position,
        })
    }

    fn encode(&self, buf: &mut bytes::BytesMut) -> Result<()> {
        buf.put_u64(self.request_id);
        buf.put_u64(self.consumer_id);
        write_string(buf, &self.topic);
        write_string(buf, &self.sub_name);
        buf.put_u8(self.sub_type as u8);
        buf.put_u8(self.initial_position as u8);
        Ok(())
    }

    fn header(&self) -> Header {
        Header::new(
            PacketType::Subscribe,
            8 + 8 + self.topic.len() + 2 + self.sub_name.len() + 2 + 1 + 1,
        )
    }
}
