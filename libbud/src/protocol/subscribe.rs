use bytes::{Buf, BufMut};

use crate::subscription::{InitialPostion, SubType};

use super::{assert_len, read_string, write_string, Codec, Header, PacketType, Result};

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
    /// consume init position
    pub initial_position: InitialPostion,
}

impl Codec for Subscribe {
    fn decode(mut buf: bytes::Bytes) -> Result<Self> {
        assert_len(&buf, 8)?;
        let consumer_id = buf.get_u64();

        let topic = read_string(&mut buf)?;
        let sub_name = read_string(&mut buf)?;

        assert_len(&buf, 1)?;
        let sub_type = buf.get_u8().try_into()?;

        assert_len(&buf, 1)?;
        let initial_position = buf.get_u8().try_into()?;

        Ok(Self {
            consumer_id,
            topic,
            sub_name,
            sub_type,
            initial_position,
        })
    }

    fn encode(&self, buf: &mut bytes::BytesMut) -> Result<()> {
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
            8 + self.topic.len() + self.sub_name.len() + 1 + 1,
        )
    }
}
