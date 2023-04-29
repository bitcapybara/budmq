use std::fmt::Display;

use bytes::BufMut;

use super::{get_u8, Codec, Error, Header, PacketType, Result};

#[derive(Debug, Clone, Copy)]
#[repr(u8)]
pub enum ReturnCode {
    Success = 0,
    AlreadyConnected = 1,
    SubOnExlusive = 2,
    UnexpectedSubType = 3,
    ConsumerDuplicated = 4,
    NotConnected = 5,
    TopicNotExists = 6,
    ConsumerNotFound = 7,
    ProduceMessageDuplicated = 8,
    ConsumeMessageDuplicated = 9,
}

impl Display for ReturnCode {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ReturnCode::Success => write!(f, "Success"),
            ReturnCode::AlreadyConnected => write!(f, "AlreadyConnected"),
            ReturnCode::SubOnExlusive => write!(f, "SubOnExclusive"),
            ReturnCode::UnexpectedSubType => write!(f, "UnexpectedSubType"),
            ReturnCode::ConsumerDuplicated => write!(f, "ConsumerDuplicated"),
            ReturnCode::NotConnected => write!(f, "NotConnected"),
            ReturnCode::TopicNotExists => write!(f, "TopicNotExists"),
            ReturnCode::ConsumerNotFound => write!(f, "ConsumerNotFound"),
            ReturnCode::ProduceMessageDuplicated => write!(f, "ProduceMessageDuplicated"),
            ReturnCode::ConsumeMessageDuplicated => write!(f, "ConsumeMessageDuplicated"),
        }
    }
}

impl TryFrom<u8> for ReturnCode {
    type Error = Error;

    fn try_from(value: u8) -> Result<Self> {
        Ok(match value {
            0 => Self::Success,
            1 => Self::AlreadyConnected,
            2 => Self::SubOnExlusive,
            3 => Self::UnexpectedSubType,
            4 => Self::ConsumerDuplicated,
            5 => Self::NotConnected,
            6 => Self::TopicNotExists,
            7 => Self::ConsumerNotFound,
            8 => Self::ProduceMessageDuplicated,
            9 => Self::ConsumeMessageDuplicated,
            _ => return Err(Error::UnsupportedReturnCode),
        })
    }
}

impl Codec for ReturnCode {
    fn decode(mut buf: bytes::Bytes) -> Result<Self> {
        get_u8(&mut buf)?.try_into()
    }

    fn encode(&self, buf: &mut bytes::BytesMut) -> Result<()> {
        buf.put_u8(*self as u8);
        Ok(())
    }

    fn header(&self) -> Header {
        Header::new(PacketType::Response, 1)
    }
}
