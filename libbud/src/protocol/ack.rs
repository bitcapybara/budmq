use std::fmt::Display;

use super::{Codec, Result};

#[derive(Debug, Clone, Copy)]
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

impl Codec for ReturnCode {
    fn decode(buf: bytes::Bytes) -> Result<Self> {
        todo!()
    }

    fn encode(&self, buf: &mut bytes::BytesMut) -> Result<()> {
        todo!()
    }
}
