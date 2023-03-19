use std::fmt::Display;

use super::{Codec, Result};

pub type ReturnCodeResult = std::result::Result<(), ReturnCode>;

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
}

impl Display for ReturnCode {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        todo!()
    }
}

impl Codec for ReturnCode {
    fn decode(buf: &mut bytes::BytesMut) -> Result<Self> {
        todo!()
    }

    fn encode(self, buf: &mut bytes::BytesMut) -> Result<()> {
        todo!()
    }
}
