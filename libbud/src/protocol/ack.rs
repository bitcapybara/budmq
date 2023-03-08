use std::fmt::Display;

use super::{Codec, Result};

#[derive(Debug, Clone, Copy)]
pub enum ReturnCode {
    Success = 0,
    AlreadyConnected = 1,
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
