use std::{fmt::Display, io};

use tokio_util::codec::{Decoder, Encoder};

type Result<T> = std::result::Result<T, Error>;

#[derive(Debug)]
pub enum Error {}

impl std::error::Error for Error {}

impl Display for Error {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        todo!()
    }
}

impl From<io::Error> for Error {
    fn from(value: io::Error) -> Self {
        todo!()
    }
}

pub struct Codec;

impl Decoder for Codec {
    type Item = Packet;

    type Error = Error;

    fn decode(&mut self, src: &mut bytes::BytesMut) -> Result<Option<Self::Item>> {
        todo!()
    }
}

impl Encoder<Packet> for Codec {
    type Error = Error;

    fn encode(&mut self, item: Packet, dst: &mut bytes::BytesMut) -> Result<()> {
        todo!()
    }
}

pub enum PacketType {
    Connect,
    Subscribe,
    Unsubscribe,
    Publish,
    Ack,
    ControlFlow,
    Disconnect,
}
pub enum Packet {
    Connect,
    Disconnect,
    Fail(String),
    Success,
}

impl Packet {
    pub fn fail(msg: &str) -> Self {
        Packet::Fail(msg.to_string())
    }
}

struct Header {
    packet_type: PacketType,
    remain_len: usize,
}
