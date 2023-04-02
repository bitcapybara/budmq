mod ack;
mod connect;
mod consume_ack;
mod control_flow;
mod publish;
mod send;
mod subscribe;
mod unsubscribe;

use std::{fmt::Display, io};

use bytes::BytesMut;
use tokio_util::codec::{Decoder, Encoder};

pub use self::{
    ack::ReturnCode, connect::Connect, consume_ack::ConsumeAck, control_flow::ControlFlow,
    publish::Publish, send::Send, subscribe::Subscribe, unsubscribe::Unsubscribe,
};

pub(in crate::protocol) type Result<T> = std::result::Result<T, Error>;

#[derive(Debug)]
pub enum Error {
    Io(io::Error),
}

impl std::error::Error for Error {}

impl Display for Error {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Error::Io(e) => write!(f, "I/O error: {e}"),
        }
    }
}

impl From<io::Error> for Error {
    fn from(e: io::Error) -> Self {
        Self::Io(e)
    }
}

pub struct PacketCodec;

impl Decoder for PacketCodec {
    type Item = Packet;

    type Error = Error;

    fn decode(&mut self, src: &mut BytesMut) -> Result<Option<Self::Item>> {
        todo!()
    }
}

impl Encoder<Packet> for PacketCodec {
    type Error = Error;

    fn encode(&mut self, item: Packet, dst: &mut bytes::BytesMut) -> Result<()> {
        todo!()
    }
}

pub trait Codec {
    fn decode(buf: &mut BytesMut) -> Result<Self>
    where
        Self: Sized;
    fn encode(self, buf: &mut BytesMut) -> Result<()>;
}

pub enum PacketType {
    Connect,
    Subscribe,
    Unsubscribe,
    Publish,
    Send,
    ConsumeAck,
    ControlFlow,
    Ack,
    Ping,
    Pong,
    Disconnect,
}
pub enum Packet {
    Connect(Connect),
    Subscribe(Subscribe),
    Unsubscribe(Unsubscribe),
    Publish(Publish),
    Send(Send),
    ConsumeAck(ConsumeAck),
    ControlFlow(ControlFlow),
    ReturnCode(ReturnCode),
    Ping,
    Pong,
    Disconnect,
}

struct Header {
    packet_type: PacketType,
    remain_len: usize,
}
