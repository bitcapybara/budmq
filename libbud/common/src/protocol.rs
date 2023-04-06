mod ack;
mod connect;
mod consume_ack;
mod control_flow;
mod publish;
mod send;
mod subscribe;
mod unsubscribe;

use std::{fmt::Display, io, slice::Iter, string};

use bytes::{Buf, BufMut, Bytes, BytesMut};
use tokio_util::codec::{Decoder, Encoder};

use crate::subscription::{InitialPostion, SubType};

pub use self::{
    ack::ReturnCode, connect::Connect, consume_ack::ConsumeAck, control_flow::ControlFlow,
    publish::Publish, send::Send, subscribe::Subscribe, unsubscribe::Unsubscribe,
};

pub(in crate::protocol) type Result<T> = std::result::Result<T, Error>;

#[derive(Debug)]
pub enum Error {
    Io(io::Error),
    InsufficientBytes,
    MalformedPacket,
    MalformedString(string::FromUtf8Error),
    UnsupportedInitPosition,
    UnsupportedSubType,
    UnsupportedReturnCode,
}

impl std::error::Error for Error {}

impl Display for Error {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Error::Io(e) => write!(f, "I/O error: {e}"),
            Error::InsufficientBytes => write!(f, "Insufficient bytes"),
            Error::MalformedPacket => write!(f, "Malformed packet"),
            Error::MalformedString(e) => write!(f, "Malformed string: {e}"),
            Error::UnsupportedInitPosition => write!(f, "Unsupported initial postion"),
            Error::UnsupportedSubType => write!(f, "Unsupported subscribe type"),
            Error::UnsupportedReturnCode => write!(f, "Unsupported return code"),
        }
    }
}

impl From<io::Error> for Error {
    fn from(e: io::Error) -> Self {
        Self::Io(e)
    }
}

impl From<string::FromUtf8Error> for Error {
    fn from(e: string::FromUtf8Error) -> Self {
        Self::MalformedString(e)
    }
}

pub struct PacketCodec;

impl Decoder for PacketCodec {
    type Item = Packet;

    type Error = Error;

    fn decode(&mut self, src: &mut BytesMut) -> Result<Option<Self::Item>> {
        let (header, header_len) = Header::read(src.iter())?;
        // header + body + other
        let bytes = src
            .split_to(header.remain_len + header_len) // header + body
            .split_off(header_len) // body
            .freeze();
        Ok(Some(match header.packet_type()? {
            PacketType::Connect => Packet::Connect(Connect::decode(bytes)?),
            PacketType::Subscribe => Packet::Subscribe(Subscribe::decode(bytes)?),
            PacketType::Unsubscribe => Packet::Unsubscribe(Unsubscribe::decode(bytes)?),
            PacketType::Publish => Packet::Publish(Publish::decode(bytes)?),
            PacketType::Send => Packet::Send(Send::decode(bytes)?),
            PacketType::ConsumeAck => Packet::ConsumeAck(ConsumeAck::decode(bytes)?),
            PacketType::ControlFlow => Packet::ControlFlow(ControlFlow::decode(bytes)?),
            PacketType::ReturnCode => Packet::ConsumeAck(ConsumeAck::decode(bytes)?),
            PacketType::Ping => Packet::Ping,
            PacketType::Pong => Packet::Pong,
            PacketType::Disconnect => Packet::Disconnect,
        }))
    }
}

impl Encoder<Packet> for PacketCodec {
    type Error = Error;

    fn encode(&mut self, item: Packet, dst: &mut bytes::BytesMut) -> Result<()> {
        item.header().write(dst)?;
        item.write(dst)?;
        Ok(())
    }
}

pub trait Codec {
    fn decode(buf: Bytes) -> Result<Self>
    where
        Self: Sized;
    fn encode(&self, buf: &mut BytesMut) -> Result<()>;

    fn header(&self) -> Header;
}

impl TryFrom<u8> for InitialPostion {
    type Error = Error;

    fn try_from(value: u8) -> Result<Self> {
        Ok(match value {
            1 => Self::Latest,
            2 => Self::Earliest,
            _ => return Err(Error::UnsupportedInitPosition),
        })
    }
}

impl TryFrom<u8> for SubType {
    type Error = Error;

    fn try_from(value: u8) -> Result<Self> {
        Ok(match value {
            1 => Self::Exclusive,
            2 => Self::Shared,
            _ => return Err(Error::UnsupportedSubType),
        })
    }
}

#[repr(u8)]
pub enum PacketType {
    Connect = 1,
    Subscribe,
    Unsubscribe,
    Publish,
    Send,
    ConsumeAck,
    ControlFlow,
    ReturnCode,
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

impl Packet {
    fn header(&self) -> Header {
        match self {
            Packet::Connect(c) => c.header(),
            Packet::Subscribe(s) => s.header(),
            Packet::Unsubscribe(u) => u.header(),
            Packet::Publish(p) => p.header(),
            Packet::Send(s) => s.header(),
            Packet::ConsumeAck(c) => c.header(),
            Packet::ControlFlow(c) => c.header(),
            Packet::ReturnCode(r) => r.header(),
            Packet::Ping => Header::new(PacketType::Ping, 0),
            Packet::Pong => Header::new(PacketType::Pong, 0),
            Packet::Disconnect => Header::new(PacketType::Disconnect, 0),
        }
    }

    fn write(&self, buf: &mut BytesMut) -> Result<()> {
        match self {
            Packet::Connect(c) => c.encode(buf),
            Packet::Subscribe(s) => s.encode(buf),
            Packet::Unsubscribe(u) => u.encode(buf),
            Packet::Publish(p) => p.encode(buf),
            Packet::Send(s) => s.encode(buf),
            Packet::ConsumeAck(a) => a.encode(buf),
            Packet::ControlFlow(c) => c.encode(buf),
            Packet::ReturnCode(r) => r.encode(buf),
            Packet::Ping => Ok(()),
            Packet::Pong => Ok(()),
            Packet::Disconnect => Ok(()),
        }
    }
}

pub struct Header {
    /// 8 bits
    type_byte: u8,
    /// mqtt remain len algorithm
    remain_len: usize,
}

impl Header {
    fn new(packet_type: PacketType, remain_len: usize) -> Self {
        Self {
            type_byte: packet_type as u8,
            remain_len,
        }
    }
    fn read(mut buf: Iter<u8>) -> Result<(Self, usize)> {
        let type_byte = buf.next().ok_or(Error::InsufficientBytes)?.to_owned();

        let mut remain_len = 0usize;
        let mut header_len = 1; // init with type_byte bit
        let mut done = false;
        let mut shift = 0;

        for byte in buf.map(|b| *b as usize) {
            header_len += 1;
            remain_len += (byte & 0x7F) << shift;

            done = (byte & 0x80) == 0;
            if done {
                break;
            }
            shift += 7;

            if shift > 21 {
                return Err(Error::MalformedPacket);
            }
        }

        if !done {
            return Err(Error::InsufficientBytes);
        }

        Ok((
            Header {
                remain_len,
                type_byte,
            },
            header_len,
        ))
    }

    fn write(&self, buf: &mut BytesMut) -> Result<()> {
        buf.put_u8(self.type_byte);

        let mut done = false;
        let mut x = self.remain_len;

        while !done {
            let mut byte = (x % 128) as u8;
            x /= 128;
            if x > 0 {
                byte |= 128;
            }

            buf.put_u8(byte);
            done = x == 0;
        }

        Ok(())
    }

    fn packet_type(&self) -> Result<PacketType> {
        Ok(match self.type_byte {
            1 => PacketType::Connect,
            2 => PacketType::Subscribe,
            3 => PacketType::Unsubscribe,
            4 => PacketType::Publish,
            5 => PacketType::Send,
            6 => PacketType::ConsumeAck,
            7 => PacketType::ControlFlow,
            8 => PacketType::ReturnCode,
            9 => PacketType::Ping,
            10 => PacketType::Pong,
            11 => PacketType::Disconnect,
            _ => return Err(Error::MalformedPacket),
        })
    }
}

fn assert_len(buf: &Bytes, len: usize) -> Result<()> {
    if buf.len() < len {
        return Err(Error::InsufficientBytes);
    }

    Ok(())
}

fn read_bytes(buf: &mut Bytes) -> Result<Bytes> {
    assert_len(buf, 2)?;
    let len = buf.get_u16() as usize;
    assert_len(buf, len)?;
    Ok(buf.split_to(len))
}

fn read_string(buf: &mut Bytes) -> Result<String> {
    let bytes = read_bytes(buf)?;
    Ok(String::from_utf8(bytes.to_vec())?)
}

fn write_bytes(buf: &mut BytesMut, bytes: &[u8]) {
    buf.put_u16(bytes.len() as u16);
    buf.extend_from_slice(bytes);
}

fn write_string(buf: &mut BytesMut, string: &str) {
    write_bytes(buf, string.as_bytes());
}