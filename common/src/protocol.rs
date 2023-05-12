mod connect;
mod consume_ack;
mod control_flow;
mod producer;
mod producer_receipt;
mod publish;
mod response;
mod send;
mod subscribe;
mod unsubscribe;

use std::{io, slice::Iter, string};

use bytes::{Buf, BufMut, Bytes, BytesMut};
use tokio_util::codec::{Decoder, Encoder};

use crate::subscription::{InitialPostion, SubType};

pub use self::{
    connect::Connect, consume_ack::ConsumeAck, control_flow::ControlFlow, producer::Producer,
    producer_receipt::ProducerReceipt, publish::Publish, response::Response, response::ReturnCode,
    send::Send, subscribe::Subscribe, unsubscribe::Unsubscribe,
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

impl std::fmt::Display for Error {
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
            PacketType::Response => Packet::Response(Response::decode(bytes)?),
            PacketType::Producer => Packet::Producer(Producer::decode(bytes)?),
            PacketType::ProducerReceipt => Packet::ProducerReceipt(ProducerReceipt::decode(bytes)?),
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

#[derive(Debug)]
#[repr(u8)]
pub enum PacketType {
    Connect = 1,
    Subscribe,
    Unsubscribe,
    Publish,
    Send,
    ConsumeAck,
    ControlFlow,
    Response,
    Producer,
    ProducerReceipt,
    Ping,
    Pong,
    Disconnect,
}

impl std::fmt::Display for PacketType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let s = match self {
            PacketType::Connect => "CONNECT",
            PacketType::Subscribe => "SUBSCRIBE",
            PacketType::Unsubscribe => "UNSUBSCRIBE",
            PacketType::Publish => "PUBLISH",
            PacketType::Send => "SEND",
            PacketType::ConsumeAck => "CONSUMEACK",
            PacketType::ControlFlow => "CONTROLFLOW",
            PacketType::Response => "RESPONSE",
            PacketType::Producer => "PRODUCER",
            PacketType::ProducerReceipt => "PRODUCER_RECEIPT",
            PacketType::Ping => "PING",
            PacketType::Pong => "PONG",
            PacketType::Disconnect => "DISCONNECT",
        };
        write!(f, "{s}")
    }
}

#[derive(Debug, PartialEq, Clone)]
pub enum Packet {
    Connect(Connect),
    Subscribe(Subscribe),
    Unsubscribe(Unsubscribe),
    Publish(Publish),
    Send(Send),
    ConsumeAck(ConsumeAck),
    ControlFlow(ControlFlow),
    Response(Response),
    Producer(Producer),
    ProducerReceipt(ProducerReceipt),
    Ping,
    Pong,
    Disconnect,
}

impl Packet {
    fn header(&self) -> Header {
        match self {
            Packet::Connect(p) => p.header(),
            Packet::Subscribe(p) => p.header(),
            Packet::Unsubscribe(p) => p.header(),
            Packet::Publish(p) => p.header(),
            Packet::Send(p) => p.header(),
            Packet::ConsumeAck(p) => p.header(),
            Packet::ControlFlow(p) => p.header(),
            Packet::Response(p) => p.header(),
            Packet::Producer(p) => p.header(),
            Packet::ProducerReceipt(p) => p.header(),
            Packet::Ping => Header::new(PacketType::Ping, 0),
            Packet::Pong => Header::new(PacketType::Pong, 0),
            Packet::Disconnect => Header::new(PacketType::Disconnect, 0),
        }
    }

    fn write(&self, buf: &mut BytesMut) -> Result<()> {
        match self {
            Packet::Connect(p) => p.encode(buf),
            Packet::Subscribe(p) => p.encode(buf),
            Packet::Unsubscribe(p) => p.encode(buf),
            Packet::Publish(p) => p.encode(buf),
            Packet::Send(p) => p.encode(buf),
            Packet::ConsumeAck(p) => p.encode(buf),
            Packet::ControlFlow(p) => p.encode(buf),
            Packet::Response(p) => p.encode(buf),
            Packet::Producer(p) => p.encode(buf),
            Packet::ProducerReceipt(p) => p.encode(buf),
            Packet::Ping => Ok(()),
            Packet::Pong => Ok(()),
            Packet::Disconnect => Ok(()),
        }
    }

    pub fn packet_type(&self) -> PacketType {
        match self {
            Packet::Connect(_) => PacketType::Connect,
            Packet::Subscribe(_) => PacketType::Subscribe,
            Packet::Unsubscribe(_) => PacketType::Unsubscribe,
            Packet::Publish(_) => PacketType::Publish,
            Packet::Send(_) => PacketType::Send,
            Packet::ConsumeAck(_) => PacketType::ConsumeAck,
            Packet::ControlFlow(_) => PacketType::ControlFlow,
            Packet::Response(_) => PacketType::Response,
            Packet::Producer(_) => PacketType::Producer,
            Packet::ProducerReceipt(_) => PacketType::ProducerReceipt,
            Packet::Ping => PacketType::Ping,
            Packet::Pong => PacketType::Pong,
            Packet::Disconnect => PacketType::Disconnect,
        }
    }

    pub fn request_id(&self) -> Option<u64> {
        match self {
            Packet::Connect(p) => Some(p.request_id),
            Packet::Subscribe(p) => Some(p.request_id),
            Packet::Unsubscribe(p) => Some(p.request_id),
            Packet::Publish(p) => Some(p.request_id),
            Packet::Send(p) => Some(p.request_id),
            Packet::ConsumeAck(p) => Some(p.request_id),
            Packet::ControlFlow(p) => Some(p.request_id),
            Packet::Response(p) => Some(p.request_id),
            Packet::Producer(p) => Some(p.request_id),
            Packet::ProducerReceipt(p) => Some(p.request_id),
            Packet::Ping => None,
            Packet::Pong => None,
            Packet::Disconnect => None,
        }
    }
}

#[derive(Debug, PartialEq)]
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
            8 => PacketType::Response,
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

pub fn read_bytes(buf: &mut Bytes) -> Result<Bytes> {
    let len = get_u16(buf)? as usize;
    assert_len(buf, len)?;
    Ok(buf.split_to(len))
}

pub fn read_string(buf: &mut Bytes) -> Result<String> {
    let bytes = read_bytes(buf)?;
    Ok(String::from_utf8(bytes.to_vec())?)
}

pub fn write_bytes(buf: &mut BytesMut, bytes: &[u8]) {
    buf.put_u16(bytes.len() as u16);
    buf.extend_from_slice(bytes);
}

pub fn write_string(buf: &mut BytesMut, string: &str) {
    write_bytes(buf, string.as_bytes());
}

pub fn get_u8(buf: &mut Bytes) -> Result<u8> {
    assert_len(buf, 1)?;
    Ok(buf.get_u8())
}

pub fn get_u16(buf: &mut Bytes) -> Result<u16> {
    assert_len(buf, 2)?;
    Ok(buf.get_u16())
}

pub fn get_u32(buf: &mut Bytes) -> Result<u32> {
    assert_len(buf, 4)?;
    Ok(buf.get_u32())
}

pub fn get_u64(buf: &mut Bytes) -> Result<u64> {
    assert_len(buf, 8)?;
    Ok(buf.get_u64())
}

#[cfg(test)]
mod tests {
    use tokio_util::codec::Encoder;

    use super::*;

    #[test]
    fn codec_header() {
        {
            let mut bytes = BytesMut::new();
            let header_write = Header::new(PacketType::Subscribe, 32);
            header_write.write(&mut bytes).unwrap();
            let (header_read, header_len) = Header::read(bytes.freeze().iter()).unwrap();
            assert_eq!(header_len, 2);
            assert_eq!(header_read.remain_len, 32);
            assert_eq!(header_write, header_read);
        }
        {
            let mut bytes = BytesMut::new();
            let header_write = Header::new(PacketType::Subscribe, 352);
            header_write.write(&mut bytes).unwrap();
            let (header_read, header_len) = Header::read(bytes.freeze().iter()).unwrap();
            assert_eq!(header_len, 3);
            assert_eq!(header_read.remain_len, 352);
            assert_eq!(header_write, header_read);
        }
        {
            let mut bytes = BytesMut::new();
            let header_write = Header::new(PacketType::Subscribe, 15352);
            header_write.write(&mut bytes).unwrap();
            let (header_read, header_len) = Header::read(bytes.freeze().iter()).unwrap();
            assert_eq!(header_len, 3);
            assert_eq!(header_read.remain_len, 15352);
            assert_eq!(header_write, header_read);
        }
    }

    #[test]
    fn codec_string() {
        let test_string = "test_string";
        let mut bytes = BytesMut::new();
        write_string(&mut bytes, test_string);
        assert_eq!(bytes.len(), 11 + 2);
        assert_eq!(test_string, read_string(&mut bytes.freeze()).unwrap());
    }

    #[test]
    fn codec_connect() {
        codec_works(Packet::Connect(Connect {
            request_id: 1,
            keepalive: 10000,
        }))
    }

    #[test]
    fn codec_subscribe() {
        codec_works(Packet::Subscribe(Subscribe {
            request_id: 1,
            consumer_id: 1,
            topic: "test-topic".to_string(),
            sub_name: "test-subname".to_string(),
            sub_type: SubType::Exclusive,
            initial_position: InitialPostion::Latest,
        }))
    }

    #[test]
    fn codec_unsubscribe() {
        codec_works(Packet::Unsubscribe(Unsubscribe {
            request_id: 1,
            consumer_id: 1,
        }))
    }

    #[test]
    fn codec_publish() {
        codec_works(Packet::Publish(Publish {
            request_id: 1,
            topic: "test-topic".to_string(),
            sequence_id: 200,
            payload: Bytes::from_static(b"hello, world"),
        }))
    }

    #[test]
    fn codec_send() {
        codec_works(Packet::Send(Send {
            request_id: 1,
            message_id: 1234,
            consumer_id: 3456,
            payload: Bytes::from_static(b"hello, world"),
        }))
    }

    #[test]
    fn codec_consume_ack() {
        codec_works(Packet::ConsumeAck(ConsumeAck {
            request_id: 1,
            consumer_id: 12345,
            message_id: 23456,
        }))
    }

    #[test]
    fn codec_control_flow() {
        codec_works(Packet::ControlFlow(ControlFlow {
            request_id: 1,
            consumer_id: 123,
            permits: 500,
        }))
    }

    #[test]
    fn codec_response() {
        codec_works(Packet::Response(Response {
            request_id: 1,
            code: ReturnCode::Success,
        }))
    }

    #[test]
    fn codec_ping_pong() {
        codec_works(Packet::Ping);
        codec_works(Packet::Pong);
    }

    #[test]
    fn codec_disconnect() {
        codec_works(Packet::Disconnect)
    }

    fn codec_works(packet: Packet) {
        let mut bytes = BytesMut::new();
        PacketCodec.encode(packet.clone(), &mut bytes).unwrap();
        assert_eq!(packet, PacketCodec.decode(&mut bytes).unwrap().unwrap());
    }
}
