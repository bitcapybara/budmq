mod connect;
mod consume_ack;
mod control_flow;
mod ping_pong;
mod producer;
mod publish;
mod response;
mod send;
mod subscribe;
mod unsubscribe;

use std::{io, slice::Iter, string};

use bytes::{BufMut, BytesMut};
use tokio_util::codec::{Decoder, Encoder};

use crate::codec::Codec;

pub use self::{
    connect::Connect,
    consume_ack::ConsumeAck,
    control_flow::ControlFlow,
    producer::{CloseProducer, CreateProducer, ProducerReceipt},
    publish::Publish,
    response::Response,
    response::ReturnCode,
    send::Send,
    subscribe::{CloseConsumer, Subscribe},
    unsubscribe::Unsubscribe,
};

pub type Result<T> = std::result::Result<T, Error>;

#[derive(Debug)]
pub enum Error {
    Io(io::Error),
    InsufficientBytes,
    MalformedPacket,
    MalformedString(string::FromUtf8Error),
    UnsupportedInitPosition,
    UnsupportedSubType,
    UnsupportedReturnCode,
    UnsupportedAccessMode,
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
            Error::UnsupportedAccessMode => write!(f, "Unsupported access mode"),
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

impl From<crate::codec::Error> for Error {
    fn from(_: crate::codec::Error) -> Self {
        todo!()
    }
}

pub struct PacketCodec;

impl Decoder for PacketCodec {
    type Item = Packet;

    type Error = Error;

    fn decode(&mut self, src: &mut BytesMut) -> Result<Option<Self::Item>> {
        let (header, header_len) = Header::read(src.iter())?;
        // header + body + other
        let mut bytes = src
            .split_to(header.remain_len + header_len) // header + body
            .split_off(header_len) // body
            .freeze();
        Ok(Some(match header.packet_type()? {
            PacketType::Connect => Packet::Connect(Connect::decode(&mut bytes)?),
            PacketType::Subscribe => Packet::Subscribe(Subscribe::decode(&mut bytes)?),
            PacketType::Unsubscribe => Packet::Unsubscribe(Unsubscribe::decode(&mut bytes)?),
            PacketType::Publish => Packet::Publish(Publish::decode(&mut bytes)?),
            PacketType::Send => Packet::Send(Send::decode(&mut bytes)?),
            PacketType::ConsumeAck => Packet::ConsumeAck(ConsumeAck::decode(&mut bytes)?),
            PacketType::ControlFlow => Packet::ControlFlow(ControlFlow::decode(&mut bytes)?),
            PacketType::Response => Packet::Response(Response::decode(&mut bytes)?),
            PacketType::CreateProducer => {
                Packet::CreateProducer(CreateProducer::decode(&mut bytes)?)
            }
            PacketType::ProducerReceipt => {
                Packet::ProducerReceipt(ProducerReceipt::decode(&mut bytes)?)
            }
            PacketType::Ping => Packet::Ping,
            PacketType::Pong => Packet::Pong,
            PacketType::Disconnect => Packet::Disconnect,
            PacketType::CloseProducer => Packet::CloseProducer(CloseProducer::decode(&mut bytes)?),
            PacketType::CloseConsumer => Packet::CloseConsumer(CloseConsumer::decode(&mut bytes)?),
        }))
    }
}

impl Encoder<Packet> for PacketCodec {
    type Error = Error;

    fn encode(&mut self, packet: Packet, dst: &mut bytes::BytesMut) -> Result<()> {
        packet.header().write(dst)?;
        packet.write(dst);
        Ok(())
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
    CreateProducer,
    ProducerReceipt,
    Ping,
    Pong,
    Disconnect,
    CloseProducer,
    CloseConsumer,
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
            PacketType::CreateProducer => "PRODUCER",
            PacketType::ProducerReceipt => "PRODUCER_RECEIPT",
            PacketType::Ping => "PING",
            PacketType::Pong => "PONG",
            PacketType::Disconnect => "DISCONNECT",
            PacketType::CloseProducer => "CLOSE_PRODUCER",
            PacketType::CloseConsumer => "CLOSE_CONSUMER",
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
    CreateProducer(CreateProducer),
    ProducerReceipt(ProducerReceipt),
    Ping,
    Pong,
    Disconnect,
    CloseProducer(CloseProducer),
    CloseConsumer(CloseConsumer),
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
            Packet::CreateProducer(p) => p.header(),
            Packet::ProducerReceipt(p) => p.header(),
            Packet::Ping => Header::new(PacketType::Ping, 0),
            Packet::Pong => Header::new(PacketType::Pong, 0),
            Packet::Disconnect => Header::new(PacketType::Disconnect, 0),
            Packet::CloseProducer(p) => p.header(),
            Packet::CloseConsumer(p) => p.header(),
        }
    }

    fn write(&self, buf: &mut BytesMut) {
        match self {
            Packet::Connect(p) => p.encode(buf),
            Packet::Subscribe(p) => p.encode(buf),
            Packet::Unsubscribe(p) => p.encode(buf),
            Packet::Publish(p) => p.encode(buf),
            Packet::Send(p) => p.encode(buf),
            Packet::ConsumeAck(p) => p.encode(buf),
            Packet::ControlFlow(p) => p.encode(buf),
            Packet::Response(p) => p.encode(buf),
            Packet::CreateProducer(p) => p.encode(buf),
            Packet::ProducerReceipt(p) => p.encode(buf),
            Packet::Ping => {}
            Packet::Pong => {}
            Packet::Disconnect => {}
            Packet::CloseProducer(p) => p.encode(buf),
            Packet::CloseConsumer(p) => p.encode(buf),
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
            Packet::CreateProducer(_) => PacketType::CreateProducer,
            Packet::ProducerReceipt(_) => PacketType::ProducerReceipt,
            Packet::Ping => PacketType::Ping,
            Packet::Pong => PacketType::Pong,
            Packet::Disconnect => PacketType::Disconnect,
            Packet::CloseProducer(_) => PacketType::CloseProducer,
            Packet::CloseConsumer(_) => PacketType::CloseConsumer,
        }
    }

    pub fn ok_response() -> Self {
        Self::Response(Response {
            code: ReturnCode::Success,
        })
    }

    pub fn err_response(code: ReturnCode) -> Self {
        Self::Response(Response { code })
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
            9 => PacketType::CreateProducer,
            10 => PacketType::ProducerReceipt,
            11 => PacketType::Ping,
            12 => PacketType::Pong,
            13 => PacketType::Disconnect,
            14 => PacketType::CloseProducer,
            15 => PacketType::CloseConsumer,
            _ => return Err(Error::MalformedPacket),
        })
    }
}

#[cfg(test)]
mod tests {
    use bytes::Bytes;
    use chrono::Utc;
    use tokio_util::codec::Encoder;

    use crate::{
        codec::{read_string, write_string},
        types::{AccessMode, InitialPostion, MessageId, SubType},
    };

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
        codec_works(Packet::Connect(Connect { keepalive: 10000 }))
    }

    #[test]
    fn codec_subscribe() {
        codec_works(Packet::Subscribe(Subscribe {
            consumer_name: "test-consumer".to_string(),
            consumer_id: 1,
            topic: "test-topic".to_string(),
            sub_name: "test-subname".to_string(),
            sub_type: SubType::Exclusive,
            initial_position: InitialPostion::Latest,
        }))
    }

    #[test]
    fn codec_unsubscribe() {
        codec_works(Packet::Unsubscribe(Unsubscribe { consumer_id: 1 }))
    }

    #[test]
    fn codec_publish() {
        codec_works(Packet::Publish(Publish {
            producer_id: 1,
            topic: "test-topic".to_string(),
            sequence_id: 200,
            payload: Bytes::from_static(b"hello, world"),
            produce_time: Utc::now(),
        }))
    }

    #[test]
    fn codec_send() {
        codec_works(Packet::Send(Send {
            message_id: MessageId {
                topic_id: 1,
                cursor_id: 200,
            },
            consumer_id: 3456,
            payload: Bytes::from_static(b"hello, world"),
            produce_time: Utc::now(),
            send_time: Utc::now(),
        }))
    }

    #[test]
    fn codec_consume_ack() {
        codec_works(Packet::ConsumeAck(ConsumeAck {
            consumer_id: 12345,
            message_id: MessageId {
                topic_id: 1,
                cursor_id: 3,
            },
        }))
    }

    #[test]
    fn codec_control_flow() {
        codec_works(Packet::ControlFlow(ControlFlow {
            consumer_id: 123,
            permits: 500,
        }))
    }

    #[test]
    fn codec_response() {
        codec_works(Packet::Response(Response {
            code: ReturnCode::Success,
        }))
    }

    #[test]
    fn codec_create_producer() {
        codec_works(Packet::CreateProducer(CreateProducer {
            producer_name: "producer-id".to_string(),
            producer_id: 555,
            topic_name: "test-topic".to_string(),
            access_mode: AccessMode::Exclusive,
        }))
    }

    #[test]
    fn codec_producer_receipt() {
        codec_works(Packet::ProducerReceipt(ProducerReceipt {
            producer_id: 9,
            sequence_id: 44,
        }))
    }

    #[test]
    fn codec_ping() {
        codec_works(Packet::Ping)
    }

    #[test]
    fn codec_pong() {
        codec_works(Packet::Pong)
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

    #[test]
    fn codec_close_producer() {
        codec_works(Packet::CloseProducer(CloseProducer { producer_id: 22 }))
    }

    #[test]
    fn codec_close_consumer() {
        codec_works(Packet::CloseConsumer(CloseConsumer { consumer_id: 55 }))
    }

    fn codec_works(packet: Packet) {
        let mut bytes = BytesMut::new();
        PacketCodec.encode(packet.clone(), &mut bytes).unwrap();
        println!("{:?}", bytes.as_ref());
        assert_eq!(packet, PacketCodec.decode(&mut bytes).unwrap().unwrap());
    }
}
