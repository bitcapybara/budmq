use bytes::{Buf, BufMut, Bytes, BytesMut};
use chrono::{DateTime, NaiveDateTime, Utc};

pub type Result<T> = std::result::Result<T, Error>;

#[derive(Debug)]
pub enum Error {
    InsufficientBytes,
    Malformed,
}

impl std::error::Error for Error {}

impl std::fmt::Display for Error {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Error::InsufficientBytes => write!(f, "InsufficientBytes"),
            Error::Malformed => write!(f, "Malformed Packet"),
        }
    }
}

pub trait Codec {
    fn decode(buf: &mut Bytes) -> Result<Self>
    where
        Self: Sized;

    fn encode(&self, buf: &mut BytesMut);

    fn size(&self) -> usize;
}

impl Codec for DateTime<Utc> {
    fn decode(buf: &mut Bytes) -> Result<Self> {
        let timestmp = get_i64(buf)?;
        let (sec, nano) = (timestmp / 1_000_000_000, (timestmp % 1_000_000_000) as u32);
        let ndt = NaiveDateTime::from_timestamp_opt(sec, nano).ok_or(Error::Malformed)?;
        let dt = DateTime::<Utc>::from_utc(ndt, Utc);
        Ok(dt)
    }

    fn encode(&self, buf: &mut BytesMut) {
        let timestamp = self.timestamp_nanos();
        buf.put_i64(timestamp);
    }

    fn size(&self) -> usize {
        8
    }
}

impl Codec for u16 {
    fn decode(buf: &mut Bytes) -> Result<Self> {
        get_u16(buf)
    }

    fn encode(&self, buf: &mut BytesMut) {
        buf.put_u16(*self)
    }

    fn size(&self) -> usize {
        2
    }
}

impl Codec for u32 {
    fn decode(buf: &mut Bytes) -> Result<Self> {
        get_u32(buf)
    }

    fn encode(&self, buf: &mut BytesMut) {
        buf.put_u32(*self)
    }

    fn size(&self) -> usize {
        4
    }
}

impl Codec for u64 {
    fn decode(buf: &mut Bytes) -> Result<Self> {
        get_u64(buf)
    }

    fn encode(&self, buf: &mut BytesMut) {
        buf.put_u64(*self)
    }

    fn size(&self) -> usize {
        8
    }
}

impl Codec for String {
    fn decode(buf: &mut Bytes) -> Result<Self> {
        read_string(buf)
    }

    fn encode(&self, buf: &mut BytesMut) {
        write_string(buf, self)
    }

    fn size(&self) -> usize {
        2 + self.len()
    }
}

impl Codec for Bytes {
    fn decode(buf: &mut Bytes) -> Result<Self> {
        read_bytes(buf)
    }

    fn encode(&self, buf: &mut BytesMut) {
        write_bytes(buf, self)
    }

    fn size(&self) -> usize {
        2 + self.len()
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
    String::from_utf8(bytes.to_vec()).map_err(|_| Error::Malformed)
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

pub fn get_i64(buf: &mut Bytes) -> Result<i64> {
    assert_len(buf, 8)?;
    Ok(buf.get_i64())
}
