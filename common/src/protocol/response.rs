#[derive(Debug, Clone, PartialEq, codec_derive::Codec)]
pub struct Response {
    pub request_id: u64,
    pub code: ReturnCode,
}

#[derive(Debug, Clone, Copy, PartialEq)]
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
    InternalError = 10,
    UnexpectedPacket = 11,
}

impl std::fmt::Display for ReturnCode {
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
            ReturnCode::InternalError => write!(f, "ServerInternalError"),
            ReturnCode::UnexpectedPacket => write!(f, "UnecpectedPacket"),
        }
    }
}

impl TryFrom<u8> for ReturnCode {
    type Error = super::Error;

    fn try_from(value: u8) -> super::Result<Self> {
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
            10 => Self::InternalError,
            11 => Self::UnexpectedPacket,
            _ => return Err(super::Error::UnsupportedReturnCode),
        })
    }
}
