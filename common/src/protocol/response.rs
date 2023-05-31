#[derive(Debug, Clone, PartialEq, bud_derive::PacketCodec)]
pub struct Response {
    pub request_id: u64,
    pub code: ReturnCode,
}

#[derive(Debug, Clone, Copy, PartialEq, bud_derive::Codec)]
#[repr(u8)]
pub enum ReturnCode {
    Success = 0,
    AlreadyConnected,
    SubOnExlusive,
    UnexpectedSubType,
    ConsumerDuplicated,
    NotConnected,
    TopicNotExists,
    ConsumerNotFound,
    ProduceMessageDuplicated,
    ConsumeMessageDuplicated,
    InternalError,
    UnexpectedPacket,
    ProducerExclusive,
    ProducerAccessModeConflict,
    ProducerNotFound,
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
            ReturnCode::ProducerExclusive => write!(f, "ProducerExclusive"),
            ReturnCode::ProducerAccessModeConflict => write!(f, "Producer access mode conflict"),
            ReturnCode::ProducerNotFound => write!(f, "Producer not found"),
        }
    }
}

impl TryFrom<u8> for ReturnCode {
    type Error = crate::codec::Error;

    fn try_from(value: u8) -> crate::codec::Result<Self> {
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
            12 => Self::ProducerExclusive,
            13 => Self::ProducerAccessModeConflict,
            14 => Self::ProducerNotFound,
            _ => return Err(Self::Error::Malformed),
        })
    }
}
