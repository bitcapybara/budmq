use std::{
    array,
    collections::HashMap,
    fmt::Display,
    io,
    ops::RangeBounds,
    string,
    sync::{
        atomic::{self, AtomicU64},
        Arc,
    },
};

use bytes::{Buf, Bytes};
use once_cell::sync::OnceCell;
use roaring::RoaringTreemap;
use tokio::sync::RwLock;

use crate::topic::{Message, SubscriptionId};

use super::get_range;

pub type Result<T> = std::result::Result<T, Error>;

#[derive(Debug)]
pub enum Error {
    InvalidRange,
    StorageAlreadyInited,
    StorageNotInited,
    Io(io::Error),
    DecodeSlice(array::TryFromSliceError),
    DecodeString(string::FromUtf8Error),
}

impl std::error::Error for Error {}

impl Display for Error {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Error::InvalidRange => write!(f, "Invalid range"),
            Error::StorageAlreadyInited => write!(f, "Storage already initialized"),
            Error::StorageNotInited => write!(f, "Storage not initialized yet"),
            Error::Io(e) => write!(f, "I/O error: {e}"),
            Error::DecodeSlice(e) => write!(f, "Decode slice error: {e}"),
            Error::DecodeString(e) => write!(f, "Decode string error: {e}"),
        }
    }
}

impl From<io::Error> for Error {
    fn from(e: io::Error) -> Self {
        Self::Io(e)
    }
}

impl From<array::TryFromSliceError> for Error {
    fn from(e: array::TryFromSliceError) -> Self {
        Self::DecodeSlice(e)
    }
}

impl From<string::FromUtf8Error> for Error {
    fn from(e: string::FromUtf8Error) -> Self {
        Self::DecodeString(e)
    }
}

trait Codec {
    fn to_vec(&self) -> Vec<u8>;
    fn from_bytes(bytes: &[u8]) -> Result<Self>
    where
        Self: Sized;
}

impl Codec for Message {
    fn to_vec(&self) -> Vec<u8> {
        let mut bytes = Vec::with_capacity(8 + self.payload.len());
        bytes.extend_from_slice(&self.seq_id.to_be_bytes());
        bytes.extend_from_slice(&self.payload);
        bytes
    }

    fn from_bytes(bytes: &[u8]) -> Result<Self> {
        let (seq_id_bytes, payload_bytes) = bytes.split_at(8);
        let seq_id = u64::from_be_bytes(seq_id_bytes.try_into()?);
        let payload = Bytes::copy_from_slice(payload_bytes);
        Ok(Self { seq_id, payload })
    }
}

impl Codec for SubscriptionId {
    fn to_vec(&self) -> Vec<u8> {
        let mut bytes = Vec::with_capacity(2 + self.topic.len() + 2 + self.name.len());
        bytes.extend_from_slice(&self.topic.len().to_be_bytes());
        bytes.extend_from_slice(self.topic.as_bytes());
        bytes.extend_from_slice(&self.name.len().to_be_bytes());
        bytes.extend_from_slice(self.name.as_bytes());
        bytes
    }

    fn from_bytes(bytes: &[u8]) -> Result<Self> {
        let mut bytes = Bytes::copy_from_slice(bytes);
        let topic = read_string(&mut bytes)?;
        let name = read_string(&mut bytes)?;
        Ok(Self { topic, name })
    }
}

fn read_string(buf: &mut Bytes) -> Result<String> {
    let len = buf.get_u16() as usize;
    let bytes = buf.split_to(len);
    Ok(String::from_utf8(bytes.to_vec())?)
}

static BASE_STORAGE: OnceCell<BaseStorage> = OnceCell::new();

/// Singleton mode, clone reference everywhere
#[derive(Debug, Clone)]
pub struct BaseStorage {
    inner: Arc<RwLock<HashMap<Vec<u8>, Vec<u8>>>>,
}

impl BaseStorage {
    pub fn init() -> Result<()> {
        let inner = Arc::new(RwLock::new(HashMap::new()));
        BASE_STORAGE
            .set(BaseStorage { inner })
            .map_err(|_| Error::StorageAlreadyInited)?;
        Ok(())
    }

    pub fn global() -> Result<Self> {
        Ok(BASE_STORAGE.get().ok_or(Error::StorageNotInited)?.clone())
    }

    pub async fn put(&self, k: &[u8], v: &[u8]) -> Result<()> {
        let mut inner = self.inner.write().await;
        inner.insert(k.to_vec(), v.to_vec());
        Ok(())
    }

    pub async fn get(&self, k: &[u8]) -> Result<Option<Vec<u8>>> {
        let inner = self.inner.read().await;
        Ok(inner.get(k).cloned())
    }

    async fn get_u64(&self, key: &[u8]) -> Result<Option<u64>> {
        Ok(self
            .get(key)
            .await?
            .map(|b| b.as_slice().try_into())
            .transpose()?
            .map(u64::from_be_bytes))
    }
    pub async fn del(&self, k: &[u8]) -> Result<()> {
        let mut inner = self.inner.write().await;
        inner.remove(k);
        Ok(())
    }
}

pub struct TopicStorage {
    topic_name: String,
    storage: BaseStorage,
    counter: AtomicU64,
}

impl TopicStorage {
    const TOPIC_KEY: &[u8] = "TOPIC".as_bytes();
    const SEQUENCE_ID_KEY: &[u8] = "SEQUENCE_ID".as_bytes();
    const SUBSCRIPTION_KEY: &[u8] = "SUBSCRIPTION".as_bytes();

    pub fn new(topic_name: &str) -> Result<Self> {
        Ok(Self {
            topic_name: topic_name.to_string(),
            storage: BaseStorage::global()?,
            counter: AtomicU64::default(),
        })
    }

    pub async fn add_subscription(&self, sub: &SubscriptionId) -> Result<()> {
        let mut id_key = self.key(Self::SUBSCRIPTION_KEY);
        let id = self
            .storage
            .get_u64(&id_key)
            .await?
            .map(|id| id + 1)
            .unwrap_or_default();
        id_key.extend_from_slice(&id.to_be_bytes());
        self.storage.put(&id_key, &sub.to_vec()).await?;
        Ok(())
    }

    pub async fn all_aubscriptions(&self) -> Result<Vec<SubscriptionId>> {
        let id_key = self.key(Self::SUBSCRIPTION_KEY);
        let Some(max_id )= self.storage.get_u64(&id_key).await? else {
            return Ok(vec![]);
        };

        let mut subs = Vec::with_capacity(max_id as usize + 1);
        for i in 0..=max_id {
            let mut key = id_key.clone();
            key.extend_from_slice(&i.to_be_bytes());
            let Some(sub) = self.storage.get(&key).await? else {
                continue;
            };
            subs.push(SubscriptionId::from_bytes(&sub)?);
        }
        Ok(subs)
    }

    pub async fn add_message(&self, message: &Message) -> Result<u64> {
        let msg_id = self.counter.fetch_add(1, atomic::Ordering::SeqCst);
        let key = self.key(msg_id.to_be_bytes().as_slice());
        let value = message.to_vec();
        self.storage.put(&key, &value).await?;
        self.set_sequence_id(message.seq_id).await?;
        Ok(msg_id)
    }

    pub async fn get_message(&self, message_id: u64) -> Result<Option<Message>> {
        let key = self.key(message_id.to_be_bytes().as_slice());
        self.storage
            .get(&key)
            .await?
            .map(|b| Message::from_bytes(&b))
            .transpose()
    }

    pub async fn delete_range<R>(&self, range: R) -> Result<()>
    where
        R: RangeBounds<u64>,
    {
        for i in get_range(range)? {
            self.storage
                .del(&self.key(i.to_be_bytes().as_slice()))
                .await?;
        }
        Ok(())
    }

    pub async fn get_sequence_id(&self) -> Result<Option<u64>> {
        let key = self.key(Self::SEQUENCE_ID_KEY);
        self.storage.get_u64(&key).await
    }

    pub async fn set_sequence_id(&self, seq_id: u64) -> Result<()> {
        let key = self.key(Self::SEQUENCE_ID_KEY);
        self.storage
            .put(&key, seq_id.to_be_bytes().as_slice())
            .await?;
        Ok(())
    }

    fn key(&self, bytes: &[u8]) -> Vec<u8> {
        let mut key = format!("TOPIC-{}", self.topic_name).as_bytes().to_vec();
        key.extend_from_slice(bytes);
        key
    }
}

#[derive(Clone)]
pub struct CursorStorage {
    sub_name: String,
    storage: BaseStorage,
}

impl CursorStorage {
    const READ_POSITION_KEY: &[u8] = "READ_POSITION".as_bytes();
    const LATEST_MESSAGE_ID_KEY: &[u8] = "LATEST_MESSAGE_ID".as_bytes();
    const ACK_BITS_KEY: &[u8] = "ACK_BITS".as_bytes();

    pub fn new(sub_name: &str) -> Result<Self> {
        Ok(Self {
            sub_name: sub_name.to_string(),
            storage: BaseStorage::global()?,
        })
    }

    pub async fn get_read_position(&self) -> Result<Option<u64>> {
        let key = self.key(Self::READ_POSITION_KEY);
        self.storage.get_u64(&key).await
    }

    pub async fn set_read_position(&self, pos: u64) -> Result<()> {
        let key = self.key(Self::READ_POSITION_KEY);
        self.storage.put(&key, pos.to_be_bytes().as_slice()).await?;
        Ok(())
    }

    pub async fn get_latest_message_id(&self) -> Result<Option<u64>> {
        let key = self.key(Self::LATEST_MESSAGE_ID_KEY);
        self.storage.get_u64(&key).await
    }

    pub async fn set_latest_message_id(&self, message_id: u64) -> Result<()> {
        let key = self.key(Self::LATEST_MESSAGE_ID_KEY);
        self.storage
            .put(&key, message_id.to_be_bytes().as_slice())
            .await?;
        Ok(())
    }

    pub async fn get_ack_bits(&self) -> Result<Option<RoaringTreemap>> {
        let key = self.key(Self::ACK_BITS_KEY);
        Ok(self
            .storage
            .get(&key)
            .await?
            .map(|b| RoaringTreemap::deserialize_from(b.as_slice()))
            .transpose()?)
    }

    pub async fn set_ack_bits(&self, bits: &RoaringTreemap) -> Result<()> {
        let key = self.key(Self::ACK_BITS_KEY);
        let mut bytes = Vec::with_capacity(bits.serialized_size());
        bits.serialize_into(&mut bytes)?;
        self.storage.put(&key, &bytes).await?;
        Ok(())
    }

    fn key(&self, bytes: &[u8]) -> Vec<u8> {
        let mut key = format!("CURSOR-{}", self.sub_name).as_bytes().to_vec();
        key.extend_from_slice(bytes);
        key
    }
}
