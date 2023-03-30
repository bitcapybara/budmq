use std::{
    collections::HashMap,
    fmt::Display,
    io,
    ops::{Bound, RangeBounds, RangeInclusive},
    path::Path,
    sync::{
        atomic::{self, AtomicU64},
        Arc,
    },
};

use once_cell::sync::OnceCell;
use roaring::RoaringTreemap;
use tokio::sync::RwLock;

use crate::topic::Message;

type Result<T> = std::result::Result<T, Error>;

#[derive(Debug)]
pub enum Error {
    InvalidRange,
    StorageHasInited,
    StorageNotInited,
}

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

impl From<std::array::TryFromSliceError> for Error {
    fn from(value: std::array::TryFromSliceError) -> Self {
        todo!()
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
        todo!()
    }

    fn from_bytes(bytes: &[u8]) -> Result<Self> {
        todo!()
    }
}

static BASE_STORAGE: OnceCell<BaseStorage> = OnceCell::new();

/// Singleton mode, clone reference everywhere
#[derive(Debug, Clone)]
pub struct BaseStorage {
    inner: Arc<RwLock<HashMap<Vec<u8>, Vec<u8>>>>,
}

impl BaseStorage {
    pub fn init(path: &Path) -> Result<()> {
        let inner = Arc::new(RwLock::new(HashMap::new()));
        BASE_STORAGE
            .set(BaseStorage { inner })
            .map_err(|_| Error::StorageHasInited)?;
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

    pub async fn del(&self, k: &[u8]) -> Result<()> {
        let mut inner = self.inner.write().await;
        inner.remove(k);
        Ok(())
    }
}

pub struct TopicStorage {
    storage: BaseStorage,
    counter: AtomicU64,
}

impl TopicStorage {
    pub fn new() -> Result<Self> {
        Ok(Self {
            storage: BaseStorage::global()?,
            counter: AtomicU64::default(),
        })
    }
    pub async fn add_message(&self, message: &Message) -> Result<u64> {
        let msg_id = self.counter.fetch_add(1, atomic::Ordering::SeqCst);
        let key = Self::key(msg_id);
        let value = message.to_vec();
        self.storage.put(&key, &value).await?;
        Ok(msg_id)
    }

    pub async fn get_message(&self, message_id: u64) -> Result<Option<Message>> {
        self.storage
            .get(&Self::key(message_id))
            .await?
            .map(|b| Message::from_bytes(&b))
            .transpose()
    }

    pub async fn delete_range<R>(&mut self, range: R) -> Result<()>
    where
        R: RangeBounds<u64>,
    {
        for i in get_range(range)? {
            self.storage.del(&Self::key(i)).await?;
        }
        Ok(())
    }

    fn key(message_id: u64) -> Vec<u8> {
        let mut key = "TOPIC".as_bytes().to_vec();
        key.extend_from_slice(message_id.to_be_bytes().as_slice());
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
        self.get_u64(&key).await
    }

    pub async fn set_read_position(&mut self, pos: u64) -> Result<()> {
        let key = self.key(Self::READ_POSITION_KEY);
        self.storage.put(&key, pos.to_be_bytes().as_slice()).await?;
        Ok(())
    }

    pub async fn get_latest_message_id(&self) -> Result<Option<u64>> {
        let key = self.key(Self::LATEST_MESSAGE_ID_KEY);
        self.get_u64(&key).await
    }

    pub async fn set_latest_message_id(&mut self, message_id: u64) -> Result<()> {
        let key = self.key(Self::LATEST_MESSAGE_ID_KEY);
        self.storage
            .put(&key, message_id.to_be_bytes().as_slice())
            .await?;
        Ok(())
    }

    async fn get_u64(&self, key: &[u8]) -> Result<Option<u64>> {
        Ok(self
            .storage
            .get(&key)
            .await?
            .map(|b| b.as_slice().try_into())
            .transpose()?
            .map(|b| u64::from_be_bytes(b)))
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

    pub async fn set_ack_bits(&mut self, bits: &RoaringTreemap) -> Result<()> {
        let key = self.key(Self::ACK_BITS_KEY);
        let mut bytes = Vec::with_capacity(bits.serialized_size());
        bits.serialize_into(&mut bytes)?;
        self.storage.put(&key, &bytes).await?;
        Ok(())
    }

    fn key(&self, bytes: &[u8]) -> Vec<u8> {
        let mut key = format!("{}CUROSR", self.sub_name).as_bytes().to_vec();
        key.extend_from_slice(bytes);
        key
    }
}

fn get_range<R>(range: R) -> Result<RangeInclusive<u64>>
where
    R: RangeBounds<u64>,
{
    let start = match range.start_bound() {
        Bound::Included(&i) => i,
        Bound::Excluded(&u64::MAX) => return Err(Error::InvalidRange),
        Bound::Excluded(&i) => i + 1,
        Bound::Unbounded => 0,
    };
    let end = match range.end_bound() {
        Bound::Included(&i) => i,
        Bound::Excluded(&0) => return Err(Error::InvalidRange),
        Bound::Excluded(&i) => i - 1,
        Bound::Unbounded => u64::MAX,
    };
    if end < start {
        return Err(Error::InvalidRange);
    }

    Ok(start..=end)
}
