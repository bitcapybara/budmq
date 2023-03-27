use std::{
    collections::HashMap,
    fmt::Display,
    ops::{Bound, RangeBounds, RangeInclusive},
    path::Path,
    sync::{
        atomic::{self, AtomicU64},
        Arc,
    },
};

use once_cell::sync::OnceCell;
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
        let key = msg_id.to_be_bytes();
        let value = message.to_vec();
        self.storage.put(&key, &value).await?;
        Ok(msg_id)
    }

    pub async fn get_message(&self, message_id: u64) -> Result<Option<Message>> {
        self.storage
            .get(&message_id.to_be_bytes())
            .await?
            .map(|b| Message::from_bytes(&b))
            .transpose()
    }

    pub async fn delete_range<R>(&mut self, range: R) -> Result<()>
    where
        R: RangeBounds<u64>,
    {
        for i in get_range(range)? {
            self.storage.del(&i.to_be_bytes()).await?;
        }
        Ok(())
    }
}

#[derive(Clone)]
pub struct CursorStorage {
    storage: BaseStorage,
}

impl CursorStorage {
    pub fn new() -> Result<Self> {
        Ok(Self {
            storage: BaseStorage::global()?,
        })
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
