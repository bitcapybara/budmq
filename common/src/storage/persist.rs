use bonsaidb::local::{
    config::{Builder, StorageConfiguration},
    AsyncDatabase,
};

pub type Result<T> = std::result::Result<T, Error>;

#[derive(Debug)]
pub enum Error {}

impl std::error::Error for Error {}

impl std::fmt::Display for Error {
    fn fmt(&self, _f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        todo!()
    }
}

impl From<bonsaidb::local::Error> for Error {
    fn from(_e: bonsaidb::local::Error) -> Self {
        todo!()
    }
}

#[derive(Clone)]
pub struct PersistStorage {
    inner: AsyncDatabase,
}

impl PersistStorage {
    async fn new() -> Result<Self> {
        let config = StorageConfiguration::new("kv.db");
        let inner = AsyncDatabase::open::<()>(config).await?;
        Ok(Self { inner })
    }

    async fn put(&self, _k: &[u8], _v: &[u8]) -> Result<()> {
        todo!()
    }

    async fn get(&self, _k: &[u8]) -> Result<Option<Vec<u8>>> {
        todo!()
    }

    async fn del(&self, _k: &[u8]) -> Result<()> {
        todo!()
    }

    async fn get_u64(&self, _k: &[u8]) -> Result<Option<u64>> {
        todo!()
    }

    async fn set_u64(&self, _k: &[u8], _v: u64) -> Result<()> {
        todo!()
    }

    async fn fetch_add(&self, _k: &[u8], _v: u64) -> Result<u64> {
        todo!()
    }
}
