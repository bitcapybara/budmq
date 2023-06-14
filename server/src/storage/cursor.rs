use bud_common::storage::MetaStorage;
use roaring::RoaringTreemap;

use super::{Error, Result};

#[derive(Clone)]
pub struct CursorStorage<S> {
    sub_name: String,
    storage: S,
}

impl<S: MetaStorage> CursorStorage<S> {
    const READ_POSITION_KEY: &str = "READ_POSITION";
    const LATEST_MESSAGE_ID_KEY: &str = "LATEST_MESSAGE_ID";
    const ACK_BITS_KEY: &str = "ACK_BITS";

    pub fn new(sub_name: &str, storage: S) -> Result<Self> {
        Ok(Self {
            sub_name: sub_name.to_string(),
            storage,
        })
    }

    pub async fn get_read_position(&self) -> Result<Option<u64>> {
        let key = self.key(Self::READ_POSITION_KEY);
        self.storage
            .get_u64(&key)
            .await
            .map_err(|e| Error::Storage(e.to_string()))
    }

    pub async fn set_read_position(&self, pos: u64) -> Result<()> {
        let key = self.key(Self::READ_POSITION_KEY);
        self.storage
            .put_u64(&key, pos)
            .await
            .map_err(|e| Error::Storage(e.to_string()))?;
        Ok(())
    }

    pub async fn get_latest_cursor_id(&self) -> Result<Option<u64>> {
        let key = self.key(Self::LATEST_MESSAGE_ID_KEY);
        self.storage
            .get_u64(&key)
            .await
            .map_err(|e| Error::Storage(e.to_string()))
    }

    pub async fn set_latest_cursor_id(&self, message_id: u64) -> Result<()> {
        let key = self.key(Self::LATEST_MESSAGE_ID_KEY);
        self.storage
            .put_u64(&key, message_id)
            .await
            .map_err(|e| Error::Storage(e.to_string()))?;
        Ok(())
    }

    pub async fn get_ack_bits(&self) -> Result<Option<RoaringTreemap>> {
        let key = self.key(Self::ACK_BITS_KEY);
        Ok(self
            .storage
            .get(&key)
            .await
            .map_err(|e| Error::Storage(e.to_string()))?
            .map(|b| RoaringTreemap::deserialize_from(b.as_slice()))
            .transpose()?)
    }

    pub async fn set_ack_bits(&self, bits: &RoaringTreemap) -> Result<()> {
        let key = self.key(Self::ACK_BITS_KEY);
        let mut bytes = Vec::with_capacity(bits.serialized_size());
        bits.serialize_into(&mut bytes)?;
        self.storage
            .put(&key, &bytes)
            .await
            .map_err(|e| Error::Storage(e.to_string()))?;
        Ok(())
    }

    fn key(&self, s: &str) -> String {
        format!("CURSOR-{}-{}", self.sub_name, s)
    }
}
