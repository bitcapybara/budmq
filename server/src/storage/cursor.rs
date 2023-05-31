use bud_common::storage::Storage;
use roaring::RoaringTreemap;

use super::Result;

#[derive(Clone)]
pub struct CursorStorage<S> {
    sub_name: String,
    storage: S,
}

impl<S: Storage> CursorStorage<S> {
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
        Ok(self.storage.get_u64(&key).await?)
    }

    pub async fn set_read_position(&self, pos: u64) -> Result<()> {
        let key = self.key(Self::READ_POSITION_KEY);
        self.storage.put(&key, pos.to_be_bytes().as_slice()).await?;
        Ok(())
    }

    pub async fn get_latest_cursor_id(&self) -> Result<Option<u64>> {
        let key = self.key(Self::LATEST_MESSAGE_ID_KEY);
        Ok(self.storage.get_u64(&key).await?)
    }

    pub async fn set_latest_cursor_id(&self, message_id: u64) -> Result<()> {
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

    fn key(&self, s: &str) -> String {
        format!("CURSOR-{}-{}", self.sub_name, s)
    }
}
