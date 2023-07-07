use bud_common::storage::{MessageStorage, MetaStorage};
use log::trace;
use roaring::RoaringTreemap;

use super::{Error, Result};

#[derive(Clone)]
pub struct CursorStorage<S1, S2> {
    topic_name: String,
    sub_name: String,
    meta_storage: S1,
    message_storage: S2,
}

impl<S1: MetaStorage, S2: MessageStorage> CursorStorage<S1, S2> {
    const READ_POSITION_KEY: &str = "READ_POSITION";
    const LATEST_MESSAGE_ID_KEY: &str = "LATEST_MESSAGE_ID";

    pub fn new(
        topic_name: &str,
        sub_name: &str,
        meta_storage: S1,
        message_storage: S2,
    ) -> Result<Self> {
        Ok(Self {
            topic_name: topic_name.to_string(),
            sub_name: sub_name.to_string(),
            meta_storage,
            message_storage,
        })
    }

    pub async fn get_read_position(&self) -> Result<Option<u64>> {
        let key = self.key(Self::READ_POSITION_KEY);
        self.meta_storage
            .get_u64(&key)
            .await
            .map_err(|e| Error::Storage(e.to_string()))
    }

    pub async fn set_read_position(&self, pos: u64) -> Result<()> {
        let key = self.key(Self::READ_POSITION_KEY);
        self.meta_storage
            .put_u64(&key, pos)
            .await
            .map_err(|e| Error::Storage(e.to_string()))?;
        Ok(())
    }

    pub async fn get_latest_cursor_id(&self) -> Result<Option<u64>> {
        let key = self.key(Self::LATEST_MESSAGE_ID_KEY);
        self.meta_storage
            .get_u64(&key)
            .await
            .map_err(|e| Error::Storage(e.to_string()))
    }

    pub async fn set_latest_cursor_id(&self, message_id: u64) -> Result<()> {
        let key = self.key(Self::LATEST_MESSAGE_ID_KEY);
        self.meta_storage
            .put_u64(&key, message_id)
            .await
            .map_err(|e| Error::Storage(e.to_string()))?;
        Ok(())
    }

    pub async fn get_ack_bits(&self) -> Result<Option<RoaringTreemap>> {
        Ok(self
            .message_storage
            .load_cursor(&self.topic_name, &self.sub_name)
            .await
            .map_err(|e| Error::Storage(e.to_string()))?
            .map(|b| RoaringTreemap::deserialize_from(b.as_slice()))
            .transpose()?)
    }

    pub async fn set_ack_bits(&self, bits: &RoaringTreemap) -> Result<()> {
        let mut bytes = Vec::with_capacity(bits.serialized_size());
        bits.serialize_into(&mut bytes)?;
        self.message_storage
            .save_cursor(&self.topic_name, &self.sub_name, &bytes)
            .await
            .map_err(|e| Error::Storage(e.to_string()))?;
        Ok(())
    }

    fn key(&self, s: &str) -> String {
        format!("CURSOR-{}-{}", self.sub_name, s)
    }
}
