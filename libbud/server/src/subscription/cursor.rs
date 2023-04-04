use roaring::RoaringTreemap;

use crate::storage::CursorStorage;

use super::Result;

/// Save consumption progress
/// persistent
/// memory
#[derive(Clone)]
pub struct Cursor {
    /// current read cursor position
    /// init by init_position arg
    read_position: u64,
    /// high water mark
    latest_message_id: u64,
    /// low water mark
    delete_position: u64,
    /// message ack info
    bits: RoaringTreemap,
    /// storage
    storage: CursorStorage,
}

impl Cursor {
    pub async fn new(sub_name: &str) -> Result<Self> {
        let storage = CursorStorage::new(sub_name)?;
        let read_position = storage.get_read_position().await?.unwrap_or_default();
        let latest_message_id = storage.get_latest_message_id().await?.unwrap_or_default();
        let bits = storage.get_ack_bits().await?.unwrap_or_default();
        let delete_position = bits.min().unwrap_or_default();
        Ok(Self {
            read_position,
            latest_message_id,
            delete_position,
            bits,
            storage,
        })
    }

    pub fn delete_position(&self) -> u64 {
        self.delete_position
    }

    pub fn peek_message(&self) -> Option<u64> {
        if self.read_position >= self.latest_message_id {
            return None;
        }
        Some(self.read_position + 1)
    }

    pub async fn read_advance(&mut self) -> Result<()> {
        if self.read_position >= self.latest_message_id {
            return Ok(());
        }
        self.read_position += 1;
        self.storage.set_read_position(self.read_position).await?;
        Ok(())
    }

    pub async fn new_message(&mut self, message_id: u64) -> Result<()> {
        self.latest_message_id = message_id;
        self.storage.set_latest_message_id(message_id).await?;
        Ok(())
    }

    pub async fn ack(&mut self, message_id: u64) -> Result<()> {
        // set message acked
        self.bits.insert(message_id);
        // update delete_position
        if message_id - self.delete_position > 1 {
            return Ok(());
        }
        let Some(max) = self.bits.max() else {
                return Ok(());
        };
        for i in message_id..max {
            if self.bits.contains(i) {
                self.delete_position = i;
            } else {
                break;
            }
        }
        // remove all values less than delete position
        self.bits.remove_range(..self.delete_position);
        self.storage.set_ack_bits(&self.bits).await?;
        Ok(())
    }
}
