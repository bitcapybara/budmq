use bud_common::{storage::Storage, types::InitialPostion};
use roaring::RoaringTreemap;

use crate::storage::cursor::CursorStorage;

use super::Result;

/// Save consumption progress
/// persistent
/// memory
#[derive(Clone)]
pub struct Cursor<S> {
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
    storage: CursorStorage<S>,
}

impl<S: Storage> Cursor<S> {
    pub async fn new(sub_name: &str, storage: S, init_position: InitialPostion) -> Result<Self> {
        let storage = CursorStorage::new(sub_name, storage)?;
        let latest_message_id = storage.get_latest_cursor_id().await?.unwrap_or_default();
        let bits = storage.get_ack_bits().await?.unwrap_or_default();
        let delete_position = bits.min().unwrap_or_default();
        let read_position = match init_position {
            InitialPostion::Latest => storage.get_read_position().await?.unwrap_or_default(),
            InitialPostion::Earliest => delete_position + 1,
        };
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
        self.storage.set_latest_cursor_id(message_id).await?;
        Ok(())
    }

    pub async fn ack(&mut self, cursor_id: u64) -> Result<()> {
        // set message acked
        self.bits.insert(cursor_id);
        // update delete_position
        if cursor_id - self.delete_position > 1 {
            return Ok(());
        }
        let Some(max) = self.bits.max() else {
                return Ok(());
        };
        for i in cursor_id..max {
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

#[cfg(test)]
mod tests {

    use bud_common::storage::memory::MemoryStorage;

    use super::*;

    #[tokio::test]
    async fn cursor_works() {
        let mut cursor = Cursor::new("test-sub", MemoryStorage::new(), InitialPostion::Latest)
            .await
            .unwrap();

        assert_eq!(cursor.peek_message(), None);
        cursor.new_message(1).await.unwrap();
        assert_eq!(cursor.peek_message(), Some(1));
        cursor.read_advance().await.unwrap();
        assert_eq!(cursor.peek_message(), None);
    }
}
