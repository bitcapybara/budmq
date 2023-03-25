use std::{fmt::Display, ops::RangeBounds, path::Path};

use crate::topic::Message;

type Result<T> = std::result::Result<T, Error>;

#[derive(Debug)]
pub enum Error {}

impl std::error::Error for Error {}

impl Display for Error {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        todo!()
    }
}

/// Singleton mode, clone reference everywhere
pub struct BaseStorage {}

impl BaseStorage {
    pub fn new(path: &Path) -> Result<Self> {
        Ok(Self {})
    }
}

pub struct TopicStorage {}

impl TopicStorage {
    pub fn new() -> Self {
        Self {}
    }
    pub fn add_message(&mut self, message: &Message) -> Result<u64> {
        Ok(0)
    }

    pub fn get_message(&self, message_id: u64) -> Result<Option<Message>> {
        Ok(None)
    }

    pub async fn delete_range(&mut self, range: impl RangeBounds<u64>) -> Result<()> {
        Ok(())
    }
}
