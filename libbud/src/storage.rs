use std::fmt::Display;

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
#[derive(Clone)]
pub struct KVStorage {}

impl KVStorage {}

pub struct TopicStorage {}

impl TopicStorage {
    pub fn new() -> Self {
        Self {}
    }
    pub fn add_message(&mut self, message: Message) -> Result<u64> {
        Ok(0)
    }
}
