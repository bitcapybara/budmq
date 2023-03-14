use std::fmt::Display;

use crate::protocol::{ReturnCode, Subscribe};

type Result<T> = std::result::Result<T, Error>;

#[derive(Debug)]
pub enum Error {}

impl std::error::Error for Error {}

impl Display for Error {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        todo!()
    }
}

impl From<Error> for ReturnCode {
    fn from(value: Error) -> Self {
        todo!()
    }
}

#[derive(Debug, Copy, Clone)]
pub enum SubType {
    /// Each subscription is only allowed to contain one client
    Exclusive,
    /// Each subscription allows multiple clients
    Shared,
}

/// clients sub to this subscription
/// Internal data is consumer_id
#[derive(Debug, Clone)]
pub enum Consumers {
    Exclusive(u64),
    Shard(Vec<u64>),
}

/// save cursor in persistent
/// save consumers in memory
#[derive(Debug, Clone)]
pub struct Subscription {
    pub topic: String,
    pub sub_id: String,
    pub consumers: Consumers,
}

impl Subscription {
    pub fn from_subscribe(consumer_id: u64, sub: &Subscribe) -> Self {
        Self {
            topic: sub.topic.clone(),
            sub_id: sub.sub_name.clone(),
            consumers: match sub.sub_type {
                SubType::Exclusive => Consumers::Exclusive(consumer_id),
                SubType::Shared => Consumers::Shard(vec![consumer_id]),
            },
        }
    }

    pub fn add_consumer(&mut self, consumer_id: u64, sub_type: SubType) -> Result<()> {
        todo!()
    }
}
