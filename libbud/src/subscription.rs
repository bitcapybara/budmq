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

/// Save consumption progress information
#[derive(Debug, Clone)]
struct Cursor {}

impl Cursor {
    fn new() -> Self {
        Self {}
    }
}

pub enum SubType {
    /// Each subscription is only allowed to contain one client
    Exclusive,
    /// Each subscription allows multiple clients
    Shared,
}

/// task:
/// 1. receive consumer add/remove cmd
/// 2. dispatch messages to consumers
struct Dispatcher {
    /// consumer_rx
    /// notify_rx(topic latest added message id)
    /// send_task_tx: Sender<(message_id, consumer_id)>, recv_task_rx in broker
    consumers: Option<Consumers>,
    cursor: Cursor,
}

impl Dispatcher {
    fn new() -> Self {
        Self {
            consumers: None,
            cursor: Cursor::new(),
        }
    }

    fn with_consumers(consumers: Consumers) -> Self {
        Self {
            consumers: Some(consumers),
            cursor: Cursor::new(),
        }
    }

    async fn run(self) -> Result<()> {
        Ok(())
    }
}

struct Consumer {
    id: u64,
    permits: u64,
}

impl Consumer {
    fn new(id: u64) -> Self {
        Self { id, permits: 0 }
    }
}

/// clients sub to this subscription
/// Internal data is consumer_id
enum Consumers {
    Exclusive(Consumer),
    Shard(Vec<Consumer>),
}

/// save cursor in persistent
/// save consumers in memory
pub struct Subscription {
    pub topic: String,
    pub sub_id: String,
    dispatcher: Dispatcher,
}

impl Subscription {
    pub fn from_subscribe(consumer_id: u64, sub: &Subscribe) -> Result<Self> {
        let consumers = match sub.sub_type {
            SubType::Exclusive => Consumers::Exclusive(Consumer::new(consumer_id)),
            SubType::Shared => Consumers::Shard(vec![Consumer::new(consumer_id)]),
        };
        Ok(Self {
            topic: sub.topic.clone(),
            sub_id: sub.sub_name.clone(),
            dispatcher: Dispatcher::with_consumers(consumers),
        })
    }

    pub fn add_consumer(&mut self, consumer_id: u64, sub_type: SubType) -> Result<()> {
        todo!()
    }

    pub fn del_consumer(&mut self, consumer_id: u64) {
        todo!()
    }

    pub fn additional_permits(&mut self, consumer_id: u64, permits: u32) {
        todo!()
    }
}
