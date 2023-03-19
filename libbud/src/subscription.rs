use std::{collections::HashMap, fmt::Display};

use crate::protocol::{ReturnCode, Subscribe};

type Result<T> = std::result::Result<T, Error>;

#[derive(Debug)]
pub enum Error {
    SubscribeOnExclusive,
    SubTypeMissMatch,
}

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

/// Save consumption progress
/// persistent
/// memory
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

pub enum InitialPostion {
    Latest,
    Earliest,
}

/// task:
/// 1. receive consumer add/remove cmd
/// 2. dispatch messages to consumers
struct Dispatcher {
    /// consumer_rx
    /// notify_rx(topic latest added message id)
    /// send_task_tx: Sender<(message_id, consumer_id)>, recv_task_rx in broker
    /// save all consumers in memory
    consumers: Option<Consumers>,
    /// save all subscription consume progress
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

    fn add_consumer(&mut self, sub_type: SubType, consumer: Consumer) -> Result<()> {
        match self.consumers {
            Some(consumers) => match consumers {
                Consumers::Exclusive(_) => return Err(Error::SubscribeOnExclusive),
                Consumers::Shared(shared) => match sub_type {
                    SubType::Exclusive => return Err(Error::SubTypeMissMatch),
                    SubType::Shared => {
                        shared.insert(consumer.id, consumer);
                    }
                },
            },
            None => self.consumers = Some(Consumers::new(sub_type, consumer)),
        }
        Ok(())
    }

    fn del_consumer(&mut self, consumer_id: u64) {
        if let Some(consumers) = self.consumers {
            match consumers {
                Consumers::Exclusive(c) if c.id == consumer_id => {
                    self.consumers.take();
                }
                Consumers::Shared(s) => {
                    s.remove(&consumer_id);
                }
                _ => {}
            }
        }
    }

    async fn run(&self) -> Result<()> {
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
    Shared(HashMap<u64, Consumer>),
}

impl Consumers {
    fn new(sub_type: SubType, consumer: Consumer) -> Self {
        match sub_type {
            SubType::Exclusive => Self::Exclusive(consumer),
            SubType::Shared => {
                let mut consumers = HashMap::new();
                consumers.insert(consumer.id, consumer);
                Self::Shared(consumers)
            }
        }
    }
}

/// save cursor in persistent
/// save consumers in memory
pub struct Subscription {
    pub topic: String,
    pub name: String,
    dispatcher: Dispatcher,
}

impl Subscription {
    pub fn from_subscribe(consumer_id: u64, sub: &Subscribe) -> Result<Self> {
        let consumers = Consumers::new(sub.sub_type, Consumer::new(consumer_id));
        Ok(Self {
            topic: sub.topic.clone(),
            name: sub.sub_name.clone(),
            dispatcher: Dispatcher::with_consumers(consumers),
        })
    }

    pub fn add_consumer(&mut self, consumer_id: u64, sub_type: SubType) -> Result<()> {
        self.dispatcher
            .add_consumer(sub_type, Consumer::new(consumer_id))?;
        Ok(())
    }

    pub fn del_consumer(&mut self, consumer_id: u64) {
        self.dispatcher.del_consumer(consumer_id);
    }

    pub fn additional_permits(&mut self, consumer_id: u64, permits: u32) {
        todo!()
    }

    pub fn message_notify(&mut self, message_id: u64) -> Result<()> {
        todo!()
    }
}
