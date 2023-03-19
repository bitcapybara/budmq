use std::{collections::HashMap, fmt::Display, sync::Arc};

use tokio::sync::{mpsc, RwLock};

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

#[derive(Debug, Clone, Copy)]
pub enum SubType {
    /// Each subscription is only allowed to contain one client
    Exclusive,
    /// Each subscription allows multiple clients
    Shared,
}

#[derive(Debug, Clone, Copy)]
pub enum InitialPostion {
    Latest,
    Earliest,
}

/// task:
/// 1. receive consumer add/remove cmd
/// 2. dispatch messages to consumers
struct Dispatcher {
    /// notify_rx(topic latest added message id)
    /// send_task_tx: Sender<(message_id, consumer_id)>, recv_task_rx in broker
    /// save all consumers in memory
    consumers: Arc<RwLock<Consumers>>,
    /// save all subscription consume progress
    cursor: Cursor,
}

impl Dispatcher {
    fn new() -> Self {
        Self {
            consumers: Arc::new(RwLock::new(Consumers(None))),
            cursor: Cursor::new(),
        }
    }

    fn with_consumer(consumer: Consumer) -> Self {
        Self {
            consumers: Arc::new(RwLock::new(Consumers::new(consumer.into()))),
            cursor: Cursor::new(),
        }
    }

    async fn add_consumer(&mut self, consumer: Consumer) -> Result<()> {
        let mut consumers = self.consumers.write().await;
        match consumers.as_mut() {
            Some(cms) => match cms {
                ConsumersType::Exclusive(_) => return Err(Error::SubscribeOnExclusive),
                ConsumersType::Shared(shared) => match consumer.sub_type {
                    SubType::Exclusive => return Err(Error::SubTypeMissMatch),
                    SubType::Shared => {
                        shared.insert(consumer.id, consumer);
                    }
                },
            },
            None => consumers.set(consumer.into()),
        }
        Ok(())
    }

    async fn del_consumer(&mut self, consumer_id: u64) {
        let mut consumers = self.consumers.write().await;
        if let Some(cms) = consumers.as_mut() {
            match cms {
                ConsumersType::Exclusive(c) if c.id == consumer_id => {
                    consumers.clear();
                }
                ConsumersType::Shared(s) => {
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
    sub_type: SubType,
    init_pos: InitialPostion,
}

impl Consumer {
    fn new(id: u64, sub_type: SubType, init_pos: InitialPostion) -> Self {
        Self {
            id,
            permits: 0,
            sub_type,
            init_pos,
        }
    }
}

struct Consumers(Option<ConsumersType>);

impl Consumers {
    fn new(consumers: ConsumersType) -> Self {
        Self(Some(consumers))
    }
    fn set(&mut self, consumers: ConsumersType) {
        self.0 = Some(consumers)
    }
    fn clear(&mut self) {
        self.0.take();
    }
}

impl AsMut<Option<ConsumersType>> for Consumers {
    fn as_mut(&mut self) -> &mut Option<ConsumersType> {
        &mut self.0
    }
}

/// clients sub to this subscription
/// Internal data is consumer_id
enum ConsumersType {
    Exclusive(Consumer),
    Shared(HashMap<u64, Consumer>),
}

impl From<Consumer> for ConsumersType {
    fn from(consumer: Consumer) -> Self {
        match consumer.sub_type {
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
        let consumer = Consumer::new(consumer_id, sub.sub_type, sub.initial_position);
        Ok(Self {
            topic: sub.topic.clone(),
            name: sub.sub_name.clone(),
            dispatcher: Dispatcher::with_consumer(consumer),
        })
    }

    pub async fn add_consumer(&mut self, sub: &Subscribe) -> Result<()> {
        self.dispatcher
            .add_consumer(Consumer::new(
                sub.consumer_id,
                sub.sub_type,
                sub.initial_position,
            ))
            .await?;
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
