use std::{collections::HashMap, fmt::Display, sync::Arc};

use futures::{future, FutureExt};
use tokio::sync::{mpsc, oneshot, RwLock};

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

impl From<oneshot::error::RecvError> for Error {
    fn from(value: oneshot::error::RecvError) -> Self {
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

pub struct PublishEvent {
    pub client_id: u64,
    pub topic_name: String,
    pub message_id: u64,
    pub consumer_id: u64,
}

enum Notify {
    /// new message id
    NewMessage(u64),
    /// consumer permits
    AddPermits { consumer_id: u64, add_permits: u32 },
}

enum ConsumerEvent {
    Add {
        consumer: Consumer,
        res_tx: oneshot::Sender<Result<()>>,
    },
    Del {
        client_id: u64,
        consumer_id: u64,
        res_tx: oneshot::Sender<()>,
    },
}

/// task:
/// 1. receive consumer add/remove cmd
/// 2. dispatch messages to consumers
#[derive(Clone)]
struct Dispatcher {
    /// save all consumers in memory
    consumers: Arc<RwLock<Consumers>>,
}

impl Dispatcher {
    /// used to create from persistent storage
    fn new() -> Self {
        todo!()
    }

    fn with_consumer(consumer: Consumer) -> Self {
        todo!()
    }
    async fn add_consumer(&self, consumer: Consumer) -> Result<()> {
        let mut consumers = self.consumers.write().await;
        match consumers.as_mut() {
            Some(cms) => match cms {
                ConsumersType::Exclusive(_) => return Err(Error::SubscribeOnExclusive),
                ConsumersType::Shared(shared) => match consumer.sub_type {
                    SubType::Exclusive => return Err(Error::SubTypeMissMatch),
                    SubType::Shared => {
                        shared.insert(consumer.client_id, consumer);
                    }
                },
            },
            None => consumers.set(consumer.into()),
        }
        Ok(())
    }

    async fn del_consumer(&self, client_id: u64) {
        let mut consumers = self.consumers.write().await;
        if let Some(cms) = consumers.as_mut() {
            match cms {
                ConsumersType::Exclusive(c) if c.client_id == client_id => {
                    consumers.clear();
                }
                ConsumersType::Shared(s) => {
                    s.remove(&client_id);
                }
                _ => {}
            }
        }
    }

    async fn run(
        self,
        cursor: Cursor,
        consumer_rx: mpsc::UnboundedReceiver<ConsumerEvent>,
        notify_rx: mpsc::UnboundedReceiver<Notify>,
        publish_tx: mpsc::UnboundedSender<PublishEvent>,
    ) -> Result<()> {
        // consumer loop
        let (consumer_task, consumer_handle) =
            self.clone().receive_consumer(consumer_rx).remote_handle();
        tokio::spawn(consumer_task);

        // notify loop
        let (notify_task, notify_handle) = self
            .clone()
            .receive_notify(cursor, notify_rx, publish_tx)
            .remote_handle();
        tokio::spawn(notify_task);

        // join
        future::try_join(consumer_handle, notify_handle).await?;
        Ok(())
    }

    async fn receive_consumer(
        self,
        mut consumer_rx: mpsc::UnboundedReceiver<ConsumerEvent>,
    ) -> Result<()> {
        while let Some(consumer_event) = consumer_rx.recv().await {
            match consumer_event {
                ConsumerEvent::Add { consumer, res_tx } => {
                    res_tx.send(self.add_consumer(consumer).await);
                }
                ConsumerEvent::Del {
                    consumer_id,
                    res_tx,
                    client_id,
                } => {
                    res_tx.send(self.del_consumer(client_id).await);
                }
            }
        }
        Ok(())
    }

    async fn receive_notify(
        self,
        cursor: Cursor,
        mut notify_rx: mpsc::UnboundedReceiver<Notify>,
        publish_tx: mpsc::UnboundedSender<PublishEvent>,
    ) -> Result<()> {
        while let Some(notify) = notify_rx.recv().await {
            match notify {
                Notify::NewMessage(_) => todo!(),
                Notify::AddPermits {
                    consumer_id,
                    add_permits,
                } => todo!(),
            }
        }
        Ok(())
    }
}

struct Consumer {
    client_id: u64,
    consumer_id: u64,
    permits: u64,
    sub_type: SubType,
    init_pos: InitialPostion,
}

impl Consumer {
    fn new(client_id: u64, consumer_id: u64, sub_type: SubType, init_pos: InitialPostion) -> Self {
        Self {
            permits: 0,
            sub_type,
            init_pos,
            client_id,
            consumer_id,
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
enum ConsumersType {
    Exclusive(Consumer),
    // key = client_id
    Shared(HashMap<u64, Consumer>),
}

impl From<Consumer> for ConsumersType {
    fn from(consumer: Consumer) -> Self {
        match consumer.sub_type {
            SubType::Exclusive => Self::Exclusive(consumer),
            SubType::Shared => {
                let mut consumers = HashMap::new();
                consumers.insert(consumer.client_id, consumer);
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
    consumer_tx: mpsc::UnboundedSender<ConsumerEvent>,
    notify_tx: mpsc::UnboundedSender<Notify>,
}

impl Subscription {
    /// used to create from persistent storage
    pub fn new() -> Self {
        todo!()
    }

    pub fn from_subscribe(
        client_id: u64,
        consumer_id: u64,
        sub: &Subscribe,
        publish_tx: mpsc::UnboundedSender<PublishEvent>,
    ) -> Result<Self> {
        // start dispatch
        let (consumer_tx, consumer_rx) = mpsc::unbounded_channel();
        let (notify_tx, notify_rx) = mpsc::unbounded_channel();
        let consumer = Consumer::new(client_id, consumer_id, sub.sub_type, sub.initial_position);
        let dispatcher = Dispatcher::with_consumer(consumer);
        let cursor = Cursor::new();
        tokio::spawn(dispatcher.run(cursor, consumer_rx, notify_rx, publish_tx));
        Ok(Self {
            topic: sub.topic.clone(),
            name: sub.sub_name.clone(),
            consumer_tx,
            notify_tx,
        })
    }

    pub async fn add_consumer(&mut self, client_id: u64, sub: &Subscribe) -> Result<()> {
        let (res_tx, res_rx) = oneshot::channel();
        let event = ConsumerEvent::Add {
            consumer: Consumer::new(
                client_id,
                sub.consumer_id,
                sub.sub_type,
                sub.initial_position,
            ),
            res_tx,
        };
        self.consumer_tx.send(event);
        res_rx.await??;
        Ok(())
    }

    pub async fn del_consumer(&mut self, client_id: u64, consumer_id: u64) -> Result<()> {
        let (res_tx, res_rx) = oneshot::channel();
        let event = ConsumerEvent::Del {
            client_id,
            consumer_id,
            res_tx,
        };
        self.consumer_tx.send(event);
        res_rx.await?;
        Ok(())
    }

    pub fn additional_permits(&self, consumer_id: u64, add_permits: u32) {
        self.notify_tx.send(Notify::AddPermits {
            consumer_id,
            add_permits,
        });
    }

    pub fn message_notify(&self, message_id: u64) -> Result<()> {
        todo!()
    }

    async fn start_dispatch(&self, dispatcher: Dispatcher) -> Result<()> {
        Ok(())
    }
}
