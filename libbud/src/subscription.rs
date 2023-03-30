use std::{collections::HashMap, fmt::Display, sync::Arc};

use roaring::RoaringTreemap;
use tokio::{
    sync::{mpsc, oneshot, RwLock},
    time::timeout,
};

use crate::{
    protocol::{ReturnCode, Subscribe},
    storage::{self, CursorStorage},
    WAIT_REPLY_TIMEOUT,
};

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

impl<T> From<mpsc::error::SendError<T>> for Error {
    fn from(value: mpsc::error::SendError<T>) -> Self {
        todo!()
    }
}

impl From<storage::Error> for Error {
    fn from(value: storage::Error) -> Self {
        todo!()
    }
}

/// Save consumption progress
/// persistent
/// memory
#[derive(Clone)]
struct Cursor {
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
    fn new() -> Result<Self> {
        // TODO load from storage
        Ok(Self {
            read_position: 0,
            latest_message_id: 0,
            delete_position: 0,
            bits: RoaringTreemap::new(),
            storage: CursorStorage::new()?,
        })
    }

    fn peek_message(&self) -> Option<u64> {
        if self.read_position >= self.latest_message_id {
            return None;
        }
        Some(self.read_position + 1)
    }

    async fn read_advance(&mut self) -> Result<()> {
        if self.read_position >= self.latest_message_id {
            return Ok(());
        }
        self.read_position += 1;
        self.storage.set_read_position(self.read_position).await?;
        Ok(())
    }

    async fn new_message(&mut self, message_id: u64) -> Result<()> {
        self.latest_message_id = message_id;
        self.storage.set_latest_message_id(message_id).await?;
        Ok(())
    }

    async fn ack(&mut self, message_id: u64) -> Result<()> {
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

pub struct SendEvent {
    pub client_id: u64,
    pub topic_name: String,
    pub message_id: u64,
    pub consumer_id: u64,
    /// true if send to client successfully
    pub res_tx: oneshot::Sender<bool>,
}

enum Notify {
    /// new message id
    NewMessage(u64),
    /// consumer permits
    AddPermits {
        client_id: u64,
        consumer_id: u64,
        add_permits: u32,
    },
}

/// task:
/// 1. receive consumer add/remove cmd
/// 2. dispatch messages to consumers
#[derive(Clone)]
struct Dispatcher {
    /// save all consumers in memory
    consumers: Arc<RwLock<Consumers>>,
    /// cursor
    cursor: Arc<RwLock<Cursor>>,
}

impl Dispatcher {
    /// used to create from persistent storage
    fn new() -> Self {
        todo!()
    }

    fn with_consumer(consumer: Consumer, cursor: Cursor) -> Self {
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

    async fn del_consumer(&self, client_id: u64, consumer_id: u64) {
        let mut consumers = self.consumers.write().await;
        let Some(cms) = consumers.as_mut() else {
            return;
        };
        match cms {
            ConsumersType::Exclusive(c)
                if c.client_id == client_id && c.consumer_id == consumer_id =>
            {
                consumers.clear();
            }
            ConsumersType::Shared(s) => {
                let Some(c) = s.get(&client_id) else {
                    return
                };
                if c.consumer_id == consumer_id {
                    s.remove(&client_id);
                }
            }
            _ => {}
        }
    }

    async fn increase_consumer_permits(&self, client_id: u64, consumer_id: u64, increase: u32) {
        self.update_consumer_permits(client_id, consumer_id, true, increase)
            .await
    }

    async fn decrease_consumer_permits(&self, client_id: u64, consumer_id: u64, decrease: u32) {
        self.update_consumer_permits(client_id, consumer_id, false, decrease)
            .await
    }

    async fn update_consumer_permits(
        &self,
        client_id: u64,
        consumer_id: u64,
        addition: bool,
        update: u32,
    ) {
        let mut consumers = self.consumers.write().await;
        let Some(cms) = consumers.as_mut() else {
            return;
        };
        match cms {
            ConsumersType::Exclusive(ex) => {
                if client_id == ex.client_id && consumer_id == ex.consumer_id {
                    if addition {
                        ex.permits += update;
                    } else {
                        ex.permits -= update;
                    }
                }
            }
            ConsumersType::Shared(shared) => {
                let Some(c) = shared.get_mut(&client_id) else {
                        return
                    };
                if c.consumer_id == consumer_id {
                    if addition {
                        c.permits += update;
                    } else {
                        c.permits -= update;
                    }
                }
            }
        }
    }

    async fn available_consumer(&self) -> Option<Consumer> {
        let consumers = self.consumers.read().await;
        let Some(cms) = consumers.as_ref() else {
            return None;
        };
        match cms {
            ConsumersType::Exclusive(c) => {
                if c.permits > 0 {
                    Some(c.clone())
                } else {
                    None
                }
            }
            ConsumersType::Shared(cs) => {
                for c in cs.values() {
                    if c.permits > 0 {
                        return Some(c.clone());
                    }
                }
                None
            }
        }
    }

    async fn consume_ack(&self, message_id: u64) -> Result<()> {
        let mut cursor = self.cursor.write().await;
        cursor.ack(message_id).await?;
        Ok(())
    }

    async fn delete_position(&self) -> u64 {
        let cursor = self.cursor.read().await;
        cursor.delete_position
    }

    async fn run(
        self,
        mut notify_rx: mpsc::UnboundedReceiver<Notify>,
        send_tx: mpsc::UnboundedSender<SendEvent>,
    ) -> Result<()> {
        while let Some(notify) = notify_rx.recv().await {
            match notify {
                Notify::NewMessage(msg_id) => {
                    let mut cursor = self.cursor.write().await;
                    cursor.new_message(msg_id).await?;
                }
                Notify::AddPermits {
                    consumer_id,
                    add_permits,
                    client_id,
                } => {
                    self.increase_consumer_permits(client_id, consumer_id, add_permits)
                        .await;
                }
            }
            let mut cursor = self.cursor.write().await;
            while let Some(next_message) = cursor.peek_message() {
                let Some(consumer) = self.available_consumer().await else {
                    return Ok(());
                };
                // serial processing
                let (res_tx, res_rx) = oneshot::channel();
                send_tx.send(SendEvent {
                    client_id: consumer.client_id,
                    topic_name: consumer.topic_name,
                    message_id: next_message,
                    consumer_id: consumer.consumer_id,
                    res_tx,
                })?;
                if let Ok(Ok(true)) = timeout(WAIT_REPLY_TIMEOUT, res_rx).await {
                    cursor.read_advance().await?;
                    self.decrease_consumer_permits(consumer.client_id, consumer.consumer_id, 1)
                        .await;
                }
            }
        }
        Ok(())
    }
}

#[derive(Debug, Clone)]
struct Consumer {
    client_id: u64,
    consumer_id: u64,
    permits: u32,
    topic_name: String,
    sub_type: SubType,
    init_pos: InitialPostion,
}

impl Consumer {
    fn new(
        client_id: u64,
        consumer_id: u64,
        topic_name: &str,
        sub_type: SubType,
        init_pos: InitialPostion,
    ) -> Self {
        Self {
            permits: 0,
            topic_name: topic_name.to_string(),
            sub_type,
            init_pos,
            client_id,
            consumer_id,
        }
    }

    fn reduce_permit(&mut self) {
        self.permits -= 1;
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

impl AsRef<Option<ConsumersType>> for Consumers {
    fn as_ref(&self) -> &Option<ConsumersType> {
        &self.0
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
    dispatcher: Dispatcher,
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
        publish_tx: mpsc::UnboundedSender<SendEvent>,
    ) -> Result<Self> {
        // start dispatch
        let (notify_tx, notify_rx) = mpsc::unbounded_channel();
        let consumer = Consumer::new(
            client_id,
            consumer_id,
            &sub.topic,
            sub.sub_type,
            sub.initial_position,
        );
        let cursor = Cursor::new()?;
        let dispatcher = Dispatcher::with_consumer(consumer, cursor);
        tokio::spawn(dispatcher.clone().run(notify_rx, publish_tx));
        Ok(Self {
            topic: sub.topic.clone(),
            name: sub.sub_name.clone(),
            notify_tx,
            dispatcher,
        })
    }

    pub async fn add_consumer(&self, client_id: u64, sub: &Subscribe) -> Result<()> {
        self.dispatcher
            .add_consumer(Consumer::new(
                client_id,
                sub.consumer_id,
                &sub.topic,
                sub.sub_type,
                sub.initial_position,
            ))
            .await
    }

    pub async fn del_consumer(&self, client_id: u64, consumer_id: u64) -> Result<()> {
        self.dispatcher.del_consumer(client_id, consumer_id).await;
        Ok(())
    }

    pub fn additional_permits(
        &self,
        client_id: u64,
        consumer_id: u64,
        add_permits: u32,
    ) -> Result<()> {
        self.notify_tx.send(Notify::AddPermits {
            client_id,
            consumer_id,
            add_permits,
        })?;
        Ok(())
    }

    pub fn message_notify(&self, message_id: u64) -> Result<()> {
        self.notify_tx.send(Notify::NewMessage(message_id))?;
        Ok(())
    }

    pub async fn consume_ack(&self, message_id: u64) -> Result<()> {
        self.dispatcher.consume_ack(message_id).await
    }

    pub async fn delete_position(&self) -> u64 {
        self.dispatcher.delete_position().await
    }
}
