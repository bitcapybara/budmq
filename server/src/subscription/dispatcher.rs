use std::sync::Arc;

use bud_common::{storage::Storage, types::InitialPostion};
use log::trace;
use tokio::{
    select,
    sync::{mpsc, oneshot, RwLock},
};
use tokio_util::sync::CancellationToken;

use super::{
    cursor::Cursor, Consumer, Consumers, Error, Result, SendEvent, SubType, TopicConsumers,
};

pub enum Notify {
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
pub struct Dispatcher<S> {
    /// save all consumers in memory
    consumers: Arc<RwLock<TopicConsumers>>,
    /// cursor
    cursor: Arc<RwLock<Cursor<S>>>,
    send_tx: mpsc::Sender<SendEvent>,
    /// token
    token: CancellationToken,
}

impl<S: Storage> Dispatcher<S> {
    /// load from storage
    pub async fn new(
        sub_name: &str,
        storage: S,
        init_position: InitialPostion,
        send_tx: mpsc::Sender<SendEvent>,
        token: CancellationToken,
    ) -> Result<Self> {
        Ok(Self {
            consumers: Arc::new(RwLock::new(TopicConsumers::empty())),
            cursor: Arc::new(RwLock::new(
                Cursor::new(sub_name, storage, init_position).await?,
            )),
            send_tx,
            token,
        })
    }

    pub async fn with_consumer(
        consumer: Consumer,
        storage: S,
        init_position: InitialPostion,
        send_tx: mpsc::Sender<SendEvent>,
        token: CancellationToken,
    ) -> Result<Self> {
        let sub_name = consumer.sub_name.clone();
        let consumers = Arc::new(RwLock::new(TopicConsumers::from_consumer(consumer)));
        let cursor = Arc::new(RwLock::new(
            Cursor::new(&sub_name, storage, init_position).await?,
        ));
        Ok(Self {
            consumers,
            cursor,
            send_tx,
            token,
        })
    }

    pub async fn add_consumer(&self, consumer: Consumer) -> Result<()> {
        let mut consumers = self.consumers.write().await;
        match consumers.as_mut() {
            Some(cms) => match cms {
                Consumers::Exclusive(_) => return Err(Error::SubscribeOnExclusive),
                Consumers::Shared(shared) => match consumer.sub_type {
                    SubType::Exclusive => return Err(Error::SubTypeUnexpected),
                    SubType::Shared => {
                        shared.insert(consumer.client_id, consumer);
                    }
                },
            },
            None => consumers.set(consumer.into()),
        }
        Ok(())
    }

    pub async fn del_consumer(&self, client_id: u64, consumer_id: u64) {
        let mut consumers = self.consumers.write().await;
        let Some(cms) = consumers.as_mut() else {
            return;
        };
        match cms {
            Consumers::Exclusive(c) if c.client_id == client_id && c.id == consumer_id => {
                consumers.clear();
            }
            Consumers::Shared(s) => {
                let Some(c) = s.get(&client_id) else {
                    return
                };
                if c.id == consumer_id {
                    s.remove(&client_id);
                }
                if s.is_empty() {
                    consumers.clear()
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
            Consumers::Exclusive(ex) => {
                if client_id == ex.client_id && consumer_id == ex.id {
                    if addition {
                        ex.permits += update;
                    } else {
                        ex.permits -= update;
                    }
                }
            }
            Consumers::Shared(shared) => {
                let Some(c) = shared.get_mut(&client_id) else {
                        return
                    };
                if c.id == consumer_id {
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
            Consumers::Exclusive(c) => {
                if c.permits > 0 {
                    Some(c.clone())
                } else {
                    None
                }
            }
            Consumers::Shared(cs) => {
                for c in cs.values() {
                    if c.permits > 0 {
                        return Some(c.clone());
                    }
                }
                None
            }
        }
    }

    pub async fn consume_ack(&self, cursor_id: u64) -> Result<()> {
        let mut cursor = self.cursor.write().await;
        cursor.ack(cursor_id).await?;
        Ok(())
    }

    pub async fn delete_position(&self) -> u64 {
        let cursor = self.cursor.read().await;
        cursor.delete_position()
    }

    /// notify_rx receive event from subscription
    pub async fn run(self, mut notify_rx: mpsc::UnboundedReceiver<Notify>) -> Result<()> {
        trace!("dispatcher::run: start dispatcher task loop");
        loop {
            select! {
                res = notify_rx.recv() => {
                    let Some(notify) = res else {
                        return Ok(());
                    };
                    match notify {
                        Notify::NewMessage(msg_id) => {
                            trace!("dispatcher::run: receive a NEW_MESSAGE notify");
                            let mut cursor = self.cursor.write().await;
                            cursor.new_message(msg_id).await?;
                        }
                        Notify::AddPermits {
                            consumer_id,
                            add_permits,
                            client_id,
                        } => {
                            trace!("dispatcher::run: receive a ADD_PERMITS notify");
                            self.increase_consumer_permits(client_id, consumer_id, add_permits)
                                .await;
                        }
                    }
                    let mut cursor = self.cursor.write().await;
                    trace!("dispatcher::run cursor peek a message");
                    while let Some(next_message) = cursor.peek_message() {
                        trace!("dispatcher::run: find available consumers");
                        let Some(consumer) = self.available_consumer().await else {
                            continue;
                        };
                        // serial processing
                        trace!("dispatcher::run: send message to broker");
                        let (res_tx, res_rx) = oneshot::channel();
                        let event = SendEvent {
                            client_id: consumer.client_id,
                            topic_name: consumer.topic_name,
                            message_id: next_message,
                            consumer_id: consumer.id,
                            res_tx,
                        };
                        select!{
                            res = self.send_tx.send(event) => {
                                res?;
                            }
                            _ = self.token.cancelled() => {
                                return Ok(());
                            }
                        }
                        trace!("dispatcher::run: waiting for replay");
                        select! {
                            res = res_rx => {
                                if res? {
                                    cursor.read_advance().await?;
                                    let (client_id, consumer_id) = (consumer.client_id, consumer.id);
                                    self.decrease_consumer_permits(client_id, consumer_id, 1).await;
                                }
                            }
                        }
                    }
                }
                _ = self.token.cancelled() => {
                    return Ok(());
                }
            }
        }
    }
}
