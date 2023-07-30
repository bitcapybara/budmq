use std::{borrow::Borrow, collections::HashMap, sync::Arc};

use bud_common::{
    protocol::ReturnCode,
    storage::{MessageStorage, MetaStorage},
    types::{InitialPostion, MessageId},
};
use log::{error, trace};
use rand::seq::SliceRandom;
use tokio::{
    select,
    sync::{mpsc, oneshot, RwLock},
};
use tokio_util::sync::CancellationToken;

use super::{
    cursor::Cursor, Consumer, Consumers, Error, Result, SendEvent, SubType, SubscriptionConsumers,
};

/// task:
/// 1. receive consumer add/remove cmd
/// 2. dispatch messages to consumers
#[derive(Clone)]
pub struct Dispatcher<S1, S2> {
    /// topic id
    topic_id: u64,
    /// topic_name
    topic_name: String,
    /// save all consumers in memory
    consumers: Arc<RwLock<SubscriptionConsumers>>,
    /// cursor
    cursor: Arc<RwLock<Cursor<S1, S2>>>,
    send_tx: mpsc::Sender<SendEvent>,
    /// token
    token: CancellationToken,
}

impl<S1: MetaStorage, S2: MessageStorage> Dispatcher<S1, S2> {
    pub async fn add_consumer(&self, consumer: Consumer) -> Result<()> {
        let mut consumers = self.consumers.write().await;
        match consumers.as_mut() {
            Some(cms) => match cms {
                Consumers::Exclusive(_) => return Err(Error::Response(ReturnCode::SubOnExlusive)),
                Consumers::Shared(shared) => match consumer.sub_type {
                    SubType::Exclusive => {
                        return Err(Error::Response(ReturnCode::UnexpectedSubType))
                    }
                    SubType::Shared => match shared.get_mut(&consumer.client_id) {
                        Some(consumers) => {
                            consumers.insert(consumer.id, consumer);
                        }
                        None => {
                            let client_id = consumer.client_id;
                            let mut consumers = HashMap::new();
                            consumers.insert(consumer.id, consumer);
                            shared.insert(client_id, consumers);
                        }
                    },
                },
            },
            None => consumers.set(consumer.into()),
        }
        Ok(())
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
            Consumers::Shared(clients) => {
                let mut rg = rand::thread_rng();
                let mut consumers = Vec::new();
                for client in clients.values() {
                    for c in client.values() {
                        if c.permits > 0 {
                            consumers.push(c);
                        }
                    }
                }
                consumers
                    .choose_weighted(&mut rg, |x| 1000 - x.permits)
                    .ok()
                    .cloned()
                    .cloned()
            }
        }
    }

    pub async fn consume_ack<T>(&self, cursor_ids: T) -> Result<()>
    where
        T: Borrow<[u64]>,
    {
        let mut cursor = self.cursor.write().await;
        for cursor_id in cursor_ids.borrow() {
            cursor.ack(*cursor_id).await?;
        }
        Ok(())
    }

    async fn decrease_consumer_permits(&self, client_id: u64, consumer_id: u64, decrease: u32) {
        self.update_consumer_permits(client_id, consumer_id, false, decrease)
            .await
    }

    pub async fn del_consumer(&self, client_id: u64, consumer_id: u64) {
        trace!("delete consumer: {consumer_id}, client_id: {client_id}");
        let mut consumers = self.consumers.write().await;
        let Some(cms) = consumers.as_mut() else {
            return;
        };
        match cms {
            Consumers::Exclusive(c) if c.client_id == client_id && c.id == consumer_id => {
                consumers.clear();
            }
            Consumers::Shared(clients) => {
                let Some(consumers) = clients.get_mut(&client_id) else {
                    return;
                };
                consumers.remove(&consumer_id);
                if consumers.is_empty() {
                    clients.remove(&client_id);
                }
            }
            _ => {}
        }
    }

    async fn get_next_cursor(&self) -> Option<u64> {
        let cursor = self.cursor.read().await;
        cursor.peek_message()
    }

    pub async fn increase_consumer_permits(&self, client_id: u64, consumer_id: u64, increase: u32) {
        self.update_consumer_permits(client_id, consumer_id, true, increase)
            .await
    }

    /// load from storage
    #[allow(clippy::too_many_arguments)]
    pub async fn new(
        topic_id: u64,
        topic_name: &str,
        sub_name: &str,
        meta_storage: S1,
        message_storage: S2,
        init_position: InitialPostion,
        send_tx: mpsc::Sender<SendEvent>,
        token: CancellationToken,
    ) -> Result<Self> {
        Ok(Self {
            consumers: Arc::new(RwLock::new(SubscriptionConsumers::empty())),
            cursor: Arc::new(RwLock::new(
                Cursor::new(
                    topic_name,
                    sub_name,
                    meta_storage,
                    message_storage,
                    init_position,
                )
                .await?,
            )),
            send_tx,
            token,
            topic_id,
            topic_name: topic_name.to_string(),
        })
    }

    async fn peek_and_send(&self) -> bool {
        trace!("dispatcher::run cursor peek a message");
        while let Some(next_cursor_id) = self.get_next_cursor().await {
            trace!("dispatcher::run: find available consumers");
            let Some(consumer) = self.available_consumer().await else {
                return false;
            };
            trace!(
                "find consumer: {}, client_id: {}",
                consumer.id,
                consumer.client_id
            );
            // serial processing
            trace!("dispatcher::run: send message to broker");
            let (res_tx, res_rx) = oneshot::channel();
            let event = SendEvent {
                client_id: consumer.client_id,
                topic_name: consumer.topic_name,
                message_id: MessageId {
                    topic_id: self.topic_id,
                    cursor_id: next_cursor_id,
                },
                consumer_id: consumer.id,
                res_tx,
            };
            select! {
                res = self.send_tx.send(event) => {
                    if res.is_err() {
                        return true;
                    }
                }
                _ = self.token.cancelled() => {
                    return true;
                }
            }
            trace!("dispatcher::run: waiting for replay");
            select! {
                res = res_rx => {
                    if res.is_ok() {
                        let mut cursor = self.cursor.write().await;
                        if let Err(e) = cursor.read_advance().await {
                            error!("cursor read advance error: {e}")
                        }
                        let (client_id, consumer_id) = (consumer.client_id, consumer.id);
                        self.decrease_consumer_permits(client_id, consumer_id, 1).await;
                    }
                }
                _ = self.token.cancelled() => {
                    return true;
                }
            }
        }
        false
    }

    /// notify_rx receive event from subscription
    pub async fn run(self, mut notify_rx: mpsc::Receiver<()>) {
        trace!("dispatcher::run: start dispatcher task loop");
        loop {
            select! {
                res = notify_rx.recv() => {
                    if res.is_none() {
                        return;
                    }
                    if self.peek_and_send().await {
                        return
                    }
                }
                _ = self.token.cancelled() => {
                    return;
                }
            }
        }
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
            Consumers::Shared(clients) => {
                let Some(consumers) = clients.get_mut(&client_id) else {
                    return;
                };
                let Some(c) = consumers.get_mut(&consumer_id) else {
                    return;
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

    pub async fn update_message_id(&self, cursor_id: u64) -> Result<()> {
        let mut cursor = self.cursor.write().await;
        cursor.new_message(cursor_id).await?;
        Ok(())
    }

    #[allow(clippy::too_many_arguments)]
    pub async fn with_consumer(
        topic_id: u64,
        topic_name: &str,
        consumer: Consumer,
        meta_storage: S1,
        message_storage: S2,
        init_position: InitialPostion,
        send_tx: mpsc::Sender<SendEvent>,
        token: CancellationToken,
    ) -> Result<Self> {
        let sub_name = consumer.sub_name.clone();
        let consumers = Arc::new(RwLock::new(consumer.into()));
        let cursor = Arc::new(RwLock::new(
            Cursor::new(
                topic_name,
                &sub_name,
                meta_storage,
                message_storage,
                init_position,
            )
            .await?,
        ));
        Ok(Self {
            consumers,
            cursor,
            send_tx,
            token,
            topic_id,
            topic_name: topic_name.to_string(),
        })
    }
}
