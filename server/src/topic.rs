use std::{borrow::Borrow, collections::HashMap};

use bud_common::{
    protocol::{Publish, ReturnCode, Subscribe},
    storage::{MessageStorage, MetaStorage},
    types::{AccessMode, MessageId, SubscriptionInfo, TopicMessage},
};
use log::trace;
use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;

use crate::{
    storage::{self, topic::TopicStorage},
    subscription::{self, SendEvent, Subscription},
};

pub type Result<T> = std::result::Result<T, Error>;

#[derive(Debug, thiserror::Error)]
pub enum Error {
    /// Server response error
    #[error("Response code: {0}")]
    Response(ReturnCode),
    /// Storage error
    #[error("Storage error: {0}")]
    Storage(#[from] storage::Error),
    /// Subscription error
    #[error("Subscription error: {0}")]
    Subscription(#[from] subscription::Error),
}

#[derive(Clone)]
pub struct Producer {
    id: u64,
    name: String,
    access_mode: AccessMode,
    sequence_id: u64,
}

pub enum Producers {
    Exclusive(Producer),
    Shared(HashMap<u64, Producer>),
}

pub struct TopicProducers(Option<Producers>);

impl TopicProducers {
    fn add_producer(&mut self, producer: &Producer) -> Result<()> {
        match self.0.as_mut() {
            Some(producers) => match (producers, producer.access_mode) {
                (Producers::Exclusive(_), AccessMode::Exclusive) => {
                    return Err(Error::Response(ReturnCode::ProducerExclusive))
                }
                (Producers::Shared(map), AccessMode::Shared) => {
                    map.insert(producer.id, producer.clone());
                }
                _ => return Err(Error::Response(ReturnCode::ProducerAccessModeConflict)),
            },
            None => match producer.access_mode {
                AccessMode::Exclusive => self.0 = Some(Producers::Exclusive(producer.clone())),
                AccessMode::Shared => {
                    let mut map = HashMap::new();
                    map.insert(producer.id, producer.clone());
                    self.0 = Some(Producers::Shared(map))
                }
            },
        }
        Ok(())
    }

    fn del_producer(&mut self, producer_id: u64) {
        let Some(producers) = self.0.as_mut() else {
            return;
        };
        match producers {
            Producers::Exclusive(p) => {
                if p.id == producer_id {
                    self.0.take();
                }
            }
            Producers::Shared(map) => {
                map.remove(&producer_id);
            }
        }
    }

    fn get_producer_name(&self, producer_id: u64) -> Option<String> {
        self.0.as_ref().and_then(|producers| match producers {
            Producers::Exclusive(p) => Some(p.name.clone()),
            Producers::Shared(map) => map.get(&producer_id).map(|p| p.name.clone()),
        })
    }

    fn get_producer_id(&self, producer_name: &str) -> Option<u64> {
        self.0.as_ref().and_then(|producers| match producers {
            Producers::Exclusive(p) => Some(p.id),
            Producers::Shared(map) => {
                for p in map.values() {
                    if p.name == producer_name {
                        return Some(p.id);
                    }
                }
                None
            }
        })
    }
}

/// Save all messages associated with this topic in subscription
/// Save subscription associated with this topic in memory
pub struct Topic<M, S> {
    /// topic id
    pub id: u64,
    /// topic name
    pub name: String,
    /// key = sub_name
    subscriptions: HashMap<String, Subscription<M, S>>,
    /// producer
    producers: TopicProducers,
    /// message storage
    storage: TopicStorage<M, S>,
}

impl<M: MetaStorage, S: MessageStorage> Topic<M, S> {
    pub async fn new(
        id: u64,
        topic: &str,
        send_tx: mpsc::Sender<SendEvent>,
        meta_storage: M,
        message_storage: S,
        token: CancellationToken,
    ) -> Result<Self> {
        let storage = TopicStorage::new(topic, meta_storage.clone(), message_storage.clone())?;

        let loaded_subscriptions = storage.all_aubscriptions().await?;
        let mut subscriptions = HashMap::with_capacity(loaded_subscriptions.len());
        for sub in loaded_subscriptions {
            let subscription = Subscription::new(
                id,
                &sub.topic,
                &sub.name,
                send_tx.clone(),
                meta_storage.clone(),
                message_storage.clone(),
                sub.init_position,
                token.child_token(),
            )
            .await?;
            subscriptions.insert(sub.name, subscription);
        }
        Ok(Self {
            name: topic.to_string(),
            subscriptions,
            storage,
            producers: TopicProducers(None),
            id,
        })
    }

    pub async fn add_subscription(
        &mut self,
        topic_id: u64,
        client_id: u64,
        consumer_id: u64,
        sub: &Subscribe,
        send_tx: mpsc::Sender<SendEvent>,
        token: CancellationToken,
    ) -> Result<()> {
        self.storage
            .add_subscription(&SubscriptionInfo {
                topic: self.name.clone(),
                name: sub.sub_name.clone(),
                sub_type: sub.sub_type,
                init_position: sub.initial_position,
            })
            .await?;
        let sp = Subscription::from_subscribe(
            topic_id,
            client_id,
            consumer_id,
            sub,
            send_tx,
            self.storage.inner_meta(),
            self.storage.inner_message(),
            token,
        )
        .await?;
        self.subscriptions.insert(sub.sub_name.clone(), sp);
        Ok(())
    }

    pub async fn del_subscription(&mut self, sub_name: &str) -> Result<Option<Subscription<M, S>>> {
        self.storage.del_subscription(sub_name).await?;
        Ok(self.subscriptions.remove(sub_name))
    }

    pub fn get_subscription(&self, sub_name: &str) -> Option<&Subscription<M, S>> {
        self.subscriptions.get(sub_name)
    }

    pub async fn add_messages(&mut self, message: &Publish) -> Result<()> {
        let Some(producer_name) = self.producers.get_producer_name(message.producer_id) else {
            return Err(Error::Response(ReturnCode::ProducerNotFound));
        };
        let sequence_id = self
            .storage
            .get_sequence_id(&producer_name)
            .await?
            .unwrap_or_default();
        if message.start_seq_id < sequence_id {
            return Err(Error::Response(ReturnCode::ProduceMessageDuplicated));
        }
        let mut sequence_id = message.start_seq_id;
        trace!("topic add {} new messages", message.payloads.len());
        for payload in &message.payloads {
            let message_id = MessageId {
                topic_id: self.id,
                cursor_id: self.storage.get_new_cursor_id().await?,
            };
            let topic_message = TopicMessage::new(
                &self.name,
                message_id,
                sequence_id,
                payload.clone(),
                message.produce_time,
            );
            self.storage.add_message(&topic_message).await?;
            for sub in self.subscriptions.values() {
                sub.add_message(message_id.cursor_id).await?;
            }
            sequence_id += 1;
        }
        self.storage
            .set_sequence_id(&producer_name, sequence_id)
            .await?;
        for sub in self.subscriptions.values() {
            sub.message_notify();
        }
        Ok(())
    }

    pub async fn get_message(&self, message_id: &MessageId) -> Result<Option<TopicMessage>> {
        Ok(self.storage.get_message(message_id).await?)
    }

    pub async fn consume_ack<T>(&mut self, sub_name: &str, message_ids: T) -> Result<()>
    where
        T: Borrow<[MessageId]>,
    {
        let Some(sp) = self.subscriptions.get(sub_name) else {
            return Ok(());
        };

        // ack
        let ids = message_ids
            .borrow()
            .iter()
            .map(|m| m.cursor_id)
            .collect::<Vec<u64>>();
        sp.consume_ack(ids).await?;

        for id in message_ids.borrow() {
            self.storage.del_message(id).await?;
        }
        Ok(())
    }

    pub async fn get_producer(&self, producer_name: &str) -> Option<u64> {
        self.producers.get_producer_id(producer_name)
    }

    pub async fn add_producer(
        &mut self,
        producer_id: u64,
        producer_name: &str,
        access_mode: AccessMode,
    ) -> Result<u64> {
        let sequence_id = self
            .storage
            .get_sequence_id(producer_name)
            .await?
            .unwrap_or_default();
        self.producers.add_producer(&Producer {
            id: producer_id,
            name: producer_name.to_string(),
            access_mode,
            sequence_id,
        })?;
        Ok(sequence_id)
    }

    pub fn del_producer(&mut self, producer_id: u64) {
        self.producers.del_producer(producer_id)
    }
}
