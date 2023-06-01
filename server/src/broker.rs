use std::{collections::HashMap, sync::Arc};

use bud_common::{
    helper::wait,
    id::SerialId,
    protocol::{
        self, CloseConsumer, CloseProducer, Connect, CreateProducer, Packet, PacketType,
        ProducerReceipt, ReturnCode, Send, Unsubscribe,
    },
    storage::Storage,
};
use chrono::Utc;
use futures::future;
use log::{error, trace};
use tokio::{
    select,
    sync::{mpsc, oneshot, RwLock},
    time::timeout,
};
use tokio_util::sync::CancellationToken;

use crate::{
    storage::{self, broker::BrokerStorage},
    subscription::{self, SendEvent},
    topic::{self, Topic},
    WAIT_REPLY_TIMEOUT,
};

const MAX_TOPIC_ID: u64 = 1024;

type Result<T> = std::result::Result<T, Error>;

#[derive(Debug)]
pub enum Error {
    ReplyChannelClosed,
    SendOnDroppedChannel,
    ConsumerDuplicateSubscribed,
    ProducerDuplicated,
    ReturnCode(ReturnCode),
    Subscription(subscription::Error),
    Topic(topic::Error),
    BrokerStorage(storage::Error),
    UnsupportedPacket(PacketType),
    /// do not throw
    Internal(String),
}

impl std::error::Error for Error {}

impl std::fmt::Display for Error {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Error::ReplyChannelClosed => write!(f, "Reply channel closed"),
            Error::SendOnDroppedChannel => write!(f, "Send on drrpped channel"),
            Error::ConsumerDuplicateSubscribed => write!(f, "Consumer duplicate subscribe"),
            Error::ReturnCode(c) => write!(f, "Receive ReturnCode {c}"),
            Error::Subscription(e) => write!(f, "Subscription error: {e}"),
            Error::Topic(e) => write!(f, "Topic error: {e}"),
            Error::Internal(s) => write!(f, "Internal error: {s}"),
            Error::UnsupportedPacket(t) => write!(f, "Unsupported packet type: {t:?}"),
            Error::ProducerDuplicated => write!(f, "Producer duplicated"),
            Error::BrokerStorage(e) => write!(f, "broker storage error: {e}"),
        }
    }
}

impl From<subscription::Error> for Error {
    fn from(e: subscription::Error) -> Self {
        Self::Subscription(e)
    }
}

impl From<topic::Error> for Error {
    fn from(e: topic::Error) -> Self {
        Self::Topic(e)
    }
}

impl<T> From<mpsc::error::SendError<T>> for Error {
    fn from(_: mpsc::error::SendError<T>) -> Self {
        Self::SendOnDroppedChannel
    }
}

impl From<storage::Error> for Error {
    fn from(e: storage::Error) -> Self {
        Self::BrokerStorage(e)
    }
}

/// messages from client to broker
pub struct ClientMessage {
    pub client_id: u64,
    pub packet: protocol::Packet,
    pub res_tx: Option<oneshot::Sender<Packet>>,
    pub client_tx: Option<mpsc::UnboundedSender<BrokerMessage>>,
}

/// messages from broker to client
pub struct BrokerMessage {
    pub packet: protocol::Packet,
    pub res_tx: oneshot::Sender<bud_common::io::Result<Packet>>,
}

#[derive(Clone)]
struct ConsumerInfo {
    sub_name: String,
    topic_name: String,
}

#[derive(Clone)]
struct ProducerInfo {
    topic_name: String,
}

/// One session per client
/// Save a client's connection information
struct Session {
    /// send to client
    client_tx: mpsc::UnboundedSender<BrokerMessage>,
    /// consumer_id -> consumer_info
    consumers: HashMap<u64, ConsumerInfo>,
    /// producer_id -> producer_info
    producers: HashMap<u64, ProducerInfo>,
}

impl Session {
    fn has_consumer(&self, consumer_id: u64) -> bool {
        self.consumers.contains_key(&consumer_id)
    }

    fn has_producer(&self, producer_id: u64) -> bool {
        self.producers.contains_key(&producer_id)
    }

    fn get_consumer(&self, consumer_id: u64) -> Option<ConsumerInfo> {
        self.consumers.get(&consumer_id).cloned()
    }

    fn add_consumer(&mut self, consumer_id: u64, sub_name: &str, topic_name: &str) {
        self.consumers.insert(
            consumer_id,
            ConsumerInfo {
                sub_name: sub_name.to_string(),
                topic_name: topic_name.to_string(),
            },
        );
    }

    fn del_consumer(&mut self, consumer_id: u64) {
        self.consumers.remove(&consumer_id);
    }

    fn get_producer(&self, producer_id: u64) -> Option<ProducerInfo> {
        self.producers.get(&producer_id).cloned()
    }

    fn add_producer(&mut self, producer_id: u64, topic_name: &str) {
        self.producers.insert(
            producer_id,
            ProducerInfo {
                topic_name: topic_name.to_string(),
            },
        );
    }

    fn del_producer(&mut self, producer_id: u64) {
        self.producers.remove(&producer_id);
    }
}

#[derive(Clone)]
pub struct Broker<S> {
    /// storage
    storage: BrokerStorage<S>,
    /// request id
    request_id: SerialId,
    /// key = client_id
    clients: Arc<RwLock<HashMap<u64, Session>>>,
    /// key = topic
    topics: Arc<RwLock<HashMap<String, Topic<S>>>>,
    /// token
    token: CancellationToken,
}

impl<S: Storage> Broker<S> {
    pub fn new(storage: S, token: CancellationToken) -> Self {
        Self {
            storage: BrokerStorage::new(storage),
            clients: Arc::new(RwLock::new(HashMap::new())),
            topics: Arc::new(RwLock::new(HashMap::new())),
            token,
            request_id: SerialId::new(),
        }
    }

    pub async fn run(self, broker_rx: mpsc::UnboundedReceiver<ClientMessage>) {
        let task_token = self.token.child_token();
        // subscription task
        let (send_tx, send_rx) = mpsc::channel(1);
        let send_task = self
            .clone()
            .handle_send_message(send_rx, task_token.clone());
        let send_handle = tokio::spawn(send_task);
        trace!("broker::run: start handle send message task");

        // client task
        let client_task = self
            .clone()
            .receive_client(send_tx, broker_rx, task_token.clone());
        let client_handle = tokio::spawn(client_task);
        trace!("broker::run start handle client message task");
        future::join(
            wait(send_handle, "broker handle send message"),
            wait(client_handle, "broker handle client message"),
        )
        .await;
    }

    /// process send event from all subscriptions
    async fn handle_send_message(
        self,
        mut send_rx: mpsc::Receiver<SendEvent>,
        token: CancellationToken,
    ) {
        loop {
            select! {
                event = send_rx.recv() => {
                    let Some(event) = event else {
                        token.cancel();
                        return
                    };
                    trace!("broker::handle_send_message task: receive a message");
                    if let Err(e) = self.process_send_event(event).await {
                        error!("broker send message to consumer error: {e}")
                    }
                }
                _ = token.cancelled() => {
                    return
                }
            }
        }
    }

    async fn process_send_event(&self, event: SendEvent) -> Result<()> {
        trace!("broker::process_send_event: send packet to consumer: {event}");
        let topics = self.topics.read().await;
        let Some(topic) = topics.get(&event.topic_name) else {
            return Err(Error::Internal(format!("topic {} not found", event.topic_name)));
        };
        let Some(message) = topic.get_message(&event.message_id).await? else {
            return Err(Error::Internal(format!("message {:?} not found", event.message_id)));
        };
        let clients = self.clients.read().await;

        let Some(session) = clients.get(&event.client_id) else {
            return Ok(());
        };
        trace!(
            "broker::process_send_event: send to client: {}",
            event.client_id
        );
        let (res_tx, res_rx) = oneshot::channel();
        session.client_tx.send(BrokerMessage {
            packet: Packet::Send(Send {
                message_id: event.message_id,
                consumer_id: event.consumer_id,
                payload: message.payload,
                produce_time: message.produce_time,
                send_time: Utc::now(),
            }),
            res_tx,
        })?;
        trace!("broker::process_send_event: send response to subscription");
        tokio::spawn(async move {
            match timeout(WAIT_REPLY_TIMEOUT, res_rx).await {
                Ok(Ok(Ok(_))) => {
                    if event.res_tx.send(true).is_err() {
                        error!("broker reply ok to send event error")
                    }
                    return;
                }
                Ok(Ok(Err(e))) => {
                    if let bud_common::io::Error::FromPeer(ReturnCode::ConsumerDuplicated) = e {
                        if event.res_tx.send(true).is_err() {
                            error!("broker reply ok to send event error")
                        }
                        return;
                    }
                }
                Ok(Err(e)) => {
                    error!("recving SEND reply from client error: {e}")
                }
                Err(_) => {
                    error!("wait client SEND reply timout")
                }
            }
            if event.res_tx.send(false).is_err() {
                error!("broker reply non-ok to send event error")
            }
        });
        Ok(())
    }

    async fn receive_client(
        self,
        send_tx: mpsc::Sender<SendEvent>,
        mut broker_rx: mpsc::UnboundedReceiver<ClientMessage>,
        token: CancellationToken,
    ) {
        loop {
            select! {
                msg = broker_rx.recv() => {
                    let Some(msg) = msg else {
                        token.cancel();
                        return
                    };
                    trace!("broker::receive_client: receive a message from client");
                    if let Err(e) = self.process_packets(msg, send_tx.clone()).await {
                        error!("broker process client packet error: {e}")
                    }
                }
                _ = token.cancelled() => {
                    return
                }
            }
        }
    }

    async fn process_packets(
        &self,
        msg: ClientMessage,
        send_tx: mpsc::Sender<SendEvent>,
    ) -> Result<()> {
        let client_id = msg.client_id;
        // handshake process
        match msg.packet {
            Packet::Connect(Connect { .. }) => {
                trace!("broker::process_packets: broker receive CONNECT packet");
                let Some(res_tx) = msg.res_tx else {
                    return Err(Error::Internal("Connect res_tx channel not found".to_string()))
                };
                let Some(client_tx) = msg.client_tx else {
                    return Err(Error::Internal("Connect client_tx channel not found".to_string()))
                };
                let mut clients = self.clients.write().await;
                if clients.contains_key(&client_id) {
                    let packet = Packet::err_response(ReturnCode::AlreadyConnected);
                    res_tx.send(packet).ok();
                    return Ok(());
                }
                clients.insert(
                    client_id,
                    Session {
                        client_tx,
                        consumers: HashMap::new(),
                        producers: HashMap::new(),
                    },
                );
                res_tx.send(Packet::ok_response()).ok();
                trace!("broker::process_packets: add new client: {}", client_id);
            }
            Packet::Disconnect => {
                // TODO close producers and consumers
                trace!("broker::process_packets: broker receive DISCONNECT packet");
                let mut clients = self.clients.write().await;
                let Some(mut session) = clients.remove(&client_id) else {
                    return Ok(())
                };
                trace!("broker::process_packets: remove client: {}", client_id);
                // remove consumer
                let mut topics = self.topics.write().await;
                for (consumer_id, consumer_info) in session.consumers.drain() {
                    let Some(topic) = topics.get(&consumer_info.topic_name) else {
                        continue;
                    };
                    trace!(
                        "broker::process_packets: remove subscription from topic: {}",
                        consumer_info.sub_name
                    );
                    let Some(sp) = topic.get_subscription(&consumer_info.sub_name) else {
                        continue;
                    };
                    trace!(
                        "broker::process_packets: remove consumer from subscription: {}",
                        consumer_id
                    );
                    sp.del_consumer(client_id, consumer_id).await?;
                    trace!("broker::process_packets: subscription task exiting");
                    trace!("broker::process_packets: subscription task exit");
                }
                // remove producer
                for (producer_id, producer_info) in session.producers.drain() {
                    let Some(topic) = topics.get_mut(&producer_info.topic_name) else {
                        continue;
                    };
                    topic.del_producer(producer_id);
                }
            }
            Packet::CloseProducer(CloseProducer { producer_id }) => {
                let mut clients = self.clients.write().await;
                let Some(session) = clients.get_mut(&client_id) else {
                    return Err(Error::ReturnCode(ReturnCode::NotConnected));
                };
                let Some(producer_info) = session.get_producer(producer_id) else {
                    return Ok(())
                };
                let mut topics = self.topics.write().await;
                let Some(topic) = topics.get_mut(&producer_info.topic_name) else {
                    return Ok(());
                };
                topic.del_producer(producer_id);
                session.del_producer(producer_id);
            }
            Packet::CloseConsumer(CloseConsumer { consumer_id }) => {
                let mut clients = self.clients.write().await;
                let Some(session) = clients.get_mut(&client_id) else {
                    return Err(Error::ReturnCode(ReturnCode::NotConnected));
                };
                let Some(consumer_info) = session.get_consumer(consumer_id) else {
                    return Ok(());
                };
                let mut topics = self.topics.write().await;
                let Some(topic) = topics.get_mut(&consumer_info.topic_name) else {
                    return Ok(());
                };
                let Some(subscription) = topic.get_subscription(&consumer_info.sub_name) else {
                    return Ok(());
                };
                subscription.del_consumer(client_id, consumer_id).await?;
                session.del_consumer(consumer_id);
            }
            _ => {
                let packet = self
                    .process_packet(send_tx.clone(), client_id, msg.packet)
                    .await?;
                if let Some(res_tx) = msg.res_tx {
                    res_tx.send(packet).ok();
                }
            }
        }
        Ok(())
    }

    /// process packets from client
    /// DO NOT BLOCK!!!
    async fn process_packet(
        &self,
        send_tx: mpsc::Sender<SendEvent>,
        client_id: u64,
        packet: Packet,
    ) -> Result<Packet> {
        match packet {
            Packet::CreateProducer(CreateProducer {
                producer_name,
                topic_name,
                access_mode,
                producer_id,
            }) => {
                let mut clients = self.clients.write().await;
                let Some(session) = clients.get_mut(&client_id) else {
                    return Err(Error::ReturnCode(ReturnCode::NotConnected));
                };
                if session.has_producer(producer_id) {
                    return Err(Error::ProducerDuplicated);
                }
                let mut topics = self.topics.write().await;
                let sequence_id = match topics.get_mut(&topic_name) {
                    Some(topic) => {
                        if topic.get_producer(&producer_name).await.is_some() {
                            return Err(Error::ProducerDuplicated);
                        }
                        topic
                            .add_producer(producer_id, &producer_name, access_mode)
                            .await?
                    }
                    None => {
                        let topic_id = self.storage.get_or_create_topic_id(&topic_name).await?;
                        let mut topic = Topic::new(
                            topic_id,
                            &topic_name,
                            send_tx.clone(),
                            self.token.child_token(),
                        )
                        .await?;
                        let sequence_id = topic
                            .add_producer(producer_id, &producer_name, access_mode)
                            .await?;
                        topics.insert(topic_name.clone(), topic);
                        sequence_id
                    }
                };
                session.add_producer(producer_id, &topic_name);
                Ok(Packet::ProducerReceipt(ProducerReceipt {
                    producer_id,
                    sequence_id,
                }))
            }
            Packet::Subscribe(sub) => {
                trace!("broker::process_packets: receive SUBSCRIBE packet");
                let mut clients = self.clients.write().await;
                let Some(session) = clients.get_mut(&client_id) else {
                    return Err(Error::ReturnCode(ReturnCode::NotConnected));
                };
                if session.consumers.contains_key(&sub.consumer_id) {
                    return Err(Error::ReturnCode(ReturnCode::ConsumerDuplicated));
                }
                // add subscription into topic
                let mut topics = self.topics.write().await;
                match topics.get_mut(&sub.topic) {
                    Some(topic) => match topic.get_subscription(&sub.sub_name) {
                        Some(sp) => {
                            trace!("broker::process_packets: add consumer to subscription");
                            sp.add_consumer(client_id, &sub).await?;
                        }
                        None => {
                            trace!("broker::process_packets: add subscription to topic");
                            topic
                                .add_subscription(
                                    topic.id,
                                    client_id,
                                    sub.consumer_id,
                                    &sub,
                                    send_tx.clone(),
                                    sub.initial_position,
                                    self.token.child_token(),
                                )
                                .await?;
                        }
                    },
                    None => {
                        trace!("broker::process_packets: create new topic");
                        let topic_id = self.storage.get_or_create_topic_id(&sub.topic).await?;
                        let mut topic = Topic::new(
                            topic_id,
                            &sub.topic,
                            send_tx.clone(),
                            self.token.child_token(),
                        )
                        .await?;
                        trace!("broker::process_packets: add subscription to topic");
                        topic
                            .add_subscription(
                                topic_id,
                                client_id,
                                sub.consumer_id,
                                &sub,
                                send_tx.clone(),
                                sub.initial_position,
                                self.token.child_token(),
                            )
                            .await?;
                        topics.insert(sub.topic.clone(), topic);
                    }
                }
                // add consumer to session
                trace!("broker::process_packets: add consumer to session");
                session.add_consumer(sub.consumer_id, &sub.sub_name, &sub.topic);
                Ok(Packet::ok_response())
            }
            Packet::Unsubscribe(Unsubscribe { consumer_id }) => {
                trace!("broker::process_packets: receive UNSUBSCRIBE packet");
                let mut clients = self.clients.write().await;
                let Some(session) = clients.get_mut(&client_id) else {
                    return Ok(Packet::ok_response());
                };
                let Some(info) = session.consumers.get(&consumer_id) else {
                    return Ok(Packet::ok_response());
                };
                let mut topics = self.topics.write().await;
                if let Some(tp) = topics.get_mut(&info.topic_name) {
                    trace!("broker::process_packets: remove subscription from topic");
                    tp.del_subscription(&info.sub_name);
                }
                trace!("broker::process_packets: remove consumer from session");
                session.del_consumer(consumer_id);
                Ok(Packet::ok_response())
            }
            Packet::Publish(p) => {
                let clients = self.clients.read().await;
                let Some(session) = clients.get(&client_id) else {
                    return Err(Error::ReturnCode(ReturnCode::NotConnected));
                };
                if session.get_producer(p.producer_id).is_none() {
                    return Err(Error::ReturnCode(ReturnCode::ProducerNotFound));
                }
                trace!("broker::process_packets: receive PUBLISH packet");
                // add to topic
                let mut topics = self.topics.write().await;
                let topic = p.topic.clone();
                match topics.get_mut(&topic) {
                    Some(topic) => {
                        trace!("broker::process_packets: add message to topic");
                        topic.add_message(&p).await?;
                    }
                    None => return Err(Error::ReturnCode(ReturnCode::TopicNotExists)),
                }
                Ok(Packet::ok_response())
            }
            Packet::ControlFlow(c) => {
                trace!("broker::process_packets: receive CONTROLFLOW packet");
                let clients = self.clients.read().await;
                // add permits to subscription
                let Some(session) = clients.get(&client_id) else {
                    return Err(Error::ReturnCode(ReturnCode::NotConnected));
                };
                let Some(info) = session.consumers.get(&c.consumer_id) else {
                    return Err(Error::ReturnCode(ReturnCode::ConsumerNotFound));
                };
                let topics = self.topics.read().await;
                let Some(topic) = topics.get(&info.topic_name) else {
                    return Err(Error::Internal(format!("topic {} not found", info.topic_name)))
                };
                let Some(sp) = topic.get_subscription(&info.sub_name) else {
                    return Err(Error::Internal(format!("subscription {} not found", info.sub_name)))
                };
                trace!("broker::process_packets: add permits to subscription");
                sp.additional_permits(client_id, c.consumer_id, c.permits)
                    .await?;
                Ok(Packet::ok_response())
            }
            Packet::ConsumeAck(c) => {
                trace!("broker::process_packets: receive CONSUMEACK packet");
                let clients = self.clients.read().await;
                let Some(session) = clients.get(&client_id) else {
                    return Err(Error::ReturnCode(ReturnCode::NotConnected))
                };
                let Some(info) = session.consumers.get(&c.consumer_id) else {
                    return Err(Error::ReturnCode(ReturnCode::ConsumerNotFound))
                };
                let mut topics = self.topics.write().await;
                let Some(topic) = topics.get_mut(&info.topic_name) else {
                    return Err(Error::Internal(format!("topic {} not found", info.topic_name)))
                };
                trace!("broker::process_packets: ack message in topic");
                topic.consume_ack(&info.sub_name, &c.message_id).await?;
                Ok(Packet::ok_response())
            }
            p => Err(Error::UnsupportedPacket(p.packet_type())),
        }
    }
}
