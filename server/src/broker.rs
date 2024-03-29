use std::{collections::HashMap, sync::Arc};

use bud_common::{
    helper::wait,
    id::SerialId,
    protocol::{
        self, topic::LookupTopicResponse, CloseConsumer, CloseProducer, Connect, CreateProducer,
        Packet, PacketType, ProducerReceipt, ReturnCode, Send, Unsubscribe,
    },
    storage::{MessageStorage, MetaStorage},
    types::BrokerAddress,
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

#[derive(Debug, thiserror::Error)]
pub enum Error {
    /// Error from Subscription
    #[error("Subscription error: {0}")]
    Subscription(#[from] subscription::Error),
    /// Error from Topic
    #[error("Topic error: {0}")]
    Topic(#[from] topic::Error),
    /// Error from Storage
    #[error("Broker storage error: {0}")]
    BrokerStorage(#[from] storage::Error),
    /// Received unsupported packet
    #[error("Unsupported packet: {0}")]
    UnsupportedPacket(PacketType),
}

macro_rules! err_conv {
    ($path: ident, $stmt: expr) => {
        if let Err(e) = $stmt {
            match e {
                $path::Error::Response(code) => return Ok(Packet::err_response(code)),
                e => Err(e)?,
            }
        }
    };
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
#[derive(Clone)]
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

    fn del_producer(&mut self, producer_id: u64) -> Option<ProducerInfo> {
        self.producers.remove(&producer_id)
    }
}

struct BrokerState<M, S> {
    /// key = client_id
    clients: HashMap<u64, Session>,
    /// key = topic
    topics: HashMap<String, Topic<M, S>>,
}

#[derive(Clone)]
pub struct Broker<M, S> {
    /// broker addr
    addr: BrokerAddress,
    /// meta storage
    broker_storage: BrokerStorage<M>,
    /// topic storage
    message_storage: S,
    /// request id
    request_id: SerialId,
    /// broker state
    state: Arc<RwLock<BrokerState<M, S>>>,
    /// token
    token: CancellationToken,
}

impl<M: MetaStorage, S: MessageStorage> Broker<M, S> {
    pub fn new(
        addr: &BrokerAddress,
        meta_storage: M,
        message_storage: S,
        token: CancellationToken,
    ) -> Self {
        Self {
            broker_storage: BrokerStorage::new(meta_storage),
            state: Arc::new(RwLock::new(BrokerState {
                clients: HashMap::new(),
                topics: HashMap::new(),
            })),
            token,
            request_id: SerialId::new(),
            message_storage,
            addr: addr.clone(),
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
            wait(
                send_handle,
                "broker handle send message",
                task_token.clone(),
            ),
            wait(
                client_handle,
                "broker handle client message",
                task_token.clone(),
            ),
        )
        .await;
        let state = self.state.read().await;
        for topic_name in state.topics.keys() {
            if let Err(e) = self
                .broker_storage
                .unregister_topic(topic_name, &self.addr)
                .await
            {
                error!("broker unregister topic error: {e}");
            }
        }
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
        let state = self.state.read().await;
        let Some(session) = state.clients.get(&event.client_id) else {
            error!("Send packet error: client {} not found", &event.client_id);
            return Ok(());
        };
        let Some(topic) = state.topics.get(&event.topic_name) else {
            error!("Send packet error: topic {} not found", &event.topic_name);
            return Ok(());
        };
        let Some(message) = topic.get_message(&event.message_id).await? else {
            error!(
                "Send packet error: message {:?} not found",
                &event.message_id
            );
            return Ok(());
        };
        trace!(
            "broker::process_send_event: send to client: {}",
            event.client_id
        );
        let (res_tx, res_rx) = oneshot::channel();
        let message = BrokerMessage {
            packet: Packet::Send(Send {
                message_id: event.message_id,
                consumer_id: event.consumer_id,
                payload: message.payload,
                produce_time: message.produce_time,
                send_time: Utc::now(),
            }),
            res_tx,
        };
        if session.client_tx.send(message).is_err() {
            error!("Send message to client channel dropped")
        }
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
                    error!("send SEND packet to client error: {e}")
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
                    error!("Connect res_tx channel not found");
                    return Ok(());
                };
                let Some(client_tx) = msg.client_tx else {
                    error!("Connect client_tx channel not found");
                    return Ok(());
                };
                let mut state = self.state.write().await;
                if state.clients.contains_key(&client_id) {
                    let packet = Packet::err_response(ReturnCode::AlreadyConnected);
                    res_tx.send(packet).ok();
                    return Ok(());
                }
                state.clients.insert(
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
                trace!("broker::process_packets: broker receive DISCONNECT packet");
                let mut state = self.state.write().await;
                let Some(mut session) = state.clients.remove(&client_id) else {
                    return Ok(());
                };
                trace!("broker::process_packets: remove client: {}", client_id);
                // remove consumer
                for (consumer_id, consumer_info) in session.consumers.drain() {
                    let Some(topic) = state.topics.get(&consumer_info.topic_name) else {
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
                    let Some(topic) = state.topics.get_mut(&producer_info.topic_name) else {
                        continue;
                    };
                    topic.del_producer(producer_id);
                }
            }
            Packet::CloseProducer(CloseProducer { producer_id }) => {
                trace!("broker::process_packets: broker receive CLOSE_PRODUCEDR packet");
                let mut state = self.state.write().await;
                let Some(session) = state.clients.get_mut(&client_id) else {
                    return Ok(());
                };
                let Some(producer_info) = session.del_producer(producer_id) else {
                    return Ok(());
                };
                session.del_producer(producer_id);
                let Some(topic) = state.topics.get_mut(&producer_info.topic_name) else {
                    return Ok(());
                };
                topic.del_producer(producer_id);
                trace!("broker::process_packets: broker delete producer: {producer_id}");
            }
            Packet::CloseConsumer(CloseConsumer { consumer_id }) => {
                trace!("broker::process_packets: broker receive CLOSE_CONSUMER packet");
                let mut state = self.state.write().await;
                let Some(session) = state.clients.get_mut(&client_id) else {
                    return Ok(());
                };
                let Some(consumer_info) = session.get_consumer(consumer_id) else {
                    return Ok(());
                };
                session.del_consumer(consumer_id);
                let Some(topic) = state.topics.get_mut(&consumer_info.topic_name) else {
                    return Ok(());
                };
                let Some(subscription) = topic.get_subscription(&consumer_info.sub_name) else {
                    return Ok(());
                };
                subscription.del_consumer(client_id, consumer_id).await?;
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
                trace!("broker::process_packets: receive CREATE_PRODUCER packet");
                let mut state = self.state.write().await;
                let Some(session) = state.clients.get_mut(&client_id) else {
                    return Ok(Packet::err_response(ReturnCode::NotConnected));
                };
                if session.has_producer(producer_id) {
                    return Ok(Packet::err_response(ReturnCode::ProducerDuplicated));
                }
                session.add_producer(producer_id, &topic_name);
                let sequence_id = match state.topics.get_mut(&topic_name) {
                    Some(topic) => {
                        if topic.get_producer(&producer_name).await.is_some() {
                            return Ok(Packet::err_response(ReturnCode::ProducerDuplicated));
                        }
                        match topic
                            .add_producer(producer_id, &producer_name, access_mode)
                            .await
                        {
                            Ok(sequence_id) => sequence_id,
                            Err(topic::Error::Response(code)) => {
                                return Ok(Packet::err_response(code))
                            }
                            Err(e) => Err(e)?,
                        }
                    }
                    None => {
                        let topic_id = self
                            .broker_storage
                            .get_or_create_topic_id(&topic_name)
                            .await?;
                        let mut topic = Topic::new(
                            topic_id,
                            &topic_name,
                            send_tx.clone(),
                            self.broker_storage.inner(),
                            self.message_storage.clone(),
                            self.token.child_token(),
                        )
                        .await?;
                        self.broker_storage
                            .register_topic(&topic_name, &self.addr)
                            .await?;
                        let sequence_id = match topic
                            .add_producer(producer_id, &producer_name, access_mode)
                            .await
                        {
                            Ok(sequence_id) => sequence_id,
                            Err(topic::Error::Response(code)) => {
                                return Ok(Packet::err_response(code))
                            }
                            Err(e) => Err(e)?,
                        };
                        state.topics.insert(topic_name.clone(), topic);
                        sequence_id
                    }
                };
                self.broker_storage
                    .register_topic(&topic_name, &self.addr)
                    .await?;
                Ok(Packet::ProducerReceipt(ProducerReceipt { sequence_id }))
            }
            Packet::Subscribe(sub) => {
                trace!("broker::process_packets: receive SUBSCRIBE packet");
                let mut state = self.state.write().await;
                let Some(session) = state.clients.get_mut(&client_id) else {
                    return Ok(Packet::err_response(ReturnCode::NotConnected));
                };
                if session.consumers.contains_key(&sub.consumer_id) {
                    return Ok(Packet::err_response(ReturnCode::ConsumerDuplicated));
                }
                trace!("broker::process_packets: add consumer to session");
                session.add_consumer(sub.consumer_id, &sub.sub_name, &sub.topic);
                // add consumer to session
                // add subscription into topic
                match state.topics.get_mut(&sub.topic) {
                    Some(topic) => match topic.get_subscription(&sub.sub_name) {
                        Some(sp) => {
                            trace!("broker::process_packets: add consumer to subscription");
                            err_conv!(subscription, sp.add_consumer(client_id, &sub).await);
                        }
                        None => {
                            trace!("broker::process_packets: add subscription to topic");
                            if let Err(e) = topic
                                .add_subscription(
                                    topic.id,
                                    client_id,
                                    sub.consumer_id,
                                    &sub,
                                    send_tx.clone(),
                                    self.token.child_token(),
                                )
                                .await
                            {
                                match e {
                                    topic::Error::Response(code) => {
                                        return Ok(Packet::err_response(code))
                                    }
                                    e => Err(e)?,
                                }
                            }
                        }
                    },
                    None => {
                        trace!("broker::process_packets: create new topic");
                        let topic_id = self
                            .broker_storage
                            .get_or_create_topic_id(&sub.topic)
                            .await?;
                        let mut topic = Topic::new(
                            topic_id,
                            &sub.topic,
                            send_tx.clone(),
                            self.broker_storage.inner(),
                            self.message_storage.clone(),
                            self.token.child_token(),
                        )
                        .await?;
                        trace!("broker::process_packets: add subscription to topic");
                        err_conv!(
                            topic,
                            topic
                                .add_subscription(
                                    topic_id,
                                    client_id,
                                    sub.consumer_id,
                                    &sub,
                                    send_tx.clone(),
                                    self.token.child_token(),
                                )
                                .await
                        );
                        state.topics.insert(sub.topic.clone(), topic);
                    }
                }
                self.broker_storage
                    .register_topic(&sub.topic, &self.addr)
                    .await?;
                Ok(Packet::ok_response())
            }
            Packet::Unsubscribe(Unsubscribe { consumer_id }) => {
                let mut state = self.state.write().await;
                trace!("broker::process_packets: receive UNSUBSCRIBE packet");
                let Some(session) = state.clients.get_mut(&client_id) else {
                    return Ok(Packet::ok_response());
                };
                trace!("broker::process_packets: remove consumer from session");
                session.del_consumer(consumer_id);
                let Some(info) = session.consumers.get(&consumer_id).cloned() else {
                    return Ok(Packet::ok_response());
                };
                if let Some(tp) = state.topics.get_mut(&info.topic_name) {
                    trace!("broker::process_packets: remove subscription from topic");
                    tp.del_subscription(&info.sub_name).await?;
                }
                Ok(Packet::ok_response())
            }
            Packet::Publish(p) => {
                let mut state = self.state.write().await;
                trace!("broker::process_packets: receive PUBLISH packet");
                let Some(session) = state.clients.get(&client_id) else {
                    return Ok(Packet::err_response(ReturnCode::NotConnected));
                };
                if session.get_producer(p.producer_id).is_none() {
                    return Ok(Packet::err_response(ReturnCode::ProducerNotFound));
                }
                let topic = p.topic.clone();
                match state.topics.get_mut(&topic) {
                    Some(topic) => {
                        trace!("broker::process_packets: add message to topic");
                        err_conv!(topic, topic.add_messages(&p).await);
                        Ok(Packet::ok_response())
                    }
                    None => Ok(Packet::err_response(ReturnCode::TopicNotExists)),
                }
            }
            Packet::ControlFlow(c) => {
                let state = self.state.read().await;
                trace!("broker::process_packets: receive CONTROLFLOW packet");
                // add permits to subscription
                let Some(session) = state.clients.get(&client_id) else {
                    return Ok(Packet::err_response(ReturnCode::NotConnected));
                };
                let Some(info) = session.consumers.get(&c.consumer_id) else {
                    return Ok(Packet::err_response(ReturnCode::ConsumerNotFound));
                };
                let Some(topic) = state.topics.get(&info.topic_name) else {
                    return Ok(Packet::err_response(ReturnCode::TopicNotExists));
                };
                let Some(sp) = topic.get_subscription(&info.sub_name) else {
                    return Ok(Packet::err_response(ReturnCode::SubscriptionNotFound));
                };
                trace!("broker::process_packets: add permits to subscription");
                sp.additional_permits(client_id, c.consumer_id, c.permits)
                    .await;
                Ok(Packet::ok_response())
            }
            Packet::ConsumeAck(c) => {
                trace!("broker::process_packets: receive CONSUMEACK packet");
                let state = self.state.read().await;
                let Some(session) = state.clients.get(&client_id) else {
                    return Ok(Packet::err_response(ReturnCode::NotConnected));
                };
                let Some(info) = session.consumers.get(&c.consumer_id) else {
                    return Ok(Packet::err_response(ReturnCode::ConsumerNotFound));
                };
                let Some(topic) = state.topics.get(&info.topic_name) else {
                    return Ok(Packet::err_response(ReturnCode::TopicNotExists));
                };
                for message_id in &c.message_ids {
                    if topic.id != message_id.topic_id {
                        return Ok(Packet::err_response(ReturnCode::AckTopicMissMatch));
                    }
                }
                trace!("broker::process_packets: ack message in topic");
                err_conv!(
                    topic,
                    topic.consume_ack(&info.sub_name, c.message_ids).await
                );
                Ok(Packet::ok_response())
            }
            Packet::LookupTopic(p) => {
                trace!("broker::process_packets: receive LOOKUP_TOPIC packet");
                match self
                    .broker_storage
                    .get_topic_broker_addr(&p.topic_name)
                    .await?
                {
                    Some(addr) => Ok(Packet::LookupTopicResponse(LookupTopicResponse {
                        broker_addr: addr.socket_addr.to_string(),
                        server_name: addr.server_name,
                    })),
                    None => Ok(Packet::err_response(ReturnCode::TopicNotExists)),
                }
            }
            p => Err(Error::UnsupportedPacket(p.packet_type())),
        }
    }
}
