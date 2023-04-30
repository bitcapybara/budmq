use std::{collections::HashMap, fmt::Display, sync::Arc};

use bud_common::{
    helper::wait,
    protocol::{self, Packet, PacketType, ReturnCode, Send, Unsubscribe},
    storage::Storage,
};
use futures::future;
use log::{debug, error, trace};
use tokio::{
    select,
    sync::{mpsc, oneshot, RwLock},
    time::timeout,
};
use tokio_util::sync::CancellationToken;

use crate::{
    client,
    subscription::{self, SendEvent, Subscription},
    topic::{self, Message, Topic},
    WAIT_REPLY_TIMEOUT,
};

type Result<T> = std::result::Result<T, Error>;

#[derive(Debug)]
pub enum Error {
    ReplyChannelClosed,
    SendOnDroppedChannel,
    ConsumerDuplicateSubscribed,
    ReturnCode(ReturnCode),
    Subscription(subscription::Error),
    Topic(topic::Error),
    UnsupportedPacket(PacketType),
    /// do not throw
    Internal(String),
}

impl std::error::Error for Error {}

impl Display for Error {
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

/// messages from client to broker
pub struct ClientMessage {
    pub client_id: u64,
    pub packet: protocol::Packet,
    pub res_tx: Option<oneshot::Sender<ReturnCode>>,
    pub client_tx: Option<mpsc::UnboundedSender<BrokerMessage>>,
}

/// messages from broker to client
pub struct BrokerMessage {
    pub packet: protocol::Packet,
    pub res_tx: Option<oneshot::Sender<client::Result<()>>>,
}

struct SubInfo {
    sub_name: String,
    topic_name: String,
}

/// One session per client
/// Save a client's connection information
struct Session {
    /// send to client
    client_tx: mpsc::UnboundedSender<BrokerMessage>,
    /// key = consumer_id, value = sub_info
    consumers: HashMap<u64, SubInfo>,
}

impl Session {
    fn has_consumer(&self, consumer_id: u64) -> bool {
        self.consumers.contains_key(&consumer_id)
    }

    fn add_consumer(&mut self, consumer_id: u64, sub_name: &str, topic_name: &str) {
        self.consumers.insert(
            consumer_id,
            SubInfo {
                sub_name: sub_name.to_string(),
                topic_name: topic_name.to_string(),
            },
        );
    }

    fn del_consumer(&mut self, consumer_id: u64) {
        self.consumers.remove(&consumer_id);
    }

    fn consumers(self) -> HashMap<u64, SubInfo> {
        self.consumers
    }
}

#[derive(Clone)]
pub struct Broker<S> {
    /// storage
    storage: S,
    /// key = client_id
    clients: Arc<RwLock<HashMap<u64, Session>>>,
    /// key = topic
    topics: Arc<RwLock<HashMap<String, Topic<S>>>>,
}

impl<S: Storage> Broker<S> {
    pub fn new(storage: S) -> Self {
        Self {
            storage,
            clients: Arc::new(RwLock::new(HashMap::new())),
            topics: Arc::new(RwLock::new(HashMap::new())),
        }
    }

    pub async fn run(
        self,
        broker_rx: mpsc::UnboundedReceiver<ClientMessage>,
        token: CancellationToken,
    ) {
        let task_token = token.child_token();
        // subscription task
        let (send_tx, send_rx) = mpsc::unbounded_channel();
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
        token.cancel();
    }

    /// process send event from all subscriptions
    async fn handle_send_message(
        self,
        mut send_rx: mpsc::UnboundedReceiver<SendEvent>,
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
        debug!("broker::process_send_event: send packet to consumer: {event}");
        let topics = self.topics.read().await;
        let Some(topic) = topics.get(&event.topic_name) else {
            return Err(Error::Internal(format!("topic {} not found", event.topic_name)));
        };
        let Some(message) = topic.get_message(event.message_id).await? else {
            return Err(Error::Internal(format!("message {} not found", event.message_id)));
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
            }),
            res_tx: Some(res_tx),
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
                    if let client::Error::Client(ReturnCode::ConsumerDuplicated) = e {
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
        send_tx: mpsc::UnboundedSender<SendEvent>,
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
        send_tx: mpsc::UnboundedSender<SendEvent>,
    ) -> Result<()> {
        let client_id = msg.client_id;
        // handshake process
        match msg.packet {
            Packet::Connect(_) => {
                trace!("broker::process_packets: broker receive CONNECT packet");
                let Some(res_tx) = msg.res_tx else {
                    return Err(Error::Internal("Connect res_tx channel not found".to_string()))
                };
                let Some(client_tx) = msg.client_tx else {
                    return Err(Error::Internal("Connect client_tx channel not found".to_string()))
                };
                let mut clients = self.clients.write().await;
                if clients.contains_key(&client_id) {
                    res_tx
                        .send(ReturnCode::AlreadyConnected)
                        .map_err(|_| Error::ReplyChannelClosed)?;
                    return Ok(());
                }
                clients.insert(
                    client_id,
                    Session {
                        client_tx,
                        consumers: HashMap::new(),
                    },
                );
                res_tx
                    .send(ReturnCode::Success)
                    .map_err(|_| Error::ReplyChannelClosed)?;
                trace!("broker::process_packets: add new client: {}", client_id);
            }
            Packet::Disconnect => {
                trace!("broker::process_packets: broker receive DISCONNECT packet");
                let mut clients = self.clients.write().await;
                let Some(session) = clients.remove(&client_id) else {
                    return Ok(())
                };
                trace!("broker::process_packets: remove client: {}", client_id);
                let consumers = session.consumers();
                let mut topics = self.topics.write().await;
                for (consumer_id, sub_info) in consumers {
                    let Some(topic) = topics.get_mut(&sub_info.topic_name) else {
                        continue;
                    };
                    let Some(sp) = topic.del_subscription(&sub_info.sub_name) else {
                        continue;
                    };
                    trace!(
                        "broker::process_packets: remove subscription from topic: {}",
                        sub_info.sub_name
                    );
                    sp.del_consumer(client_id, consumer_id).await?;
                    trace!(
                        "broker::process_packets: remove consumer from subscription: {}",
                        consumer_id
                    );
                }
            }
            _ => {
                let code = match self
                    .process_packet(send_tx.clone(), client_id, msg.packet)
                    .await
                {
                    Ok(_) => ReturnCode::Success,
                    Err(Error::ReturnCode(code)) => code,
                    Err(e) => Err(e)?,
                };
                if let Some(res_tx) = msg.res_tx {
                    res_tx.send(code).map_err(|_| Error::ReplyChannelClosed)?;
                }
            }
        }
        Ok(())
    }

    /// process packets from client
    /// DO NOT BLOCK!!!
    async fn process_packet(
        &self,
        send_tx: mpsc::UnboundedSender<SendEvent>,
        client_id: u64,
        packet: Packet,
    ) -> Result<()> {
        match packet {
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
                            trace!("broker::process_peckets: create new subscription");
                            let sp = Subscription::from_subscribe(
                                client_id,
                                sub.consumer_id,
                                &sub,
                                send_tx.clone(),
                                self.storage.clone(),
                            )
                            .await?;
                            trace!("broker::process_packets: add consumer to subscription");
                            sp.add_consumer(client_id, &sub).await?;
                            trace!("broker::process_packets: add subscription to topic");
                            topic.add_subscription(sp);
                        }
                    },
                    None => {
                        trace!("broker::process_packets: create new topic");
                        let mut topic =
                            Topic::new(&sub.topic, send_tx.clone(), self.storage.clone()).await?;
                        trace!("broker::process_packets: create subscription");
                        let sp = Subscription::from_subscribe(
                            client_id,
                            sub.consumer_id,
                            &sub,
                            send_tx.clone(),
                            self.storage.clone(),
                        )
                        .await?;
                        trace!("broker::process_packets: add subscription to topic");
                        topic.add_subscription(sp);
                        topics.insert(sub.topic.clone(), topic);
                    }
                }
                // add consumer to session
                trace!("broker::process_packets: add consumer to session");
                session.add_consumer(sub.consumer_id, &sub.sub_name, &sub.topic);
            }
            Packet::Unsubscribe(Unsubscribe { consumer_id }) => {
                trace!("broker::process_packets: receive UNSUBSCRIBE packet");
                let mut clients = self.clients.write().await;
                let Some(session) = clients.get_mut(&client_id) else {
                    return Ok(());
                };
                let Some(info) = session.consumers.get(&consumer_id) else {
                    return Ok(());
                };
                let mut topics = self.topics.write().await;
                if let Some(tp) = topics.get_mut(&info.topic_name) {
                    trace!("broker::process_packets: remove subscription from topic");
                    tp.del_subscription(&info.sub_name);
                }
                trace!("broker::process_packets: remove consumer from session");
                session.del_consumer(consumer_id);
            }
            Packet::Publish(p) => {
                trace!("broker::process_packets: receive PUBLISH packet");
                // add to topic
                let mut topics = self.topics.write().await;
                let topic = p.topic.clone();
                let message = Message::from_publish(p);
                match topics.get_mut(&topic) {
                    Some(topic) => {
                        trace!("broker::process_packets: add message to topic");
                        topic.add_message(message).await?;
                    }
                    None => return Err(Error::ReturnCode(ReturnCode::TopicNotExists)),
                }
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
                sp.additional_permits(client_id, c.consumer_id, c.permits)?;
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
                topic.consume_ack(&info.sub_name, c.message_id).await?;
            }
            p => return Err(Error::UnsupportedPacket(p.packet_type())),
        }
        Ok(())
    }
}
