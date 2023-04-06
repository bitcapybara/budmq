use std::{collections::HashMap, fmt::Display, sync::Arc};

use libbud_common::protocol::{self, Packet, ReturnCode, Send, Unsubscribe};
use log::error;
use tokio::{
    select,
    sync::{mpsc, oneshot, watch, RwLock},
    time::timeout,
};

use crate::{
    client,
    helper::wait,
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
pub struct Broker {
    /// key = client_id
    clients: Arc<RwLock<HashMap<u64, Session>>>,
    /// key = topic
    topics: Arc<RwLock<HashMap<String, Topic>>>,
}

impl Broker {
    pub fn new() -> Self {
        Self {
            clients: Arc::new(RwLock::new(HashMap::new())),
            topics: Arc::new(RwLock::new(HashMap::new())),
        }
    }

    pub async fn run(
        self,
        broker_rx: mpsc::UnboundedReceiver<ClientMessage>,
        close_rx: watch::Receiver<()>,
    ) {
        // subscription task
        let (send_tx, send_rx) = mpsc::unbounded_channel();
        let sub_task = self.clone().receive_subscription(send_rx, close_rx.clone());
        let sub_handle = tokio::spawn(sub_task);

        // client task
        let client_task = self
            .clone()
            .receive_client(send_tx, broker_rx, close_rx.clone());
        let client_handle = tokio::spawn(client_task);

        wait(sub_handle, "broker subscription").await;
        wait(client_handle, "broker client").await;
    }

    /// process send event from all subscriptions
    async fn receive_subscription(
        self,
        mut send_rx: mpsc::UnboundedReceiver<SendEvent>,
        mut close_rx: watch::Receiver<()>,
    ) -> Result<()> {
        loop {
            select! {
                event = send_rx.recv() => {
                    let Some(event) = event else {
                        return Ok(())
                    };
                    self.process_send_event(event).await?;
                }
                _ = close_rx.changed() => {
                    return Ok(())
                }
            }
        }
    }

    async fn process_send_event(&self, event: SendEvent) -> Result<()> {
        let topics = self.topics.read().await;
        let Some(topic) = topics.get(&event.topic_name) else {
                unreachable!()
            };
        let Some(message) = topic.get_message(event.message_id).await? else {
                unreachable!()
            };
        let clients = self.clients.read().await;
        if let Some(session) = clients.get(&event.client_id) {
            let (res_tx, res_rx) = oneshot::channel();
            session.client_tx.send(BrokerMessage {
                packet: Packet::Send(Send {
                    consumer_id: event.consumer_id,
                    payload: message.payload,
                }),
                res_tx: Some(res_tx),
            })?;
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
        }
        Ok(())
    }

    async fn receive_client(
        self,
        send_tx: mpsc::UnboundedSender<SendEvent>,
        mut broker_rx: mpsc::UnboundedReceiver<ClientMessage>,
        mut close_rx: watch::Receiver<()>,
    ) -> Result<()> {
        loop {
            select! {
                msg = broker_rx.recv() => {
                    let Some(msg) = msg else {
                        return Ok(())
                    };
                    self.process_packets(msg, send_tx.clone()).await?;
                }
                _ = close_rx.changed() => {
                    return Ok(())
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
                let Some(res_tx) = msg.res_tx else {
                    unreachable!()
                };
                let Some(client_tx) = msg.client_tx else {
                    unreachable!()
                };
                let mut clients = self.clients.write().await;
                if clients.contains_key(&client_id) {
                    res_tx
                        .send(ReturnCode::AlreadyConnected)
                        .map_err(|_| Error::ReplyChannelClosed)?;
                }
                clients.insert(
                    client_id,
                    Session {
                        client_tx,
                        consumers: HashMap::new(),
                    },
                );
            }
            Packet::Disconnect => {
                let mut clients = self.clients.write().await;
                let Some(session) = clients.remove(&client_id) else {
                    return Ok(())
                    };
                let consumers = session.consumers();
                let mut topics = self.topics.write().await;
                for (consumer_id, sub_info) in consumers {
                    let Some(mut topic) = topics.remove(&sub_info.topic_name) else {
                        continue;
                    };
                    let Some(sp) = topic.del_subscription(&sub_info.sub_name) else {
                        continue;
                    };
                    sp.del_consumer(client_id, consumer_id).await?;
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
                            sp.add_consumer(client_id, &sub).await?;
                        }
                        None => {
                            let sp = Subscription::from_subscribe(
                                client_id,
                                sub.consumer_id,
                                &sub,
                                send_tx.clone(),
                            )
                            .await?;
                            sp.add_consumer(client_id, &sub).await?;
                            topic.add_subscription(sp);
                        }
                    },
                    None => {
                        let mut topic = Topic::new(&sub.topic, send_tx.clone()).await?;
                        let sp = Subscription::from_subscribe(
                            client_id,
                            sub.consumer_id,
                            &sub,
                            send_tx.clone(),
                        )
                        .await?;
                        topic.add_subscription(sp);
                        topics.insert(sub.topic.clone(), topic);
                    }
                }
                // add consumer to session
                session.add_consumer(sub.consumer_id, &sub.sub_name, &sub.topic);
            }
            Packet::Unsubscribe(Unsubscribe { consumer_id }) => {
                let mut clients = self.clients.write().await;
                let Some(session) = clients.get_mut(&client_id) else {
                    return Ok(());
                };
                let Some(info) = session.consumers.get(&consumer_id) else {
                    return Ok(());
                };
                let mut topics = self.topics.write().await;
                if let Some(tp) = topics.get_mut(&info.topic_name) {
                    tp.del_subscription(&info.sub_name);
                }
                session.del_consumer(consumer_id);
            }
            Packet::Publish(p) => {
                // add to topic
                let mut topics = self.topics.write().await;
                let topic = p.topic.clone();
                let message = Message::from_publish(p);
                match topics.get_mut(&topic) {
                    Some(topic) => {
                        topic.add_message(message).await?;
                    }
                    None => return Err(Error::ReturnCode(ReturnCode::TopicNotExists)),
                }
            }
            Packet::ControlFlow(c) => {
                // TODO simplify code
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
                    unreachable!()
                };
                let Some(sp) = topic.get_subscription(&info.sub_name) else {
                    unreachable!()
                };
                sp.additional_permits(client_id, c.consumer_id, c.permits)?;
            }
            Packet::ConsumeAck(c) => {
                // TODO simplify code
                let clients = self.clients.read().await;
                let Some(session) = clients.get(&client_id) else {
                    return Err(Error::ReturnCode(ReturnCode::NotConnected))
                };
                let Some(info) = session.consumers.get(&c.consumer_id) else {
                    return Err(Error::ReturnCode(ReturnCode::ConsumerNotFound))
                };
                let mut topics = self.topics.write().await;
                let Some(topic) = topics.get_mut(&info.topic_name) else {
                    unreachable!()
                };
                topic.consume_ack(&info.sub_name, c.message_id).await?;
            }
            _ => unreachable!(),
        }
        Ok(())
    }
}