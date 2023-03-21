use std::{collections::HashMap, fmt::Display, sync::Arc};

use futures::{future, FutureExt};
use log::error;
use tokio::{
    sync::{mpsc, oneshot, RwLock},
    time::timeout,
};

use crate::{
    client,
    protocol::{self, Packet, ReturnCode, Send, Unsubscribe},
    subscription::{self, SendEvent, Subscription},
    topic::{self, Message, Topic},
    WAIT_REPLY_TIMEOUT,
};

type Result<T> = std::result::Result<T, Error>;

#[derive(Debug)]
pub enum Error {
    ReplyChanClosed,
    ConsumerDuplicateSubscribed,
    ReturnCode(ReturnCode),
}

impl std::error::Error for Error {}

impl Display for Error {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        todo!()
    }
}

impl From<subscription::Error> for Error {
    fn from(e: subscription::Error) -> Self {
        todo!()
    }
}

impl From<topic::Error> for Error {
    fn from(value: topic::Error) -> Self {
        todo!()
    }
}

impl<T> From<mpsc::error::SendError<T>> for Error {
    fn from(value: mpsc::error::SendError<T>) -> Self {
        todo!()
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

    async fn run(self, broker_rx: mpsc::UnboundedReceiver<ClientMessage>) -> Result<()> {
        // subscription task
        let (send_tx, send_rx) = mpsc::unbounded_channel();
        let (sub_task, sub_handle) = self.clone().receive_subscription(send_rx).remote_handle();
        tokio::spawn(sub_task);

        // client task
        let (client_task, client_handle) = self
            .clone()
            .receive_client(send_tx, broker_rx)
            .remote_handle();
        tokio::spawn(client_task);

        future::try_join(sub_handle, client_handle).await?;
        Ok(())
    }

    /// process send event from all subscriptions
    async fn receive_subscription(
        self,
        mut send_rx: mpsc::UnboundedReceiver<SendEvent>,
    ) -> Result<()> {
        while let Some(event) = send_rx.recv().await {
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
                            event.res_tx.send(true);
                            return;
                        }
                        Ok(Ok(Err(e))) => {
                            if let client::Error::Client(ReturnCode::ConsumerDuplicated) = e {
                                event.res_tx.send(true);
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
                    event.res_tx.send(false);
                });
            }
        }
        Ok(())
    }

    async fn receive_client(
        self,
        publish_tx: mpsc::UnboundedSender<SendEvent>,
        mut broker_rx: mpsc::UnboundedReceiver<ClientMessage>,
    ) -> Result<()> {
        while let Some(msg) = broker_rx.recv().await {
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
                            .map_err(|_| Error::ReplyChanClosed)?;
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
                        continue;
                    };
                    let consumers = session.consumers();
                    let mut topics = self.topics.write().await;
                    for (consumer_id, sub_info) in consumers {
                        let Some(mut topic) = topics.remove(&sub_info.topic_name) else {
                            continue;
                        };
                        let Some(mut sp) = topic.del_subscription(&sub_info.sub_name) else {
                            continue;
                        };
                        sp.del_consumer(client_id, consumer_id).await?;
                    }
                }
                _ => {
                    let code = match self
                        .process_packet(publish_tx.clone(), client_id, msg.packet)
                        .await
                    {
                        Ok(_) => ReturnCode::Success,
                        Err(Error::ReturnCode(code)) => code,
                        Err(e) => Err(e)?,
                    };
                    if let Some(res_tx) = msg.res_tx {
                        res_tx.send(code).map_err(|_| Error::ReplyChanClosed)?;
                    }
                }
            }
        }
        Ok(())
    }

    /// process packets from client
    /// DO NOT BLOCK!!!
    async fn process_packet(
        &self,
        publish_tx: mpsc::UnboundedSender<SendEvent>,
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
                    Some(topic) => match topic.get_mut_subscription(&sub.sub_name) {
                        Some(sp) => {
                            sp.add_consumer(client_id, &sub).await?;
                        }
                        None => {
                            let mut sp = Subscription::from_subscribe(
                                client_id,
                                sub.consumer_id,
                                &sub,
                                publish_tx.clone(),
                            )?;
                            sp.add_consumer(client_id, &sub).await?;
                            topic.add_subscription(sp);
                        }
                    },
                    None => {
                        let mut topic = Topic::new(&sub.topic);
                        let sp = Subscription::from_subscribe(
                            client_id,
                            sub.consumer_id,
                            &sub,
                            publish_tx.clone(),
                        )?;
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
                        topic.add_message(message)?;
                    }
                    None => return Err(Error::ReturnCode(ReturnCode::TopicNotExists)),
                }
            }
            Packet::ControlFlow(c) => {
                let clients = self.clients.read().await;
                // add permits to subscription
                let Some(session) = clients.get(&client_id) else {
                    return Err(Error::ReturnCode(ReturnCode::NotConnected));
                };
                let Some(info) = session.consumers.get(&c.consumer_id) else {
                    return Err(Error::ReturnCode(ReturnCode::ConsumerNotFound));
                };
                let mut topics = self.topics.write().await;
                let Some(topic) = topics.get_mut(&info.topic_name) else {
                    unreachable!()
                };
                let Some(sp) = topic.get_mut_subscription(&info.sub_name) else {
                    unreachable!()
                };
                sp.additional_permits(client_id, c.consumer_id, c.permits)?;
            }
            _ => unreachable!(),
        }
        Ok(())
    }
}
