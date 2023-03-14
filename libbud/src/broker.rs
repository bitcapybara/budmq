use std::{collections::HashMap, fmt::Display};

use tokio::sync::{mpsc, oneshot};

use crate::{
    protocol::{self, Packet, Publish, ReturnCode, ReturnCodeResult, Unsubscribe},
    subscription::{self, Subscription},
    topic::{self, Message, Topic},
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
    pub res_tx: Option<oneshot::Sender<ReturnCode>>,
}

struct SubInfo {
    sub_name: String,
    topic_name: String,
}

/// One session per client
/// Save a client's connection information
struct Session {
    client_tx: mpsc::UnboundedSender<BrokerMessage>,
    /// key = consumer_id, value = sub_id
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
}

pub struct Broker {
    /// key = client_id
    clients: HashMap<u64, Session>,
    /// key = topic
    topics: HashMap<String, Topic>,
    // /// key = sub_id, value = topic
    // subscriptions: HashMap<String, String>,
    /// channel to receive client messages
    broker_rx: mpsc::UnboundedReceiver<ClientMessage>,
}

impl Broker {
    pub fn new(broker_rx: mpsc::UnboundedReceiver<ClientMessage>) -> Self {
        Self {
            clients: HashMap::new(),
            broker_rx,
            topics: HashMap::new(),
            // subscriptions: HashMap::new(),
        }
    }

    pub async fn run(mut self) -> Result<()> {
        while let Some(msg) = self.broker_rx.recv().await {
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
                    if self.clients.contains_key(&client_id) {
                        res_tx
                            .send(ReturnCode::AlreadyConnected)
                            .map_err(|_| Error::ReplyChanClosed)?;
                    }
                    self.clients.insert(
                        client_id,
                        Session {
                            client_tx,
                            consumers: HashMap::new(),
                        },
                    );
                }
                _ => {
                    let code = match self.process_packet(client_id, msg.packet) {
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
    fn process_packet(&mut self, client_id: u64, packet: Packet) -> Result<()> {
        match packet {
            Packet::Subscribe(sub) => {
                let Some( session) = self.clients.get_mut(&client_id) else {
                    return Err(Error::ReturnCode(ReturnCode::NotConnected));
                };
                if session.consumers.contains_key(&sub.consumer_id) {
                    return Err(Error::ReturnCode(ReturnCode::ConsumerDuplicated));
                }
                // add subscription into topic
                match self.topics.get_mut(&sub.topic) {
                    Some(topic) => match topic.get_mut_subscription(&sub.sub_name) {
                        Some(sp) => {
                            sp.add_consumer(sub.consumer_id, sub.sub_type)?;
                        }
                        None => {
                            let mut sp = Subscription::from_subscribe(client_id, &sub)?;
                            sp.add_consumer(sub.consumer_id, sub.sub_type)?;
                            topic.add_subscription(sp);
                        }
                    },
                    None => {
                        let mut topic = Topic::new(&sub.topic);
                        let sp = Subscription::from_subscribe(client_id, &sub)?;
                        topic.add_subscription(sp);
                        self.topics.insert(sub.topic.clone(), topic);
                    }
                }
                // add consumer to session
                session.add_consumer(sub.consumer_id, &sub.sub_name, &sub.topic);
            }
            Packet::Unsubscribe(Unsubscribe { consumer_id }) => {
                if let Some(session) = self.clients.get_mut(&client_id) {
                    if let Some(info) = session.consumers.get(&consumer_id) {
                        if let Some(tp) = self.topics.get_mut(&info.topic_name) {
                            tp.del_subscription(&info.sub_name);
                        }
                        session.del_consumer(consumer_id);
                    }
                }
            }
            Packet::Publish(p) => {
                // add to topic
                let topic = p.topic.clone();
                let message = Message::from_publish(p);
                match self.topics.get_mut(&topic) {
                    Some(topic) => topic.add_message(message),
                    None => return Err(Error::ReturnCode(ReturnCode::TopicNotExists)),
                }
            }
            _ => unreachable!(),
        }
        Ok(())
    }
}
