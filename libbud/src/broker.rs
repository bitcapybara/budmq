use std::{
    collections::{HashMap, HashSet},
    fmt::Display,
};

use tokio::sync::{mpsc, oneshot};

use crate::{
    protocol::{self, Packet, Publish, ReturnCode, ReturnCodeResult, Subscribe, Unsubscribe},
    subscription::{
        self, check_pub_subject, check_sub_subject, RawSubscription, SubClients, SubType,
        Subscription,
    },
    topic::Topic,
};

type Result<T> = std::result::Result<T, Error>;

#[derive(Debug)]
pub enum Error {
    ReplyChanClosed,
}

impl std::error::Error for Error {}

impl Display for Error {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        todo!()
    }
}

impl From<subscription::Error> for Error {
    fn from(value: subscription::Error) -> Self {
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
    sub_id: String,
    topic: String
}

/// One session per clientg
struct Session {
    client_id: u64,
    /// key = consumer_id, value = sub_id
    consumers: HashMap<u64, SubInfo>,
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
            if let Some(res_tx) = msg.res_tx {
                let code = self
                    .process_packet(client_id, msg.packet)
                    .err()
                    .unwrap_or(ReturnCode::Success);
                res_tx.send(code).map_err(|_| Error::ReplyChanClosed)?;
            }
        }
        Ok(())
    }

    /// process packets from client
    /// DO NOT BLOCK!!!
    fn process_packet(&mut self, client_id: u64, packet: Packet) -> ReturnCodeResult {
        match packet {
            Packet::Connect(c) => {
                if self.clients.contains_key(&client_id) {
                    return Err(ReturnCode::AlreadyConnected);
                }
                self.clients.insert(client_id, Session {
                    
                });
            }
            Packet::Subscribe(sub) => {
                // add subscription into topic
                match self.topics.get(&sub.topic) {
                    Some(topic) => match topic.get_mut_subscription(&sub.sub_id) {
                        Some(sp) => {
                            sp.add_client(client_id, sub.sub_type)?;
                        }
                        None => {
                            let mut sp = Subscription::from_subscribe(client_id, &sub);
                            sp.add_client(client_id, sub.sub_type)?;
                            topic.add_subscription(sp);
                        }
                    },
                    None => {
                        let mut topic = Topic::new(&sub.topic);
                        let sp = Subscription::from_subscribe(client_id, &sub);
                        topic.add_subscription(sp);
                        self.topics.insert(sub.topic.clone(), topic);
                    }
                }
                self.clients.get()
            }
            Packet::Unsubscribe(Unsubscribe { consumer_id }) => {
                if let Some(session) = self.clients.get(&client_id) {
                    let Some(info) = session.consumers.get(consumer_id) {
                        
                    if let Some(tp) = self.topics.get(&info.topic) {
                        tp.del_subscription(&sub_id);
                    }
                    }
                }
            }
            Packet::Publish(Publish {
                topic,
                sequence_id,
                payload,
            }) => {}
            _ => unreachable!(),
        }
        Ok(())
    }
}
