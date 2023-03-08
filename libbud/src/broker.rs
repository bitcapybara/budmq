use std::fmt::Display;

use tokio::sync::{mpsc, oneshot};

use crate::protocol::{self, ReturnCode};

type Result<T> = std::result::Result<T, Error>;

#[derive(Debug)]
pub enum Error {}

impl std::error::Error for Error {}

impl Display for Error {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
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

pub struct Broker {
    broker_rx: mpsc::UnboundedReceiver<ClientMessage>,
}

impl Broker {
    pub fn new(broker_rx: mpsc::UnboundedReceiver<ClientMessage>) -> Self {
        Self { broker_rx }
    }

    pub async fn run(mut self) -> Result<()> {
        while let Some(msg) = self.broker_rx.recv().await {
            let client_id = msg.client_id;
            match msg.packet {
                protocol::Packet::Connect(c) => {
                    todo!()
                }
                _ => unreachable!(),
            }
        }
        Ok(())
    }
}
