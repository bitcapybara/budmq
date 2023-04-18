use std::time::Duration;

use bud_common::protocol::{Packet, PacketCodec, ReturnCode};
use futures::{SinkExt, StreamExt};
use log::error;
use s2n_quic::{connection::StreamAcceptor, stream::BidirectionalStream};
use tokio::{
    sync::{mpsc, oneshot},
    time::timeout,
};
use tokio_util::codec::Framed;

use super::Result;
use crate::{broker, WAIT_REPLY_TIMEOUT};

pub struct Reader {
    client_id: u64,
    local_addr: String,
    broker_tx: mpsc::UnboundedSender<broker::ClientMessage>,
}

impl Reader {
    pub fn new(
        client_id: u64,
        local_addr: &str,
        broker_tx: mpsc::UnboundedSender<broker::ClientMessage>,
    ) -> Self {
        Self {
            client_id,
            local_addr: local_addr.to_string(),
            broker_tx,
        }
    }

    /// accept new stream to read a packet
    pub async fn run(self, mut acceptor: StreamAcceptor, keepalive: u16) {
        // 1.5 times keepalive value
        let keepalive = Duration::from_millis((keepalive + keepalive / 2) as u64);
        loop {
            match timeout(keepalive, acceptor.accept_bidirectional_stream()).await {
                Ok(Ok(Some(stream))) => {
                    let mut framed = Framed::new(stream, PacketCodec);
                    match framed.next().await {
                        Some(Ok(packet)) => match packet {
                            Packet::Connect(_) => {
                                // Do not allow duplicate connections
                                let code = ReturnCode::AlreadyConnected;
                                if let Err(e) = framed.send(Packet::ReturnCode(code)).await {
                                    error!("send packet to client error: {e}");
                                }
                            }
                            p @ (Packet::Subscribe(_)
                            | Packet::Unsubscribe(_)
                            | Packet::Publish(_)
                            | Packet::ConsumeAck(_)
                            | Packet::ControlFlow(_)) => {
                                if let Err(e) = self.send(p, framed).await {
                                    error!("send packet to broker error: {e}")
                                }
                            }
                            Packet::Ping => {
                                // return pong packet directly
                                tokio::spawn(async move {
                                    if let Err(e) = framed.send(Packet::Pong).await {
                                        error!("send pong packet error: {e}")
                                    }
                                });
                            }
                            Packet::Disconnect => {
                                if let Err(e) = self.broker_tx.send(broker::ClientMessage {
                                    client_id: self.client_id,
                                    packet: Packet::Disconnect,
                                    res_tx: None,
                                    client_tx: None,
                                }) {
                                    error!("send DISCONNECT to broker error: {e}");
                                }
                            }
                            p => error!("received unsupported packet: {:?}", p.packet_type()),
                        },
                        Some(Err(e)) => error!("read from stream error: {e}"),
                        None => error!("framed stream closed"),
                    }
                }
                Ok(Ok(None)) => return,
                Ok(Err(e)) => {
                    error!("accept stream error: {e}");
                    continue;
                }
                Err(_) => {
                    // receive ping timeout
                    if let Err(e) = self.broker_tx.send(broker::ClientMessage {
                        client_id: self.client_id,
                        packet: Packet::Disconnect,
                        res_tx: None,
                        client_tx: None,
                    }) {
                        error!("send packet to broker error: {e}")
                    }
                }
            }
        }
    }

    async fn send(
        &self,
        packet: Packet,
        mut framed: Framed<BidirectionalStream, PacketCodec>,
    ) -> Result<()> {
        let (res_tx, res_rx) = oneshot::channel();
        // send to broker
        self.broker_tx.send(broker::ClientMessage {
            client_id: self.client_id,
            packet,
            res_tx: Some(res_tx),
            client_tx: None,
        })?;
        // wait for response in coroutine
        let local = self.local_addr.clone();
        tokio::spawn(async move {
            // wait for reply from broker
            match timeout(WAIT_REPLY_TIMEOUT, res_rx).await {
                Ok(Ok(code)) => {
                    if let Err(e) = framed.send(Packet::ReturnCode(code)).await {
                        error!("send reply to client {local} error: {e}",)
                    }
                }
                Ok(Err(e)) => {
                    error!("recv client {local} reply error: {e}")
                }
                Err(_) => error!("process client {local} timeout"),
            }
        });
        Ok(())
    }
}
