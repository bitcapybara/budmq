use std::time::Duration;

use bud_common::{
    io::{
        reader::{self, Request},
        SharedError,
    },
    protocol::{Connect, Packet, Response, ReturnCode},
};
use log::{error, trace, warn};
use s2n_quic::connection::StreamAcceptor;
use tokio::{
    select,
    sync::{
        mpsc::{self, UnboundedSender},
        oneshot,
    },
    time::timeout,
};
use tokio_util::sync::CancellationToken;

use super::Result;
use crate::{
    broker::{self, BrokerMessage},
    client::Error,
    WAIT_REPLY_TIMEOUT,
};

const HANDSHAKE_TIMOUT: Duration = Duration::from_secs(5);

pub struct Reader {
    client_id: u64,
    local_addr: String,
    /// send message to broker
    broker_tx: mpsc::UnboundedSender<broker::ClientMessage>,
    /// receive message from io stream
    receiver: mpsc::Receiver<Request>,
    error: SharedError,
    token: CancellationToken,
}

impl Reader {
    pub fn new(
        client_id: u64,
        local_addr: &str,
        broker_tx: mpsc::UnboundedSender<broker::ClientMessage>,
        acceptor: StreamAcceptor,
        error: SharedError,
        token: CancellationToken,
    ) -> Self {
        let (sender, receiver) = mpsc::channel(1);
        tokio::spawn(reader::Reader::new(sender, acceptor, error.clone(), token.clone()).run());
        Self {
            client_id,
            local_addr: local_addr.to_string(),
            broker_tx,
            receiver,
            token,
            error,
        }
    }

    /// accept new stream to read a packet
    /// TODO impl Future trait
    pub async fn run(mut self, client_tx: UnboundedSender<BrokerMessage>) {
        let keepalive = match self.handshake(client_tx).await {
            Ok(keepalive) => keepalive,
            Err(e) => {
                error!("waiting for handshake packet error: {e}");
                return;
            }
        };
        // 1.5 times keepalive value
        let keepalive = Duration::from_millis((keepalive + keepalive / 2) as u64);
        loop {
            if self.error.is_set() {
                self.token.cancel();
                return;
            }
            select! {
                res = timeout(keepalive, self.receiver.recv()) => {
                    match res {
                        Ok(Some(request)) => {
                            if let Err(_e) = self.process_request(request).await {
                                self.error.set_disconnect().await;
                                return
                            }
                        },
                        Ok(None) => {
                            self.error.set_disconnect().await;
                            return;
                        }
                        Err(_) => {
                            trace!("client::reader::run: waiting for PING packet timeout");
                            // receive ping timeout
                            if let Err(e) = self.broker_tx.send(broker::ClientMessage {
                                client_id: self.client_id,
                                packet: Packet::Disconnect,
                                res_tx: None,
                                client_tx: None,
                            }) {
                                error!("wait for PING timeout, send DISCONNECT packet to broker error: {e}")
                            }
                            self.error.set_disconnect().await;
                            return
                        }
                    }
                }
                _ = self.token.cancelled() => {
                    return
                }
            }
        }
    }

    async fn handshake(&mut self, client_tx: UnboundedSender<BrokerMessage>) -> Result<u16> {
        let request = timeout(HANDSHAKE_TIMOUT, self.receiver.recv())
            .await?
            .ok_or(Error::Disconnect)?;
        let Request { packet, res_tx } = request;
        match packet {
            Packet::Connect(Connect { keepalive }) => {
                self.send(packet, res_tx, Some(client_tx)).await?;
                Ok(keepalive)
            }
            _ => {
                res_tx
                    .send(Some(Packet::Response(Response {
                        code: ReturnCode::UnexpectedPacket,
                    })))
                    .map_err(|_| Error::Disconnect)?;
                Err(Error::MissConnectPacket)
            }
        }
    }

    async fn process_request(&self, request: Request) -> Result<()> {
        let Request { packet, res_tx } = request;
        match packet {
            Packet::Connect(_) => {
                warn!("client::reader: receive a CONNECT packet after handshake");
                // Do not allow duplicate connections
                let code = ReturnCode::AlreadyConnected;
                let resp = Packet::Response(Response { code });
                res_tx.send(Some(resp)).ok();
            }
            p @ (Packet::Subscribe(_)
            | Packet::Unsubscribe(_)
            | Packet::Publish(_)
            | Packet::ConsumeAck(_)
            | Packet::ControlFlow(_)
            | Packet::LookupTopic(_)
            | Packet::CreateProducer(_)) => {
                trace!("client::reader: receive {} packet", p.packet_type());
                if let Err(e) = self.send(p, res_tx, None).await {
                    error!("send packet to broker error: {e}");
                    return Err(e);
                }
            }
            p @ (Packet::CloseProducer(_) | Packet::CloseConsumer(_) | Packet::Disconnect) => {
                trace!("client::reader: receive {} packet", p.packet_type());
                res_tx.send(None).ok();
                trace!("client::reader: receive {} packet", p.packet_type());
                if let Err(e) = self.send_async(p, None).await {
                    error!("send packet to broker error: {e}");
                    return Err(e);
                }
            }
            Packet::Ping => {
                // return pong packet directly
                trace!("client::reader: receive PING packet");
                let packet = Packet::Pong;
                res_tx.send(Some(packet)).ok();
            }
            p => {
                error!("received unsupported packet: {}", p.packet_type());
                res_tx
                    .send(Some(Packet::Response(Response {
                        code: ReturnCode::UnexpectedPacket,
                    })))
                    .ok();
            }
        }
        Ok(())
    }

    /// send packet to broker, waiting for response
    async fn send(
        &self,
        packet: Packet,
        res_tx: oneshot::Sender<Option<Packet>>,
        client_tx: Option<UnboundedSender<BrokerMessage>>,
    ) -> Result<()> {
        let (broker_res_tx, broker_res_rx) = oneshot::channel();
        // send to broker
        trace!("client::reader: send packet to broker");
        self.broker_tx
            .send(broker::ClientMessage {
                client_id: self.client_id,
                packet,
                res_tx: Some(broker_res_tx),
                client_tx,
            })
            .map_err(|_| Error::Disconnect)?;
        // wait for response in coroutine
        let local = self.local_addr.clone();
        let token = self.token.child_token();
        tokio::spawn(async move {
            // wait for reply from broker
            trace!("client::reader[spawn]: waiting for response from broker");
            select! {
                res = timeout(WAIT_REPLY_TIMEOUT, broker_res_rx) => {
                    match res {
                        Ok(Ok(packet)) => {
                            trace!("client::reader[spawn]: response from broker: {packet:?}" );
                            res_tx.send(Some(packet)).ok();
                        }
                        Ok(Err(e)) => {
                            error!("recv client {local} reply error: {e}")
                        }
                        Err(_) => error!("process client {local} timeout"),
                    }
                }
                _ = token.cancelled() => {}
            }
        });
        Ok(())
    }

    /// send packet to broker, without waiting for response
    async fn send_async(
        &self,
        packet: Packet,
        client_tx: Option<UnboundedSender<BrokerMessage>>,
    ) -> Result<()> {
        // send to broker
        trace!("client::reader: send packet to broker");
        self.broker_tx
            .send(broker::ClientMessage {
                client_id: self.client_id,
                packet,
                res_tx: None,
                client_tx,
            })
            .map_err(|_| Error::Disconnect)
    }
}
