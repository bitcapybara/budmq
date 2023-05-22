use std::time::Duration;

use bud_common::{
    io::{
        reader::{self, Request},
        Error, SharedError,
    },
    protocol::{Packet, Pong, Response, ReturnCode},
};
use log::{error, trace, warn};
use s2n_quic::connection::StreamAcceptor;
use tokio::{
    select,
    sync::{mpsc, oneshot},
    time::timeout,
};
use tokio_util::sync::CancellationToken;

use super::Result;
use crate::{broker, client, WAIT_REPLY_TIMEOUT};

pub struct Reader {
    client_id: u64,
    local_addr: String,
    /// send message to broker
    broker_tx: mpsc::UnboundedSender<broker::ClientMessage>,
    /// receive message from io stream
    receiver: mpsc::Receiver<Request>,
    keepalive: u16,
    error: SharedError,
    token: CancellationToken,
}

impl Reader {
    pub fn new(
        client_id: u64,
        local_addr: &str,
        broker_tx: mpsc::UnboundedSender<broker::ClientMessage>,
        acceptor: StreamAcceptor,
        keepalive: u16,
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
            keepalive,
            token,
            error,
        }
    }

    /// accept new stream to read a packet
    /// TODO impl Future trait
    pub async fn run(mut self) {
        // 1.5 times keepalive value
        let keepalive = Duration::from_millis((self.keepalive + self.keepalive / 2) as u64);
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
                                self.error.set(Error::ConnectionClosed).await;
                                return
                            }
                        },
                        Ok(None) => {
                            error!("server connection closed");
                            self.error.set(Error::ConnectionClosed).await;
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
                            self.error.set(Error::ConnectionClosed).await;
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

    async fn process_request(&self, request: Request) -> Result<()> {
        trace!("client::reader: waiting for framed packet");
        let Request { packet, res_tx } = request;
        match packet {
            Packet::Connect(c) => {
                warn!("client::reader: receive a CONNECT packet after handshake");
                // Do not allow duplicate connections
                let code = ReturnCode::AlreadyConnected;
                let request_id = c.request_id;
                let resp = Packet::Response(Response { request_id, code });
                res_tx.send(Some(resp)).ok();
            }
            p @ (Packet::Subscribe(_)
            | Packet::Unsubscribe(_)
            | Packet::Publish(_)
            | Packet::ConsumeAck(_)
            | Packet::ControlFlow(_)) => {
                trace!("client::reader: receive {} packet", p.packet_type());
                if let Err(e) = self.send(p, res_tx).await {
                    error!("send packet to broker error: {e}");
                    return Err(e);
                }
            }
            Packet::Ping(p) => {
                // return pong packet directly
                trace!("client::reader: receive PING packet");
                let packet = Packet::Pong(Pong {
                    request_id: p.request_id,
                });
                res_tx.send(Some(packet)).ok();
            }
            Packet::Disconnect => {
                res_tx.send(None).ok();
                if let Err(e) = self.broker_tx.send(broker::ClientMessage {
                    client_id: self.client_id,
                    packet: Packet::Disconnect,
                    res_tx: None,
                    client_tx: None,
                }) {
                    error!("send DISCONNECT to broker error: {e}");
                }
                return Err(client::Error::ClientDisconnect);
            }
            p => {
                error!("received unsupported packet: {}", p.packet_type());
                match p.request_id() {
                    Some(request_id) => {
                        res_tx
                            .send(Some(Packet::Response(Response {
                                request_id,
                                code: ReturnCode::UnexpectedPacket,
                            })))
                            .ok();
                    }
                    None => {
                        res_tx.send(None).ok();
                    }
                }
            }
        }
        Ok(())
    }

    async fn send(&self, packet: Packet, res_tx: oneshot::Sender<Option<Packet>>) -> Result<()> {
        let (broker_res_tx, broker_res_rx) = oneshot::channel();
        // send to broker
        trace!("client::reader: send packet to broker");
        self.broker_tx
            .send(broker::ClientMessage {
                client_id: self.client_id,
                packet,
                res_tx: Some(broker_res_tx),
                client_tx: None,
            })
            .map_err(|_| Error::ConnectionClosed)?;
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
}
