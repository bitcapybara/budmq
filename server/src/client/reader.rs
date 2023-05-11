use std::time::Duration;

use bud_common::{
    id::next_id,
    protocol::{Packet, PacketCodec, Response, ReturnCode},
};
use futures::{SinkExt, StreamExt};
use log::{error, trace, warn};
use s2n_quic::{connection::StreamAcceptor, stream::BidirectionalStream};
use tokio::{
    select,
    sync::{mpsc, oneshot},
    time::timeout,
};
use tokio_util::{codec::Framed, sync::CancellationToken};

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
    pub async fn run(self, mut acceptor: StreamAcceptor, keepalive: u16, token: CancellationToken) {
        // 1.5 times keepalive value
        let keepalive = Duration::from_millis((keepalive + keepalive / 2) as u64);
        loop {
            select! {
                res = timeout(keepalive, acceptor.accept_bidirectional_stream()) => {
                    match res {
                        Ok(Ok(Some(stream))) => self.process_stream(stream).await,
                        Ok(Ok(None)) => {
                            error!("server connection closed");
                            token.cancel();
                            return;
                        }
                        Ok(Err(e)) => error!("accept stream error: {e}"),
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
                            // quit the loop
                            token.cancel();
                            return
                        }
                    }
                }
                _ = token.cancelled() => {
                    return
                }
            }
        }
    }

    async fn process_stream(&self, stream: BidirectionalStream) {
        let mut framed = Framed::new(stream, PacketCodec);
        trace!("client::reader: waiting for framed packet");
        match framed.next().await {
            Some(Ok(packet)) => match packet {
                Packet::Connect(c) => {
                    warn!("client::reader: receive a CONNECT packet after handshake");
                    // Do not allow duplicate connections
                    let code = ReturnCode::AlreadyConnected;
                    if let Err(e) = framed
                        .send(Packet::Response(Response {
                            request_id: c.request_id,
                            code,
                        }))
                        .await
                    {
                        error!("send packet to client error: {e}");
                    }
                }
                p @ (Packet::Subscribe(_)
                | Packet::Unsubscribe(_)
                | Packet::Publish(_)
                | Packet::ConsumeAck(_)
                | Packet::ControlFlow(_)) => {
                    trace!("client::reader: receive {} packet", p.packet_type());
                    if let Err(e) = self.send(p, framed).await {
                        error!("send packet to broker error: {e}")
                    }
                }
                Packet::Ping => {
                    // return pong packet directly
                    trace!("client::reader: receive PING packet");
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
                p => error!("received unsupported packet: {}", p.packet_type()),
            },
            Some(Err(e)) => error!("read from stream error: {e}"),
            None => error!("framed stream closed"),
        }
    }

    async fn send(
        &self,
        packet: Packet,
        mut framed: Framed<BidirectionalStream, PacketCodec>,
    ) -> Result<()> {
        let (res_tx, res_rx) = oneshot::channel();
        // send to broker
        trace!("client::reader: send packet to broker");
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
            trace!("client::reader[spawn]: waiting for response from broker");
            match timeout(WAIT_REPLY_TIMEOUT, res_rx).await {
                Ok(Ok(code)) => {
                    trace!("client::reader[spawn]: response from broker: {code}");
                    if let Err(e) = framed
                        .send(Packet::Response(Response {
                            request_id: next_id(),
                            code,
                        }))
                        .await
                    {
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
