use std::time::Duration;

use bud_common::{
    id::next_id,
    io::{stream::Request, writer},
    protocol::{Packet, Ping},
};
use log::{error, trace, warn};
use s2n_quic::connection::Handle;
use tokio::{
    select,
    sync::{mpsc, oneshot},
    time::timeout,
};
use tokio_util::sync::CancellationToken;

use crate::WAIT_REPLY_TIMEOUT;

use super::{Result, SharedError};

pub struct OutgoingMessage {
    pub packet: Packet,
    pub res_tx: oneshot::Sender<Option<Packet>>,
}

pub struct Writer {
    /// sender message to io::writer
    sender: mpsc::Sender<Request>,
    /// connection error
    error: SharedError,
    token: CancellationToken,
}

impl Writer {
    pub fn new(
        handle: Handle,
        ordered: bool,
        error: SharedError,
        token: CancellationToken,
    ) -> Self {
        let (sender, receiver) = mpsc::channel(1);
        let writer = writer::Writer::builder(receiver, handle, token.clone())
            .ordered(ordered)
            .build();
        tokio::spawn(writer.run());
        Self {
            sender,
            error,
            token,
        }
    }
    pub async fn run(
        mut self,
        mut server_rx: mpsc::UnboundedReceiver<OutgoingMessage>,
        keepalive: u16,
    ) {
        let keepalive = Duration::from_millis(keepalive as u64);
        let mut ping_err_count = 0;
        loop {
            select! {
                res = timeout(keepalive, server_rx.recv()) => {
                    match res {
                        Ok(res) => {
                            let Some(msg) = res else {
                                trace!("connector::writer: receive none, exit");
                                return;
                            };
                            if let Err(e) = self.write(msg).await {
                                self.error.set(e).await;
                                return
                            }
                        }
                        Err(_) => {
                            match self.ping().await {
                                Ok(Some(_)) => {},
                                Ok(None) => {
                                    trace!("connector::writer::run: miss pong packet");
                                    ping_err_count += 1;
                                    if ping_err_count >= 3 {
                                        return
                                    }
                                },
                                Err(_) => {
                                    return
                                },
                            }
                        }
                    }
                }
                _ = self.token.cancelled() => {
                    return
                }
            }
        }
    }

    async fn ping(&mut self) -> Result<Option<()>> {
        trace!("connector::writer::run: send PING packet to server, open bi stream");
        // send ping
        let (res_tx, res_rx) = oneshot::channel();
        let request = Request {
            packet: Packet::Ping(Ping {
                request_id: next_id(),
            }),
            res_tx,
        };
        self.sender.send(request).await?;
        trace!("connector::writer::run: waiting for PONG packet");
        match timeout(WAIT_REPLY_TIMEOUT, res_rx).await {
            Ok(Ok(Ok(Packet::Pong(_)))) => Ok(Some(())),
            Ok(Ok(Ok(_))) => {
                error!("received unexpected packet, expected PONG packet");
                Ok(None)
            }
            Ok(Ok(Err(e))) => {
                error!("send ping error: {e}");
                Ok(None)
            }
            Ok(Err(e)) => {
                error!("client ping send endpoint dropped");
                Err(e.into())
            }
            Err(_) => {
                error!("waiting for PONG packet timeout");
                Ok(None)
            }
        }
    }

    async fn write(&self, msg: OutgoingMessage) -> Result<()> {
        let (packet, client_res_tx) = (msg.packet, msg.res_tx);
        let packet_type = packet.packet_type();
        // send publish message
        trace!("connector::writer::run: send {packet_type} message to server");
        let (res_tx, res_rx) = oneshot::channel();
        let request = Request { packet, res_tx };
        if let Err(e) = self.sender.send(request).await {
            if client_res_tx.send(None).is_err() {
                error!("client writer wait channel dropped by user");
            }
            return Err(e.into());
        }
        // wait for ack
        trace!("connector::writer::run: waiting for server response");
        let error = self.error.clone();
        tokio::spawn(async move {
            match Self::wait(res_rx).await {
                Ok(Some(resp)) => {
                    if client_res_tx.send(Some(resp)).is_err() {
                        warn!("client writer wait channel dropped");
                    }
                }
                Ok(None) => {}
                Err(e) => {
                    error.set(e).await;
                }
            }
        });

        Ok(())
    }

    async fn wait(
        res_rx: oneshot::Receiver<bud_common::io::Result<Packet>>,
    ) -> Result<Option<Packet>> {
        let resp = match timeout(WAIT_REPLY_TIMEOUT, res_rx).await {
            Ok(Ok(Ok(p))) => p,
            Ok(Ok(Err(e))) => {
                error!("client writer decode frame error: {e}");
                return Err(e.into());
            }
            Ok(Err(e)) => {
                error!("client writer: res_tx dropped");
                return Err(e.into());
            }
            Err(_) => {
                error!("client writer wait for reply timeout");
                return Ok(None);
            }
        };
        Ok(Some(resp))
    }
}
