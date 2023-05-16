use std::{sync::Arc, time::Duration};

use bud_common::{
    id::next_id,
    io::{stream::Request, writer},
    protocol::{Packet, Ping, ReturnCode},
};
use log::{error, trace};
use s2n_quic::connection::Handle;
use tokio::{
    select,
    sync::{mpsc, oneshot, Mutex},
    time::timeout,
};
use tokio_util::sync::CancellationToken;

use crate::{connector::Error, WAIT_REPLY_TIMEOUT};

use super::Result;

pub struct OutgoingMessage {
    pub packet: Packet,
    pub res_tx: oneshot::Sender<Result<ReturnCode>>,
}

pub struct Writer {
    sender: mpsc::Sender<Request>,
    token: CancellationToken,
}

impl Writer {
    pub async fn new(handle: Handle, token: CancellationToken) -> Result<Self> {
        let (writer, sender) = writer::Writer::builder(handle, token.clone())
            .build()
            .await?;
        tokio::spawn(writer.run());
        Ok(Self { sender, token })
    }
    pub async fn run(
        mut self,
        server_rx: Arc<Mutex<mpsc::UnboundedReceiver<OutgoingMessage>>>,
        keepalive: u16,
    ) {
        let keepalive = Duration::from_millis(keepalive as u64);
        let mut server_rx = server_rx.lock().await;
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
                                error!("write to connection error: {e}");
                                continue;
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

    async fn write(&mut self, msg: OutgoingMessage) -> Result<()> {
        // send message to connection
        let sender = self.sender.clone();
        tokio::spawn(send_and_wait(sender, msg));
        Ok(())
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
            Ok(Err(_)) => {
                error!("client ping send endpoint dropped");
                Err(Error::WaitChannelDropped)
            }
            Err(_) => {
                error!("waiting for PONG packet timeout");
                Ok(None)
            }
        }
    }
}

async fn send_and_wait(sender: mpsc::Sender<Request>, msg: OutgoingMessage) {
    let (packet, client_res_tx) = (msg.packet, msg.res_tx);
    let packet_type = packet.packet_type();
    // send publish message
    trace!("connector::writer::run: send {packet_type} message to server");
    let (res_tx, res_rx) = oneshot::channel();
    let request = Request { packet, res_tx };
    if let Err(e) = sender.send(request).await {
        if client_res_tx.send(Err(e.into())).is_err() {
            error!("client writer wait channel dropped");
        }
        return;
    }
    // wait for ack
    trace!("connector::writer::run: waiting for server response");
    let resp = match timeout(WAIT_REPLY_TIMEOUT, res_rx).await {
        Ok(Ok(Ok(Packet::Response(resp)))) => resp,
        Ok(Ok(Ok(p))) => {
            error!(
                "client writer: expected Packet::ReturnCode, found {:?}",
                p.packet_type()
            );
            return;
        }
        Ok(Ok(Err(_))) => {
            error!("client writer: framed stream dropped");
            return;
        }
        Ok(Err(e)) => {
            error!("client writer decode frame error: {e}");
            return;
        }
        Err(_) => {
            error!("client writer wait for {packet_type} reply timeout");
            return;
        }
    };
    trace!(
        "connector::writer::run: receive reponse from server: {}",
        resp.code
    );
    if client_res_tx.send(Ok(resp.code)).is_err() {
        error!("client writer wait channel dropped");
    }
}
