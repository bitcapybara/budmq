use std::time::Duration;

use bud_common::{
    id::SerialId,
    io::writer,
    protocol::{Packet, Ping},
};
use log::{error, trace};
use s2n_quic::connection::Handle;
use tokio::{
    select,
    sync::{mpsc, oneshot},
    time::timeout,
};
use tokio_util::sync::CancellationToken;

use super::{Event, Result, SharedError};

pub struct OutgoingMessage {
    pub packet: Packet,
    pub res_tx: oneshot::Sender<Option<Packet>>,
}

pub struct Writer {
    request_id: SerialId,
    /// sender message to io::writer
    sender: writer::Writer,
    /// regist consumer to reader
    register_tx: mpsc::Sender<Event>,
    /// error
    error: SharedError,
    token: CancellationToken,
}

impl Writer {
    pub fn new(
        handle: Handle,
        request_id: SerialId,
        ordered: bool,
        error: SharedError,
        token: CancellationToken,
        register_tx: mpsc::Sender<Event>,
    ) -> Self {
        let sender = writer::Writer::new(handle, ordered, error.clone(), token.clone());
        Self {
            sender,
            token,
            register_tx,
            error,
            request_id,
        }
    }

    pub async fn run(
        mut self,
        mut request_rx: mpsc::UnboundedReceiver<OutgoingMessage>,
        keepalive: u16,
    ) {
        let keepalive = Duration::from_millis(keepalive as u64);
        let mut ping_err_count = 0;
        loop {
            select! {
                res = timeout(keepalive, request_rx.recv()) => {
                    match res {
                        Ok(res) => {
                            let Some(msg) = res else {
                                trace!("connector::writer: receive none, exit");
                                return;
                            };
                            if let Err(_e) = self.send(msg).await {
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
        // send ping
        trace!("connector::writer::run: waiting for PONG packet");
        self.sender
            .send(Packet::Ping(Ping {
                request_id: self.request_id.next(),
            }))
            .await?;
        todo!()
    }

    async fn send(&self, msg: OutgoingMessage) -> Result<()> {
        let (packet, client_res_tx) = (msg.packet, msg.res_tx);
        let packet_type = packet.packet_type();
        // send publish message
        trace!("connector::writer::run: send {packet_type} message to server");
        match self.sender.send(packet).await {
            Ok(packet) => {
                client_res_tx.send(Some(packet)).ok();
                Ok(())
            }
            Err(e) => {
                if client_res_tx.send(None).is_err() {
                    error!("client writer wait channel dropped by user");
                }
                Err(e.into())
            }
        }
    }
}
