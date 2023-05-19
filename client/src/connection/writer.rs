use std::time::Duration;

use bud_common::{
    id::SerialId,
    io::writer::{new_pool, Request},
    protocol::{Packet, Ping},
};
use log::trace;
use s2n_quic::connection::Handle;
use tokio::{
    select,
    sync::{mpsc, oneshot},
    time::timeout,
};
use tokio_util::sync::CancellationToken;

use super::{Event, Result, SharedError};

pub struct Writer {
    request_id: SerialId,
    /// send message to server
    sender: mpsc::Sender<Request>,
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
        let sender = new_pool(handle, ordered, error.clone(), token.clone());
        Self {
            token,
            register_tx,
            error,
            request_id,
            sender,
        }
    }

    async fn ping(&mut self) -> Result<Option<()>> {
        // send ping
        trace!("connector::writer::run: waiting for PONG packet");
        let (res_tx, _res_rx) = oneshot::channel();
        self.sender
            .send(Request {
                packet: Packet::Ping(Ping {
                    request_id: self.request_id.next(),
                }),
                res_tx: Some(res_tx),
            })
            .await?;
        todo!()
    }

    pub async fn run(mut self, mut request_rx: mpsc::UnboundedReceiver<Request>, keepalive: u16) {
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

    async fn send(&self, _msg: Request) -> Result<()> {
        // let (packet, client_res_tx) = (msg.packet, msg.res_tx);
        // let packet_type = packet.packet_type();
        // // send publish message
        // trace!("connector::writer::run: send {packet_type} message to server");
        // match self.sender.send(msg).await {
        //     Ok(packet) => {
        //         // client_res_tx.send(Some(packet)).ok();
        //         Ok(())
        //     }
        //     Err(e) => {
        //         // if client_res_tx.send(None).is_err() {
        //         //     error!("client writer wait channel dropped by user");
        //         // }
        //         Err(e.into())
        //     }
        // }
        todo!()
    }
}
