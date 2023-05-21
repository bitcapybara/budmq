use std::time::Duration;

use bud_common::{
    id::SerialId,
    io::{
        writer::{new_pool, Request},
        Error,
    },
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

use super::{Result, SharedError};

pub struct Writer {
    request_id: SerialId,
    /// send message to server
    sender: mpsc::Sender<Request>,
    /// error
    error: SharedError,
    token: CancellationToken,
    request_rx: mpsc::UnboundedReceiver<Request>,
    keepalive: u16,
}

impl Writer {
    pub fn new(
        handle: Handle,
        request_id: SerialId,
        ordered: bool,
        error: SharedError,
        token: CancellationToken,
        request_rx: mpsc::UnboundedReceiver<Request>,
        keepalive: u16,
    ) -> Self {
        let (tx, rx) = mpsc::channel(1);
        new_pool(handle, rx, ordered, error.clone(), token.clone());
        Self {
            token,
            error,
            request_id,
            sender: tx,
            request_rx,
            keepalive,
        }
    }

    /// receive message from user, send to server
    /// TODO impl Future trait?
    pub async fn run(mut self) {
        let keepalive = Duration::from_millis(self.keepalive as u64);
        let mut ping_err_count = 0;
        loop {
            select! {
                res = timeout(keepalive, self.request_rx.recv()) => {
                    match res {
                        Ok(res) => {
                            let Some(msg) = res else {
                                trace!("connector::writer: receive none, exit");
                                return;
                            };
                            if let Err(e) = self.sender.send(msg).await {
                                self.error.set(e.into()).await;
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
                                    self.error.set(Error::ConnectionClosed).await;
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
        let (res_tx, res_rx) = oneshot::channel();
        self.sender
            .send(Request {
                packet: Packet::Ping(Ping {
                    request_id: self.request_id.next(),
                }),
                res_tx: Some(res_tx),
            })
            .await?;
        match res_rx.await? {
            Ok(Packet::Pong(_)) => Ok(Some(())),
            Ok(_) => Ok(None),
            Err(e) => Err(e)?,
        }
    }
}
