use bud_common::io::{
    writer::{new_pool, Request},
    Error, SharedError,
};
use log::trace;
use s2n_quic::connection;
use tokio::{select, sync::mpsc, time::timeout};
use tokio_util::sync::CancellationToken;

use crate::{broker::BrokerMessage, WAIT_REPLY_TIMEOUT};

pub struct Writer {
    local_addr: String,
    /// send message to io stream
    sender: mpsc::Sender<Request>,
    /// receive message from broker, send to client
    client_rx: mpsc::UnboundedReceiver<BrokerMessage>,
    error: SharedError,
    token: CancellationToken,
}

impl Writer {
    pub fn new(
        local_addr: &str,
        handle: connection::Handle,
        client_rx: mpsc::UnboundedReceiver<BrokerMessage>,
        error: SharedError,
        token: CancellationToken,
    ) -> Self {
        let (tx, rx) = mpsc::channel(1);
        new_pool(handle, rx, true, error.clone(), token.child_token());
        Self {
            local_addr: local_addr.to_string(),
            sender: tx,
            token,
            error,
            client_rx,
        }
    }

    /// messages sent from server to client
    /// need to open a new stream to send messages
    /// client_rx: receive message from broker
    /// TODO impl Future trait
    pub async fn run(mut self) {
        loop {
            if self.error.is_set() {
                self.token.cancel();
                return;
            }
            select! {
                res = self.client_rx.recv() => {
                    let Some(message) = res else {
                        self.error.set(Error::ConnectionDisconnect).await;
                        return
                    };
                    trace!("client:writer: receive a new packet task, open stream");
                    // send to client: framed.send(Packet) async? error log?
                    trace!("client::writer: Send SEND packet to client");
                    self.send(message).await;
                }
                _ = self.token.cancelled() => {
                    return
                }
            }
        }
    }

    async fn send(&self, message: BrokerMessage) {
        let BrokerMessage { packet, res_tx } = message;
        let sender = self.sender.clone();
        let token = self.token.child_token();
        let error = self.error.clone();
        tokio::spawn(async move {
            let timeout_token = token.clone();
            tokio::spawn(async move {
                if timeout(WAIT_REPLY_TIMEOUT, timeout_token.cancelled())
                    .await
                    .is_err()
                {
                    timeout_token.cancel();
                }
            });

            select! {
                res = sender.send(Request { packet , res_tx: Some(res_tx)}) => {
                    if let Err(_e) = res {
                        error.set(Error::ConnectionDisconnect).await;
                    }
                }
                _ = token.cancelled() => {}
            }
        });
    }
}
