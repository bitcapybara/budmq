use bud_common::{
    io::{
        stream::{self, Request},
        writer,
    },
    protocol::Packet,
};
use log::{error, trace};
use s2n_quic::connection;
use tokio::{
    select,
    sync::{mpsc, oneshot},
    time::timeout,
};
use tokio_util::sync::CancellationToken;

use super::Result;
use crate::{broker::BrokerMessage, WAIT_REPLY_TIMEOUT};

pub struct Writer {
    local_addr: String,
    sender: mpsc::Sender<Request>,
    token: CancellationToken,
}

impl Writer {
    pub async fn new(
        local_addr: &str,
        handle: connection::Handle,
        token: CancellationToken,
    ) -> Result<Self> {
        let (sender, receiver) = mpsc::channel(1);
        let writer = writer::Writer::builder(receiver, handle, token.child_token())
            .build()
            .await?;
        tokio::spawn(writer.run());
        Ok(Self {
            local_addr: local_addr.to_string(),
            sender,
            token,
        })
    }

    /// messages sent from server to client
    /// need to open a new stream to send messages
    /// client_rx: receive message from broker
    pub async fn run(self, mut client_rx: mpsc::UnboundedReceiver<BrokerMessage>) {
        loop {
            select! {
                res = client_rx.recv() => {
                    let Some(message) = res else {
                        return
                    };
                    trace!("client:writer: receive a new packet task, open stream");
                    // send to client: framed.send(Packet) async? error log?
                    match message.packet {
                        p @ Packet::Send(_) => {
                            trace!("client::writer: Send SEND packet to client");
                            if let Err(e) = self.send(message.res_tx, p).await {
                                error!("send message to client error: {e}");
                            }
                        }
                        p => error!(
                            "received unexpected packet from broker: {:?}",
                            p.packet_type()
                        ),
                    }
                }
                _ = self.token.cancelled() => {
                    return
                }
            }
        }
    }

    async fn send(&self, client_res_tx: oneshot::Sender<Result<()>>, packet: Packet) -> Result<()> {
        let sender = self.sender.clone();
        let token = self.token.child_token();
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
            let (res_tx, res_rx) = oneshot::channel();
            select! {
                res = sender.send(stream::Request { packet, res_tx }) => {
                    if let Err(e) = res {
                        error!("server writer send packet error: {e}");
                    }
                }
                _ = token.cancelled() => {
                    return
                }
            }
            select! {
                res = res_rx => {
                    match res {
                        Ok(_) => client_res_tx.send(Ok(())).ok(),
                        Err(e) => client_res_tx.send(Err(e.into())).ok(),
                    };
                }
                _ = token.cancelled() => {}
            }
        });
        Ok(())
    }
}
