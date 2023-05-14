use std::sync::Arc;

use bud_common::{
    io::{stream, writer},
    protocol::Packet,
};
use log::{error, trace};
use s2n_quic::connection;
use tokio::{
    select,
    sync::{mpsc, oneshot, Mutex},
    task::JoinSet,
    time::timeout,
};
use tokio_util::sync::CancellationToken;

use super::Result;
use crate::{broker::BrokerMessage, WAIT_REPLY_TIMEOUT};

pub struct WriteHandle {
    tasks: Arc<Mutex<JoinSet<()>>>,
}

impl WriteHandle {
    pub fn new(tasks: Arc<Mutex<JoinSet<()>>>) -> Self {
        Self { tasks }
    }

    pub async fn close(self) {
        let mut tasks = self.tasks.lock().await;
        while let Some(res) = tasks.join_next().await {
            if let Err(e) = res {
                error!("write handle task panics: {e}")
            }
        }
    }
}

pub struct Writer {
    local_addr: String,
    writer_handle: writer::WriterHandle,
    tasks: Arc<Mutex<JoinSet<()>>>,
    token: CancellationToken,
}

impl Writer {
    pub async fn new(
        local_addr: &str,
        handle: connection::Handle,
        token: CancellationToken,
    ) -> Result<(Self, WriteHandle)> {
        let (writer, writer_handle) = writer::Writer::builder(handle, token.child_token())
            .build()
            .await?;
        let mut tasks = JoinSet::new();
        tasks.spawn(writer.run());
        let tasks = Arc::new(Mutex::new(tasks));
        Ok((
            Self {
                local_addr: local_addr.to_string(),
                writer_handle,
                tasks: tasks.clone(),
                token,
            },
            WriteHandle::new(tasks),
        ))
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
        let sender = self.writer_handle.tx.clone();
        let token = self.token.child_token();
        let mut tasks = self.tasks.lock().await;
        tasks.spawn(async move {
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
