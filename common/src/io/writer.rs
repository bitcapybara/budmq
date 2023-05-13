use std::time::Duration;

use log::error;
use s2n_quic::connection::Handle;
use tokio::{
    select,
    sync::{mpsc, oneshot},
    task::JoinSet,
    time::{timeout, Instant},
};
use tokio_util::sync::CancellationToken;

use super::{
    stream::{pool::PoolInner, single::SingleInner, PoolCloser, PoolSender, Request, StreamPool},
    Result,
};
use crate::protocol::Packet;

pub struct WriterBuilder {
    handle: Handle,
    ordered: bool,
    token: CancellationToken,
}

impl WriterBuilder {
    fn new(handle: Handle, token: CancellationToken) -> Self {
        Self {
            handle,
            ordered: false,
            token,
        }
    }

    pub fn ordered(mut self, ordered: bool) -> Self {
        self.ordered = ordered;
        self
    }

    pub async fn build(self) -> Result<(Writer, WriterHandler)> {
        let (tx, rx) = mpsc::channel(1);
        let mut tasks = JoinSet::new();
        let (pool_sender, pool_closer) = if self.ordered {
            let (single, sender, closer) =
                StreamPool::<SingleInner>::new(self.handle, self.token.child_token());
            tasks.spawn(single.run());
            (sender, closer)
        } else {
            let (single, sender, closer) =
                StreamPool::<PoolInner>::new(self.handle, self.token.child_token());
            tasks.spawn(single.run());
            (sender, closer)
        };
        let writer_handler = WriterHandler::new(tx, tasks, pool_closer, self.token.clone());
        Ok((
            Writer {
                pool_sender,
                ordered: self.ordered,
                rx,
            },
            writer_handler,
        ))
    }
}

pub struct Writer {
    pool_sender: PoolSender,
    ordered: bool,
    rx: mpsc::Receiver<Request>,
}

impl Writer {
    pub fn builder(handle: Handle, token: CancellationToken) -> WriterBuilder {
        WriterBuilder::new(handle, token)
    }

    pub async fn run(mut self, token: CancellationToken) {
        loop {
            select! {
                res = self.rx.recv() => {
                    let Some(request) = res else {
                        token.cancel();
                        return
                    };
                    if let Err(e) = self.pool_sender.send(request).await {
                        error!("error occurs while sending packet: {e}")
                    }
                }
                _ = token.cancelled() => {
                    return
                }
            }
        }
    }
}

pub struct WriterHandler {
    tx: mpsc::Sender<Request>,
    tasks: JoinSet<()>,
    pool_closer: PoolCloser,
    token: CancellationToken,
}

impl WriterHandler {
    fn new(
        tx: mpsc::Sender<Request>,
        tasks: JoinSet<()>,
        pool_closer: PoolCloser,
        token: CancellationToken,
    ) -> Self {
        Self {
            tx,
            token,
            tasks,
            pool_closer,
        }
    }

    pub async fn send(&self, packet: Packet) -> Result<()> {
        let (res_tx, res_rx) = oneshot::channel();
        self.tx.send(Request { packet, res_tx }).await?;
        res_rx.await?
    }

    pub async fn send_timeout(&self, packet: Packet, duration: Duration) -> Result<()> {
        let (res_tx, res_rx) = oneshot::channel();
        let start = Instant::now();
        timeout(duration, self.tx.send(Request { packet, res_tx })).await??;
        let send_duration = start.elapsed();
        timeout(duration - send_duration, res_rx).await??
    }

    pub async fn close(mut self) {
        self.token.cancel();
        self.pool_closer.close().await;
        while self.tasks.join_next().await.is_some() {}
    }
}
