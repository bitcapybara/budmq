use std::time::Duration;

use log::error;
use s2n_quic::connection::Handle;
use tokio::{
    select,
    sync::{mpsc, oneshot},
    time::{timeout, Instant},
};
use tokio_util::sync::CancellationToken;

use super::{
    stream::{pool::PoolInner, single::SingleInner, PoolSender, Request, StreamPool},
    Result,
};
use crate::protocol::Packet;

pub struct WriterBuilder {
    handle: Handle,
    ordered: bool,
}

impl WriterBuilder {
    /// create an unordered writer.
    ///
    /// maintain a QUIC steam pool internally, each time a stream
    /// is acquired from the stream pool to send a message
    ///
    /// the order of message delivery is not guaranteed, but the
    /// concurrency performance and throughput are better (no
    /// head-of-queue blocking of a single stream)
    fn new(handle: Handle) -> Self {
        Self {
            handle,
            ordered: false,
        }
    }

    /// TODO
    /// create an ordered writer.
    ///
    /// use a single QUIC stream internally
    ///
    /// the order of message delivery is guaranteed, but there
    /// will be stream head blocking problems
    pub fn ordered(mut self, ordered: bool) -> Self {
        self.ordered = ordered;
        self
    }

    pub async fn build(self) -> Result<(Writer, Sender)> {
        let (tx, rx) = mpsc::channel(1);
        let sender = Sender::new(tx);
        let pool_sender = if self.ordered {
            let (single, sender) = StreamPool::<SingleInner>::new(self.handle);
            tokio::spawn(single.run());
            sender
        } else {
            let (single, sender) = StreamPool::<PoolInner>::new(self.handle);
            tokio::spawn(single.run());
            sender
        };
        Ok((
            Writer {
                pool_sender,
                ordered: self.ordered,
                rx,
            },
            sender,
        ))
    }
}

pub struct Writer {
    pool_sender: PoolSender,
    ordered: bool,
    rx: mpsc::Receiver<Request>,
}

impl Writer {
    pub fn builder(handle: Handle) -> WriterBuilder {
        WriterBuilder::new(handle)
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

pub struct Sender {
    tx: mpsc::Sender<Request>,
}

impl Sender {
    fn new(tx: mpsc::Sender<Request>) -> Self {
        Self { tx }
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
}
