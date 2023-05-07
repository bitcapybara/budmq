use std::time::Duration;

use s2n_quic::connection::Handle;
use tokio::{
    select,
    sync::{mpsc, oneshot},
    time::{timeout, Instant},
};
use tokio_util::sync::CancellationToken;

use super::Result;
use crate::protocol::Packet;

struct Request {
    packet: Packet,
    res_tx: oneshot::Sender<Result<()>>,
}

pub struct Writer {
    handle: Handle,
    rx: mpsc::Receiver<Request>,
}

impl Writer {
    /// create an unordered writer.
    ///
    /// maintain a QUIC steam pool internally, each time a stream
    /// is acquired from the stream pool to send a message
    ///
    /// the order of message delivery is not guaranteed, but the
    /// concurrency performance and throughput are better (no
    /// head-of-queue blocking of a single stream)
    pub fn new(handle: Handle) -> (Self, Sender) {
        let (tx, rx) = mpsc::channel(1);
        let sender = Sender::new(tx);
        (Self { handle, rx }, sender)
    }

    /// create an ordered writer.
    ///
    /// use a single QUIC stream internally
    ///
    /// the order of message delivery is guaranteed, but there
    /// will be stream head blocking problems
    pub fn new_with_ordered(handle: Handle) -> (Self, Sender) {
        let (tx, rx) = mpsc::channel(1);
        let sender = Sender::new(tx);
        (Self { handle, rx }, sender)
    }

    pub async fn run(mut self, token: CancellationToken) {
        loop {
            select! {
                res = self.rx.recv() => {
                    let Some(packet) = res else {
                        token.cancel();
                        return
                    };
                    self.send(packet).await;
                }
                _ = token.cancelled() => {
                    return
                }
            }
        }
    }

    async fn send(&self, _request: Request) {
        todo!()
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
