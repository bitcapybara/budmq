use std::time::Duration;

use futures::{SinkExt, StreamExt};
use log::error;
use s2n_quic::connection::Handle;
use tokio::{
    select,
    sync::{mpsc, oneshot},
    time::{timeout, Instant},
};
use tokio_util::sync::CancellationToken;

use super::{
    pool::{Pool, PooledStream},
    Error, Result,
};
use crate::protocol::{Packet, ReturnCode};

const DEFAULT_WAIT_REPLY_TIMEOUT: Duration = Duration::from_millis(200);

struct Request {
    packet: Packet,
    res_tx: oneshot::Sender<Result<()>>,
}

pub struct WriterBuilder {
    handle: Handle,
    wait_reply_timeout: Option<Duration>,
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
            wait_reply_timeout: None,
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

    pub fn wait_reply_timeout(mut self, timeout: Duration) -> Self {
        self.wait_reply_timeout = Some(timeout);
        self
    }

    pub fn build(self) -> (Writer, Sender) {
        let (tx, rx) = mpsc::channel(1);
        let sender = Sender::new(tx);
        let pool = Pool::new(self.handle);
        let wait_reply_timeout = self
            .wait_reply_timeout
            .unwrap_or(DEFAULT_WAIT_REPLY_TIMEOUT);
        (
            Writer {
                pool,
                ordered: self.ordered,
                rx,
                wait_reply_timeout,
            },
            sender,
        )
    }
}

pub struct Writer {
    pool: Pool,
    ordered: bool,
    rx: mpsc::Receiver<Request>,
    wait_reply_timeout: Duration,
}

impl Writer {
    pub fn builder(handle: Handle) -> WriterBuilder {
        WriterBuilder::new(handle)
    }

    pub async fn run(mut self, token: CancellationToken) {
        loop {
            select! {
                res = self.rx.recv() => {
                    let Some(packet) = res else {
                        token.cancel();
                        return
                    };
                    if let Err(e) = if self.ordered {
                        self.send(packet).await
                    } else {
                        self.send_async(packet).await
                    } {
                        error!("error occurs while sending packet: {e}")
                    }
                }
                _ = token.cancelled() => {
                    return
                }
            }
        }
    }

    async fn send_async(&mut self, request: Request) -> Result<()> {
        let framed = self.pool.get().await?;
        let duration = self.wait_reply_timeout;
        tokio::spawn(Self::send_and_wait(framed, duration, request));
        Ok(())
    }

    async fn send(&mut self, request: Request) -> Result<()> {
        let framed = self.pool.get().await?;
        let duration = self.wait_reply_timeout;
        Self::send_and_wait(framed, duration, request).await;
        Ok(())
    }

    async fn send_and_wait(mut framed: PooledStream, duration: Duration, request: Request) {
        let Request { packet, res_tx } = request;
        if let Err(e) = framed.send(packet).await {
            res_tx.send(Err(e.into())).ok();
            return;
        }
        let reply = match timeout(duration, framed.next()).await {
            Ok(Some(Ok(Packet::Response(ReturnCode::Success)))) => Ok(()),
            Ok(Some(Ok(Packet::Response(code)))) => Err(Error::FromServer(code)),
            Ok(Some(Ok(_))) => Err(Error::ReceivedUnexpectedPacket),
            Ok(Some(Err(e))) => Err(Error::Protocol(e)),
            Ok(None) => Err(Error::StreamClosed),
            Err(_) => Err(Error::WaitReplyTimeout),
        };
        res_tx.send(reply).ok();
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
