//! Writer:
//! 1. get() a stream sender from pool
//! 2. send packet, recycle to pool
//! 3. wait for response on this stream receiver
//!
//! Reader:
//! 1. accept() a stream
//! 2. recv a packet from stream
//! 3. send packet to observer
//! 4. wait for response (not for PING/DISCONNECT packet)
//! 5. send response to this stream

use std::{
    ops::{Deref, DerefMut},
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc,
    },
};

use futures::{SinkExt, StreamExt};
use log::error;
use s2n_quic::{
    connection::Handle,
    stream::{ReceiveStream, SendStream},
};
use tokio::{
    select,
    sync::{mpsc, oneshot},
};
use tokio_util::{
    codec::{FramedRead, FramedWrite},
    sync::CancellationToken,
};

use self::{pool::PoolInner, single::SingleInner};

use super::{Error, Result, SharedError};
use crate::protocol::{Packet, PacketCodec};

pub mod pool;
pub mod single;

pub enum ResultWaiter {
    Sync(oneshot::Sender<Result<Packet>>),
    Async(oneshot::Sender<Result<()>>),
}

impl ResultWaiter {
    pub fn send_err(self, error: Error) {
        match self {
            ResultWaiter::Sync(res_tx) => res_tx.send(Err(error)).ok(),
            ResultWaiter::Async(res_tx) => res_tx.send(Err(error)).ok(),
        };
    }
}

pub struct Request {
    /// packet send to peer
    pub packet: Packet,
    /// result notify
    pub res_tx: ResultWaiter,
}

pub fn new_pool(
    handle: Handle,
    rx: mpsc::Receiver<Request>,
    ordered: bool,
    error: SharedError,
    token: CancellationToken,
) {
    if ordered {
        let pool = StreamPool::<SingleInner>::new(handle, rx, error, token.child_token());
        tokio::spawn(pool.run());
    } else {
        let pool = StreamPool::<PoolInner>::new(handle, rx, error, token.child_token());
        tokio::spawn(pool.run());
    }
}

pub trait PoolRecycle: Default + Clone {
    fn get(&self) -> Option<IdleStream>;
    fn put(&self, stream: IdleStream);
}

pub struct StreamPool<T: PoolRecycle> {
    handle: Handle,
    inner: T,
    error: SharedError,
    request_rx: mpsc::Receiver<Request>,
    token: CancellationToken,
}

impl<T: PoolRecycle> StreamPool<T> {
    pub fn new(
        handle: Handle,
        rx: mpsc::Receiver<Request>,
        error: SharedError,
        token: CancellationToken,
    ) -> Self {
        Self {
            inner: T::default(),
            request_rx: rx,
            handle,
            token,
            error,
        }
    }

    pub async fn run(mut self) {
        loop {
            select! {
                res = self.request_rx.recv() => {
                    let Some(Request { packet, res_tx }) = res else {
                        return
                    };
                    // framed will recycled after send packet
                    let mut pooled = match self.create().await {
                        Some(stream) => stream,
                        None => {
                            self.error.set_disconnect().await;
                            res_tx.send_err(Error::ConnectionDisconnect);
                            return
                        }
                    };
                    match res_tx {
                        ResultWaiter::Sync(res_tx) => {
                            // send to server
                            if let Err(e) = pooled.framed.send(packet).await {
                                pooled.set_error().await;
                                res_tx.send(Err(e.into())).ok();
                                continue;
                            }
                            // wait for reply
                            pooled.res_sender.send(res_tx).await.ok();
                        },
                        ResultWaiter::Async(res_tx) => {
                            if let Err(e) = pooled.framed.send(packet).await {
                                pooled.set_error().await;
                                res_tx.send(Err(e.into())).ok();
                                continue;
                            }
                            res_tx.send(Ok(())).ok();
                        },
                    }
                }
                _ = self.token.cancelled() => {
                    return
                }
            }
        }
    }

    async fn create(&mut self) -> Option<PooledStream<T>> {
        match self.inner.get() {
            Some(stream) => Some(PooledStream::new_idle(self.inner.clone(), stream)),
            None => {
                let stream = match self.handle.open_bidirectional_stream().await {
                    Ok(stream) => stream,
                    Err(e) => {
                        error!("Open QUIC stream error: {e}");
                        return None;
                    }
                };
                let (recv_stream, send_stream) = stream.split();
                let framed = FramedWrite::new(send_stream, PacketCodec);
                let error = Arc::new(AtomicBool::new(false));
                // TODO back pressure for max inflight requests
                let (res_sender, res_receiver) = mpsc::channel(100);
                tokio::spawn(start_recv(
                    res_receiver,
                    FramedRead::new(recv_stream, PacketCodec),
                    error.clone(),
                    self.token.child_token(),
                ));
                Some(PooledStream::new(
                    self.inner.clone(),
                    res_sender,
                    framed,
                    error,
                ))
            }
        }
    }
}

async fn start_recv(
    mut res_receiver: mpsc::Receiver<oneshot::Sender<Result<Packet>>>,
    mut framed: FramedRead<ReceiveStream, PacketCodec>,
    error: Arc<AtomicBool>,
    token: CancellationToken,
) {
    loop {
        select! {
            res_tx = res_receiver.recv() => {
                let Some(res_tx) = res_tx else {
                    return;
                };
                match framed.next().await {
                    Some(Ok(resp)) => {
                        res_tx.send(Ok(resp)).ok();
                    }
                    Some(Err(e)) => {
                        error.store(true, Ordering::Release);
                        res_tx.send(Err(e.into())).ok();
                    }
                    None => {
                        error.store(true, Ordering::Release);
                        res_tx.send(Err(Error::StreamDisconnect)).ok();
                    }
                }
            }
            _ = token.cancelled() => {
                return
            }
        }
    }
}

/// used in pool
pub struct IdleStream {
    res_sender: mpsc::Sender<oneshot::Sender<Result<Packet>>>,
    framed: FramedWrite<SendStream, PacketCodec>,
    error: Arc<AtomicBool>,
}

impl IdleStream {
    fn new(
        res_sender: mpsc::Sender<oneshot::Sender<Result<Packet>>>,
        framed: FramedWrite<SendStream, PacketCodec>,
        error: Arc<AtomicBool>,
    ) -> Self {
        Self {
            framed,
            error,
            res_sender,
        }
    }
}

/// use by users
pub struct PooledStream<T: PoolRecycle> {
    pool: T,
    stream: Option<IdleStream>,
}

impl<T: PoolRecycle> PooledStream<T> {
    fn new(
        pool: T,
        res_sender: mpsc::Sender<oneshot::Sender<Result<Packet>>>,
        framed: FramedWrite<SendStream, PacketCodec>,
        error: Arc<AtomicBool>,
    ) -> Self {
        Self::new_idle(pool, IdleStream::new(res_sender, framed, error))
    }

    fn new_idle(pool: T, stream: IdleStream) -> Self {
        Self {
            pool,
            stream: Some(stream),
        }
    }

    async fn set_error(&mut self) {
        let Some(stream) = &mut self.stream else {
            return
        };
        stream.error.store(true, Ordering::Release);
    }
}

impl<T: PoolRecycle> Drop for PooledStream<T> {
    fn drop(&mut self) {
        self.pool.put(self.stream.take().unwrap())
    }
}

impl<T: PoolRecycle> Deref for PooledStream<T> {
    type Target = IdleStream;

    fn deref(&self) -> &Self::Target {
        self.stream.as_ref().unwrap()
    }
}

impl<T: PoolRecycle> DerefMut for PooledStream<T> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        self.stream.as_mut().unwrap()
    }
}
