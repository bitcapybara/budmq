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
    collections::HashMap,
    ops::{Deref, DerefMut},
    sync::Arc,
};

use futures::{SinkExt, StreamExt};
use log::error;
use s2n_quic::{
    connection::Handle,
    stream::{ReceiveStream, SendStream},
};
use tokio::{
    sync::{mpsc, oneshot, Mutex},
    task::JoinSet,
};
use tokio_util::{
    codec::{FramedRead, FramedWrite},
    sync::CancellationToken,
};

use super::{Error, Result};
use crate::protocol::{Packet, PacketCodec, Response, ReturnCode};

pub mod pool;
pub mod single;

pub struct Request {
    pub packet: Packet,
    pub res_tx: oneshot::Sender<Result<()>>,
}

pub struct PoolSender(mpsc::Sender<Request>);

impl PoolSender {
    pub async fn send(&self, request: Request) -> Result<()> {
        self.0.send(request).await?;
        Ok(())
    }
}

pub struct PoolCloser {
    tasks: Arc<Mutex<JoinSet<()>>>,
    token: CancellationToken,
}

impl PoolCloser {
    pub fn new(tasks: Arc<Mutex<JoinSet<()>>>, token: CancellationToken) -> Self {
        Self { tasks, token }
    }

    pub async fn close(self) {
        let mut tasks = self.tasks.lock().await;
        while tasks.join_next().await.is_some() {}
    }
}

#[derive(Clone)]
pub struct ResMap(Arc<tokio::sync::Mutex<HashMap<u64, oneshot::Sender<Result<()>>>>>);

impl ResMap {
    async fn add_res_tx(&self, id: u64, res_tx: oneshot::Sender<Result<()>>) {
        let mut map = self.0.lock().await;
        map.insert(id, res_tx);
    }

    async fn remove_res_tx(&self, id: u64) -> Option<oneshot::Sender<Result<()>>> {
        let mut map = self.0.lock().await;
        map.remove(&id)
    }
}

impl Default for ResMap {
    fn default() -> Self {
        Self(Arc::new(tokio::sync::Mutex::new(HashMap::new())))
    }
}

pub trait PoolRecycle: Default + Clone {
    fn get(&self) -> Option<IdleStream>;
    fn put(&self, stream: IdleStream);
}

pub struct StreamPool<T: PoolRecycle> {
    handle: Handle,
    res_map: ResMap,
    inner: T,
    request_rx: mpsc::Receiver<Request>,
    tasks: Arc<Mutex<JoinSet<()>>>,
    token: CancellationToken,
}

impl<T: PoolRecycle> StreamPool<T> {
    pub fn new(handle: Handle, token: CancellationToken) -> (Self, PoolSender, PoolCloser) {
        let (request_tx, request_rx) = mpsc::channel(1);
        let tasks = Arc::new(Mutex::new(JoinSet::new()));
        (
            Self {
                inner: T::default(),
                request_rx,
                handle,
                res_map: ResMap::default(),
                tasks: tasks.clone(),
                token: token.clone(),
            },
            PoolSender(request_tx),
            PoolCloser::new(tasks, token.child_token()),
        )
    }

    pub async fn run(mut self) {
        while let Some(request) = self.request_rx.recv().await {
            // framed will recycled after send packet
            let mut framed = match self.create().await {
                Ok(stream) => stream,
                Err(e) => {
                    error!("pool get stream error: {e}");
                    continue;
                }
            };
            let Request { packet, res_tx } = request;
            match packet.request_id() {
                Some(id) => {
                    self.res_map.add_res_tx(id, res_tx).await;
                    if let Err(e) = framed.send(packet).await {
                        if let Some(res_tx) = self.res_map.remove_res_tx(id).await {
                            res_tx.send(Err(e.into())).ok();
                        }
                    }
                }
                None => {
                    if let Err(e) = framed.send(packet).await {
                        error!("io::writer send packet error: {e}")
                    }
                }
            }
        }
    }

    async fn create(&mut self) -> Result<PooledStream<T>> {
        match self.inner.get() {
            Some(stream) => Ok(PooledStream::new_idle(self.inner.clone(), stream)),
            None => {
                let stream = self.handle.open_bidirectional_stream().await?;
                let (recv_stream, send_stream) = stream.split();
                let framed = FramedWrite::new(send_stream, PacketCodec);
                let mut tasks = self.tasks.lock().await;
                tasks.spawn(start_recv(
                    self.res_map.clone(),
                    FramedRead::new(recv_stream, PacketCodec),
                ));
                Ok(PooledStream::new(self.inner.clone(), framed))
            }
        }
    }
}

async fn start_recv(res_map: ResMap, mut framed: FramedRead<ReceiveStream, PacketCodec>) {
    while let Some(packet_res) = framed.next().await {
        let (id, resp) = match packet_res {
            Ok(Packet::Response(Response { request_id, code })) => {
                if code == ReturnCode::Success {
                    (request_id, Ok(()))
                } else {
                    (request_id, Err(Error::FromPeer(code)))
                }
            }
            Ok(_) => {
                error!("io::writer received unexpected packet, execpted RESPONSE");
                continue;
            }
            Err(e) => {
                error!("io::writer decode protocol error: {e}");
                continue;
            }
        };
        if let Some(res_tx) = res_map.remove_res_tx(id).await {
            res_tx.send(resp).ok();
        }
    }
}

/// used in pool
pub struct IdleStream {
    framed: FramedWrite<SendStream, PacketCodec>,
    /// statistics
    used_count: u64,
}

impl IdleStream {
    fn new(framed: FramedWrite<SendStream, PacketCodec>) -> Self {
        Self {
            framed,
            used_count: 0,
        }
    }
}

/// use by users
pub struct PooledStream<T: PoolRecycle> {
    pool: T,
    stream: Option<IdleStream>,
}

impl<T: PoolRecycle> PooledStream<T> {
    fn new(pool: T, framed: FramedWrite<SendStream, PacketCodec>) -> Self {
        Self::new_idle(pool, IdleStream::new(framed))
    }

    fn new_idle(pool: T, stream: IdleStream) -> Self {
        Self {
            pool,
            stream: Some(stream),
        }
    }
}

impl<T: PoolRecycle> Drop for PooledStream<T> {
    fn drop(&mut self) {
        self.pool.put(self.stream.take().unwrap())
    }
}

impl<T: PoolRecycle> Deref for PooledStream<T> {
    type Target = FramedWrite<SendStream, PacketCodec>;

    fn deref(&self) -> &Self::Target {
        &self.stream.as_ref().unwrap().framed
    }
}

impl<T: PoolRecycle> DerefMut for PooledStream<T> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.stream.as_mut().unwrap().framed
    }
}
