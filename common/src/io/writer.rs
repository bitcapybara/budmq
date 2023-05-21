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

pub struct Request {
    pub packet: Packet,
    pub res_tx: Option<oneshot::Sender<Result<Packet>>>,
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

#[derive(Clone)]
pub struct ResMap(Arc<tokio::sync::Mutex<HashMap<u64, oneshot::Sender<Result<Packet>>>>>);

impl ResMap {
    async fn add_res_tx(&self, id: u64, res_tx: oneshot::Sender<Result<Packet>>) {
        let mut map = self.0.lock().await;
        map.retain(|_, s| !s.is_closed());
        map.insert(id, res_tx);
    }

    async fn remove_res_tx(&self, id: u64) -> Option<oneshot::Sender<Result<Packet>>> {
        let mut map = self.0.lock().await;
        map.retain(|_, s| !s.is_closed());
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
            res_map: ResMap::default(),
            token,
            error,
        }
    }

    pub async fn run(mut self) {
        // TODO set sharederror when error occurs
        loop {
            select! {
                res = self.request_rx.recv() => {
                    let Some(request) = res else {
                        return
                    };
                    // framed will recycled after send packet
                    let mut pooled = match self.create().await {
                        Ok(stream) => stream,
                        Err(e) => {
                            self.error.set(e).await;
                            return
                        }
                    };
                    let Request { packet, res_tx } = request;
                    match (packet.request_id(), res_tx) {
                        (Some(id), Some(res_tx)) =>  {
                            self.res_map.add_res_tx(id, res_tx).await;
                            if let Err(e) = pooled.send(packet).await {
                                pooled.set_error(Error::Send(format!("send packet error: {e}"))).await;
                                if let Some(res_tx) = self.res_map.remove_res_tx(id).await {
                                    res_tx.send(Err(e.into())).ok();
                                }
                            }
                        }
                        _ => {
                            if let Err(e) = pooled.send(packet).await {
                                pooled.set_error(Error::Send(format!("send packet error: {e}"))).await;
                                error!("io::writer send packet error: {e}")
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

    async fn create(&mut self) -> Result<PooledStream<T>> {
        match self.inner.get() {
            Some(stream) => Ok(PooledStream::new_idle(self.inner.clone(), stream)),
            None => {
                let stream = self.handle.open_bidirectional_stream().await?;
                let (recv_stream, send_stream) = stream.split();
                let framed = FramedWrite::new(send_stream, PacketCodec);
                let error = SharedError::new();
                tokio::spawn(start_recv(
                    self.res_map.clone(),
                    FramedRead::new(recv_stream, PacketCodec),
                    error.clone(),
                ));
                Ok(PooledStream::new(self.inner.clone(), framed, error))
            }
        }
    }
}

async fn start_recv(
    res_map: ResMap,
    mut framed: FramedRead<ReceiveStream, PacketCodec>,
    error: SharedError,
) {
    while let Some(packet_res) = framed.next().await {
        let packet = match packet_res {
            Ok(packet) => packet,
            Err(e) => {
                error.set(e.into()).await;
                return;
            }
        };
        let Some(request_id) = packet.request_id() else {
            continue;
        };
        if let Some(res_tx) = res_map.remove_res_tx(request_id).await {
            res_tx.send(Ok(packet)).ok();
        }
    }
}

/// used in pool
pub struct IdleStream {
    framed: FramedWrite<SendStream, PacketCodec>,
    error: SharedError,
}

impl IdleStream {
    fn new(framed: FramedWrite<SendStream, PacketCodec>, error: SharedError) -> Self {
        Self { framed, error }
    }
}

/// use by users
pub struct PooledStream<T: PoolRecycle> {
    pool: T,
    stream: Option<IdleStream>,
}

impl<T: PoolRecycle> PooledStream<T> {
    fn new(pool: T, framed: FramedWrite<SendStream, PacketCodec>, error: SharedError) -> Self {
        Self::new_idle(pool, IdleStream::new(framed, error))
    }

    fn new_idle(pool: T, stream: IdleStream) -> Self {
        Self {
            pool,
            stream: Some(stream),
        }
    }

    async fn set_error(&mut self, error: Error) {
        let Some(stream) = &mut self.stream else {
            return
        };
        stream.error.set(error).await;
        stream.framed.close().await.ok();
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
