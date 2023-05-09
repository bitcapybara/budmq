use std::{
    collections::HashMap,
    ops::{Deref, DerefMut},
    sync::Arc,
};

use s2n_quic::stream::SendStream;
use tokio::sync::{mpsc, oneshot};
use tokio_util::codec::FramedWrite;

use super::Result;
use crate::protocol::{Packet, PacketCodec};

mod helper;
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

pub trait PoolRecycle {
    fn put(&self, stream: IdleStream);
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
