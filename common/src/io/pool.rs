use std::{
    ops::{Deref, DerefMut},
    sync::Arc,
};

use parking_lot::Mutex;
use s2n_quic::{connection::Handle, stream::BidirectionalStream};

use super::Result;

#[derive(Clone)]
pub struct Pool(PoolInner);

impl Pool {
    pub fn new(handle: Handle) -> Self {
        Self(PoolInner::new(handle))
    }

    pub async fn get(&mut self) -> Result<PooledStream> {
        self.0.get().await
    }
}

#[derive(Clone)]
struct PoolInner {
    handle: Handle,
    idle_streams: Arc<Mutex<Vec<IdleStream>>>,
}

impl PoolInner {
    fn new(handle: Handle) -> Self {
        Self {
            handle,
            idle_streams: Arc::new(Mutex::new(Vec::with_capacity(10))),
        }
    }

    async fn get(&mut self) -> Result<PooledStream> {
        match self.get_one() {
            Some(stream) => Ok(PooledStream::new_idle(self.clone(), stream)),
            None => {
                let stream = self.handle.open_bidirectional_stream().await?;
                Ok(PooledStream::new(self.clone(), stream))
            }
        }
    }

    fn get_one(&mut self) -> Option<IdleStream> {
        let mut streams = self.idle_streams.lock();
        streams.pop()
    }

    fn put(&self, stream: IdleStream) {
        let mut streams = self.idle_streams.lock();
        streams.push(stream)
    }
}

/// used in pool
pub struct IdleStream {
    stream: BidirectionalStream,
    /// statistics
    used_count: u64,
}

impl IdleStream {
    fn new(stream: BidirectionalStream) -> Self {
        Self {
            stream,
            used_count: 0,
        }
    }
}

/// use by users
pub struct PooledStream {
    pool: PoolInner,
    stream: Option<IdleStream>,
}

impl PooledStream {
    fn new(pool: PoolInner, stream: BidirectionalStream) -> Self {
        Self::new_idle(pool, IdleStream::new(stream))
    }

    fn new_idle(pool: PoolInner, stream: IdleStream) -> Self {
        Self {
            pool,
            stream: Some(stream),
        }
    }
}

impl Drop for PooledStream {
    fn drop(&mut self) {
        self.pool.put(self.stream.take().unwrap())
    }
}

impl Deref for PooledStream {
    type Target = BidirectionalStream;

    fn deref(&self) -> &Self::Target {
        &self.stream.as_ref().unwrap().stream
    }
}

impl DerefMut for PooledStream {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.stream.as_mut().unwrap().stream
    }
}
