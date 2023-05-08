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

    pub async fn get(&self) -> Result<BidirectionalStream> {
        self.0.get().await
    }
}

#[derive(Clone)]
struct PoolInner {
    handle: Handle,
    idle_conns: Arc<Mutex<Vec<PooledStream>>>,
}

impl PoolInner {
    fn new(handle: Handle) -> Self {
        Self {
            handle,
            idle_conns: Arc::new(Mutex::new(Vec::with_capacity(10))),
        }
    }

    async fn get(&self) -> Result<BidirectionalStream> {
        todo!()
    }

    fn put(&self, _stream: Option<BidirectionalStream>) {
        todo!()
    }
}

struct PooledStream {
    pool: PoolInner,
    stream: Option<BidirectionalStream>,
}

impl Drop for PooledStream {
    fn drop(&mut self) {
        self.pool.put(self.stream.take())
    }
}

impl Deref for PooledStream {
    type Target = BidirectionalStream;

    fn deref(&self) -> &Self::Target {
        self.stream.as_ref().unwrap()
    }
}

impl DerefMut for PooledStream {
    fn deref_mut(&mut self) -> &mut Self::Target {
        self.stream.as_mut().unwrap()
    }
}
