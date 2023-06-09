use std::sync::{atomic::Ordering, Arc};

use futures::SinkExt;
use parking_lot::Mutex;

use super::{IdleStream, PoolRecycle};

#[derive(Clone)]
pub struct PoolInner {
    idle_streams: Arc<Mutex<Vec<IdleStream>>>,
}

impl Default for PoolInner {
    fn default() -> Self {
        Self {
            idle_streams: Arc::new(Mutex::new(Vec::with_capacity(10))),
        }
    }
}

impl PoolRecycle for PoolInner {
    fn get(&self) -> Option<IdleStream> {
        let mut streams = self.idle_streams.lock();
        streams.pop()
    }

    fn put(&self, mut stream: IdleStream) {
        if stream.error.load(Ordering::Relaxed) {
            tokio::spawn(async move {
                stream.framed.close().await.ok();
            });
            return;
        }
        let mut streams = self.idle_streams.lock();
        streams.push(stream);
    }
}
