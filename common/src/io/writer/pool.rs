use std::sync::Arc;

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

    fn put(&self, stream: IdleStream) {
        if stream.error.is_set() {
            return;
        }
        let mut streams = self.idle_streams.lock();
        streams.push(stream)
    }
}
