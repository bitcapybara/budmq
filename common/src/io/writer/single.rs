use std::sync::Arc;

use parking_lot::Mutex;

use super::{IdleStream, PoolRecycle};

#[derive(Clone)]
pub struct SingleInner {
    stream: Arc<Mutex<Option<IdleStream>>>,
}

impl Default for SingleInner {
    fn default() -> Self {
        Self {
            stream: Arc::new(Mutex::new(None)),
        }
    }
}

impl PoolRecycle for SingleInner {
    fn get(&self) -> Option<IdleStream> {
        let mut stream = self.stream.lock();
        stream.take()
    }

    fn put(&self, idle_stream: super::IdleStream) {
        let mut stream = self.stream.lock();
        if idle_stream.error.is_set() {
            stream.take();
        }
        stream.replace(idle_stream);
    }
}
