use std::sync::Arc;

use futures::SinkExt;
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

    fn put(&self, mut idle_stream: IdleStream) {
        let mut stream = self.stream.lock();
        if idle_stream.error.is_set() {
            tokio::spawn(async move {
                idle_stream.framed.close().await.ok();
            });
            stream.take();
            return;
        }
        stream.replace(idle_stream);
    }
}
