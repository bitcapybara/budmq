use std::sync::{
    atomic::{AtomicBool, Ordering},
    Arc,
};

use bud_common::io::Error;
use tokio::sync::Mutex;

#[derive(Clone)]
pub(crate) struct SharedError {
    error_set: Arc<AtomicBool>,
    error: Arc<Mutex<Option<Error>>>,
}

impl SharedError {
    pub fn new() -> SharedError {
        SharedError {
            error_set: Arc::new(AtomicBool::new(false)),
            error: Arc::new(Mutex::new(None)),
        }
    }

    /// used by stream reader, check if error need to reconnect
    pub fn is_set(&self) -> bool {
        self.error_set.load(Ordering::Relaxed)
    }

    /// used by stream reader, check if error need to reconnect
    pub async fn remove(&self) -> Option<Error> {
        let mut lock = self.error.lock().await;
        let error = lock.take();
        self.error_set.store(false, Ordering::Release);
        error
    }

    /// used by stream writer, set when error occurs in writing packets
    pub async fn set(&self, error: Error) {
        let mut lock = self.error.lock().await;
        *lock = Some(error);
        self.error_set.store(true, Ordering::Release);
    }
}
