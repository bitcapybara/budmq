use std::sync::{
    atomic::{AtomicBool, Ordering},
    Arc,
};

use tokio::sync::Mutex;

use crate::protocol;

pub mod reader;
pub mod writer;

pub type Result<T> = std::result::Result<T, Error>;

#[derive(Debug, thiserror::Error)]
pub enum Error {
    /// set in SharedError, indicate Connection Disconnect
    #[error("Connection closed")]
    ConnectionDisconnect,
    /// send to senders(client/server), indicate Stream Disconnect
    #[error("Stream disconnect")]
    StreamDisconnect,
    // send to senders(client/server), indicate Protocol error
    #[error("Protocol error: {0}")]
    Protocol(#[from] protocol::Error),
}

#[derive(Clone)]
pub struct SharedError {
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

    pub async fn set_disconnect(&self) {
        let mut lock = self.error.lock().await;
        *lock = Some(Error::ConnectionDisconnect);
        self.error_set.store(true, Ordering::Release);
    }
}

impl Default for SharedError {
    fn default() -> Self {
        Self::new()
    }
}
