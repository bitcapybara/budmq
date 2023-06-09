use std::sync::{
    atomic::{AtomicBool, Ordering},
    Arc,
};

use s2n_quic::connection;
use tokio::sync::{oneshot, Mutex};

use crate::protocol::{self, ReturnCode};

pub mod reader;
pub mod writer;

pub type Result<T> = std::result::Result<T, Error>;

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("Error from Peer: {0}")]
    FromPeer(ReturnCode),
    #[error("Res tx dropped without send: {0}")]
    ResDropped(#[from] oneshot::error::RecvError),
    #[error("Wait fro message error")]
    Timeout,
    #[error("Connection error: {0}")]
    Connection(#[from] connection::Error),
    #[error("Connection closed")]
    ConnectionClosed,
    #[error("Stream disconnect")]
    StreamDisconnect,
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

    /// used by stream writer, set when error occurs in writing packets
    pub async fn set(&self, error: Error) {
        let mut lock = self.error.lock().await;
        *lock = Some(error);
        self.error_set.store(true, Ordering::Release);
    }
}

impl Default for SharedError {
    fn default() -> Self {
        Self::new()
    }
}
