use std::sync::{
    atomic::{AtomicBool, Ordering},
    Arc,
};

use s2n_quic::connection;
use tokio::{
    sync::{mpsc, oneshot, Mutex},
    time,
};

use crate::protocol::{self, ReturnCode};

pub mod reader;
pub mod writer;

pub type Result<T> = std::result::Result<T, Error>;

#[derive(Debug)]
pub enum Error {
    FromPeer(ReturnCode),
    Send(String),
    ResDropped(oneshot::error::RecvError),
    Timeout,
    Connection(connection::Error),
    ConnectionClosed,
    StreamDisconnect,
    Protocol(protocol::Error),
}

impl std::error::Error for Error {}

impl std::fmt::Display for Error {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Error::FromPeer(e) => write!(f, "error from peer: {e}"),
            Error::Send(e) => write!(f, "message send error: {e}"),
            Error::ResDropped(e) => write!(f, "res tx dropped without send: {e}"),
            Error::Timeout => write!(f, "wait for message timeout"),
            Error::Connection(e) => write!(f, "connection error: {e}"),
            Error::Protocol(e) => write!(f, "protocol error: {e}"),
            Error::ConnectionClosed => write!(f, "connection closed without error"),
            Error::StreamDisconnect => write!(f, "stream disconnect"),
        }
    }
}

impl<T> From<mpsc::error::SendError<T>> for Error {
    fn from(e: mpsc::error::SendError<T>) -> Self {
        Self::Send(e.to_string())
    }
}

impl From<oneshot::error::RecvError> for Error {
    fn from(e: oneshot::error::RecvError) -> Self {
        Self::ResDropped(e)
    }
}

impl From<time::error::Elapsed> for Error {
    fn from(_: time::error::Elapsed) -> Self {
        Self::Timeout
    }
}

impl From<connection::Error> for Error {
    fn from(e: connection::Error) -> Self {
        Self::Connection(e)
    }
}

impl From<protocol::Error> for Error {
    fn from(e: protocol::Error) -> Self {
        Self::Protocol(e)
    }
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
