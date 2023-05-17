mod reader;
mod writer;

use std::{
    net::SocketAddr,
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc,
    },
};

use bud_common::{id::SerialId, protocol::Packet};
use tokio::sync::{mpsc, oneshot, Mutex};
use tokio_util::sync::CancellationToken;

pub type Result<T> = std::result::Result<T, Error>;

#[derive(Debug)]
pub enum Error {
    CommonIo(bud_common::io::Error),
}

impl std::error::Error for Error {}

impl std::fmt::Display for Error {
    fn fmt(&self, _f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        todo!()
    }
}

impl From<bud_common::io::Error> for Error {
    fn from(_e: bud_common::io::Error) -> Self {
        todo!()
    }
}

impl<T> From<mpsc::error::SendError<T>> for Error {
    fn from(_e: mpsc::error::SendError<T>) -> Self {
        todo!()
    }
}

impl From<oneshot::error::RecvError> for Error {
    fn from(_e: oneshot::error::RecvError) -> Self {
        todo!()
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

pub enum Event {
    /// register consumer to client reader
    AddConsumer {
        consumer_id: u64,
        /// sender held by client reader
        /// receiver held by client consumer
        sender: mpsc::UnboundedSender<Packet>,
    },
    /// unregister consumer from client reader
    DelConsumer { consumer_id: u64 },
}

pub enum ConnectionState {
    Connected(Arc<Connection>),
    Connecting(Option<oneshot::Receiver<Arc<Connection>>>),
}

/// held by producer/consumer
pub struct Connection {
    /// use sender to send all packets
    request_id: SerialId,
    event_tx: mpsc::UnboundedSender<Event>,
    /// error to indicate weather producer/consumer need to get a new connection
    error: SharedError,
    /// self-managed token
    token: CancellationToken,
}

/// connection manager held client
pub struct ConnectionHandle {
    addr: SocketAddr,
    conn: Arc<Mutex<ConnectionState>>,
}

impl ConnectionHandle {
    pub fn new(_addr: &SocketAddr) -> Self {
        // create reader from s2n_quic::Acceptor
        todo!()
    }

    /// get the connection in manager, setup a new writer
    pub async fn get_connection(&self, _ordered: bool) -> Arc<Connection> {
        // create writer from s2n_quic::Handle
        todo!()
    }
}

/// cancel token, don't need to wait all spawned reader/writer tasks exit
impl Drop for Connection {
    fn drop(&mut self) {
        self.token.cancel()
    }
}
