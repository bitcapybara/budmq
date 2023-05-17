mod reader;
mod writer;

use std::{
    io,
    net::SocketAddr,
    ops::DerefMut,
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc,
    },
};

use bud_common::{id::SerialId, mtls::MtlsProvider};
use log::trace;
use s2n_quic::client;
use tokio::sync::{mpsc, oneshot, Mutex};
use tokio_util::sync::CancellationToken;

use crate::consumer::ConsumeMessage;

pub type Result<T> = std::result::Result<T, Error>;

#[derive(Debug)]
pub enum Error {
    Disconnect,
    Canceled,
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

impl From<io::Error> for Error {
    fn from(_e: io::Error) -> Self {
        todo!()
    }
}

impl From<s2n_quic::provider::StartError> for Error {
    fn from(_e: s2n_quic::provider::StartError) -> Self {
        todo!()
    }
}

impl From<s2n_quic::connection::Error> for Error {
    fn from(_e: s2n_quic::connection::Error) -> Self {
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
        sender: mpsc::UnboundedSender<ConsumeMessage>,
    },
    /// unregister consumer from client reader
    DelConsumer { consumer_id: u64 },
}

pub enum ConnectionState {
    Connected(Arc<Connection>),
    Connecting(Vec<oneshot::Sender<Result<Arc<Connection>>>>),
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

impl Connection {
    fn new() -> Self {
        // create reader from s2n_quic::Acceptor
        // create writer from s2n_quic::Handle
        todo!()
    }

    fn clone_one(&self, _ordered: bool) -> Self {
        // create writer from s2n_quic::Handle
        todo!()
    }

    fn is_valid(&self) -> bool {
        !self.error.is_set()
    }
}

/// connection manager held client
pub struct ConnectionHandle {
    addr: SocketAddr,
    server_name: String,
    provider: MtlsProvider,
    conn: Arc<Mutex<ConnectionState>>,
}

impl ConnectionHandle {
    pub fn new(_addr: &SocketAddr) -> Self {
        todo!()
    }

    /// get the connection in manager, setup a new writer
    pub async fn get_connection(&self, _ordered: bool) -> Result<Arc<Connection>> {
        let rx = {
            let mut conn = self.conn.lock().await;
            match conn.deref_mut() {
                ConnectionState::Connected(c) => {
                    if c.is_valid() {
                        return Ok(c.clone());
                    } else {
                        None
                    }
                }
                ConnectionState::Connecting(waiters) => {
                    let (tx, rx) = oneshot::channel();
                    waiters.push(tx);
                    Some(rx)
                }
            }
        };

        match rx {
            Some(rx) => match rx.await {
                Ok(conn) => conn,
                Err(_) => Err(Error::Canceled),
            },
            None => self.connect().await,
        }
    }

    async fn connect(&self) -> Result<Arc<Connection>> {
        let new_conn = match self.connect_inner().await {
            Ok(c) => c,
            Err(e) => {
                let mut conn = self.conn.lock().await;
                if let ConnectionState::Connecting(waiters) = conn.deref_mut() {
                    for tx in waiters.drain(..) {
                        tx.send(Err(Error::Canceled)).ok();
                    }
                }
                return Err(e);
            }
        };
        let mut conn = self.conn.lock().await;
        match conn.deref_mut() {
            ConnectionState::Connected(c) => *c = new_conn.clone(),
            ConnectionState::Connecting(waiters) => {
                for tx in waiters.drain(..) {
                    tx.send(Ok(new_conn.clone())).ok();
                }
            }
        }
        Ok(new_conn)
    }

    async fn connect_inner(&self) -> Result<Arc<Connection>> {
        // TODO loop retry backoff
        // unwrap safe: with_tls error is infallible
        trace!("connector::connect: create client and connect");
        let client: client::Client = client::Client::builder()
            .with_tls(self.provider.clone())
            .unwrap()
            .with_io("0.0.0.0:0")?
            .start()?;
        let connector =
            s2n_quic::client::Connect::new(self.addr).with_server_name(self.server_name.as_str());
        let mut connection = client.connect(connector).await?;
        connection.keep_alive(true)?;
        Ok(Arc::new(Connection::new()))
    }
}

/// cancel token, don't need to wait all spawned reader/writer tasks exit
impl Drop for Connection {
    fn drop(&mut self) {
        self.token.cancel()
    }
}
