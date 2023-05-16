mod error;

use std::{net::SocketAddr, sync::Arc};

use tokio::sync::{oneshot, Mutex};
use tokio_util::sync::CancellationToken;

use self::error::SharedError;

pub enum ConnectionState {
    Connected(Arc<Connection>),
    Connecting(Option<oneshot::Receiver<Arc<Connection>>>),
}

/// held by producer/consumer
pub struct Connection {
    conn: s2n_quic::Connection,
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
        todo!()
    }

    /// get the connection in manager, setup a new writer
    pub async fn get_connection(&self, _ordered: bool) -> Arc<Connection> {
        todo!()
    }
}

/// cancel token, don't need to wait all spawned reader/writer tasks exit
impl Drop for Connection {
    fn drop(&mut self) {
        self.token.cancel()
    }
}
