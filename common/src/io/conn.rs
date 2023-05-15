use std::{net::SocketAddr, sync::Arc};

use tokio::sync::{oneshot, Mutex};

pub enum ConnectionState {
    Connected(Arc<Connection>),
    Connecting(Option<oneshot::Receiver<Arc<Connection>>>),
}

/// hold by producer/consumer
/// may be used by one producer/consumer, or multi
pub struct Connection {
    conn: s2n_quic::Connection,
}

/// connection manager for client
pub struct ConnectionHandle {
    addr: SocketAddr,
    conn: Arc<Mutex<ConnectionState>>,
}

impl ConnectionHandle {
    pub fn new(_addr: &SocketAddr) -> Self {
        todo!()
    }

    pub async fn get_connection(&self) -> Arc<Connection> {
        todo!()
    }
}
