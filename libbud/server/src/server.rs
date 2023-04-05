use std::{fmt::Display, io, net::SocketAddr};

use libbud_common::mtls::MtlsProvider;
use log::{error, info};
use s2n_quic::{connection, provider, Connection};
use tokio::sync::{mpsc, watch};

use crate::{
    broker::{self, Broker},
    client::Client,
};

type Result<T> = std::result::Result<T, Error>;

#[derive(Debug)]
pub enum Error {
    Io(io::Error),
    StartUp(String),
    Connection(connection::Error),
}

impl std::error::Error for Error {}

impl Display for Error {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Error::Io(e) => write!(f, "I/O error: {e}"),
            Error::StartUp(e) => write!(f, "Start up error: {e}"),
            Error::Connection(e) => write!(f, "Connection error: {e}"),
        }
    }
}

impl From<io::Error> for Error {
    fn from(e: io::Error) -> Self {
        Self::Io(e)
    }
}

impl From<provider::StartError> for Error {
    fn from(e: provider::StartError) -> Self {
        Self::StartUp(e.to_string())
    }
}

impl From<connection::Error> for Error {
    fn from(e: connection::Error) -> Self {
        Self::Connection(e)
    }
}

pub struct Server {
    provider: MtlsProvider,
    addr: SocketAddr,
    client_id_gen: u64,
}

impl Server {
    pub fn new(provider: MtlsProvider, addr: SocketAddr) -> Self {
        Self {
            provider,
            addr,
            client_id_gen: 0,
        }
    }

    pub async fn start(mut self, close_rx: watch::Receiver<()>) -> Result<()> {
        let mut server = s2n_quic::Server::builder()
            .with_tls(self.provider)
            .unwrap()
            .with_io(self.addr)?
            .start()?;

        let (broker_tx, broker_rx) = mpsc::unbounded_channel();
        let broker = Broker::new();
        let broker_handle = tokio::spawn(broker.run(broker_rx, close_rx));

        // one connection per client
        while let Some(conn) = server.accept().await {
            let local = conn.local_addr()?.to_string();
            let client_id = self.client_id_gen;
            self.client_id_gen += 1;
            // start client
            let task = Self::handle_conn(local, client_id, conn, broker_tx.clone());
            tokio::spawn(task);
        }

        match broker_handle.await {
            Ok(_) => info!("broker exit successfully"),
            Err(e) => error!("broker panic: {e}"),
        }
        Ok(())
    }

    async fn handle_conn(
        local: String,
        client_id: u64,
        conn: Connection,
        broker_tx: mpsc::UnboundedSender<broker::ClientMessage>,
    ) {
        match Client::handshake(client_id, conn, broker_tx).await {
            Ok(client) => {
                if let Err(e) = client.start().await {
                    error!("handle connection from {local} error: {e}");
                }
            }
            Err(e) => {
                error!("connection from {local} handshake error: {e}");
            }
        }
    }
}
