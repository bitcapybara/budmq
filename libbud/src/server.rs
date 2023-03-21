use std::{fmt::Display, io, net::SocketAddr};

use log::error;
use s2n_quic::{connection, provider, Connection};
use tokio::sync::mpsc;

use crate::{broker, client::Client, mtls::MtlsProvider};

type Result<T> = std::result::Result<T, Error>;

#[derive(Debug)]
pub enum Error {
    Io(io::Error),
    StartUp(String),
}

impl std::error::Error for Error {}

impl Display for Error {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        todo!()
    }
}

impl From<io::Error> for Error {
    fn from(e: io::Error) -> Self {
        Error::Io(e)
    }
}

impl From<std::convert::Infallible> for Error {
    fn from(_: std::convert::Infallible) -> Self {
        unreachable!()
    }
}

impl From<provider::StartError> for Error {
    fn from(e: provider::StartError) -> Self {
        Error::StartUp(e.to_string())
    }
}

impl From<connection::Error> for Error {
    fn from(value: connection::Error) -> Self {
        todo!()
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

    pub async fn start(mut self) -> Result<()> {
        let mut server = s2n_quic::Server::builder()
            .with_tls(self.provider)?
            .with_io(self.addr)?
            .start()?;

        let (broker_tx, broker_rx) = mpsc::unbounded_channel();

        // one connection per client
        while let Some(conn) = server.accept().await {
            let local = conn.local_addr()?.to_string();
            let client_id = self.client_id_gen;
            self.client_id_gen += 1;
            // start client
            let task = Self::handle_conn(local, client_id, conn, broker_tx.clone());
            tokio::spawn(task);
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
