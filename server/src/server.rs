use std::{fmt::Display, io, net::SocketAddr};

use bud_common::{helper::wait, mtls::MtlsProvider, storage::Storage};
use futures::future;
use log::{error, trace};
use s2n_quic::{connection, provider, Connection};
use tokio::{select, sync::mpsc};
use tokio_util::sync::CancellationToken;

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
    token: CancellationToken,
}

impl Server {
    pub fn new(provider: MtlsProvider, addr: SocketAddr) -> (CancellationToken, Self) {
        let token = CancellationToken::new();
        (
            token.clone(),
            Self {
                provider,
                addr,
                token,
            },
        )
    }

    pub async fn start<S: Storage>(self, storage: S) -> Result<()> {
        let token = self.token.child_token();
        // start broker loop
        trace!("server::start: start broker task");
        let (broker_tx, broker_rx) = mpsc::unbounded_channel();
        let broker_task = Broker::new(storage).run(broker_rx, token.clone());
        let broker_handle = tokio::spawn(broker_task);

        // start server loop
        // unwrap: with_tls error is infallible
        trace!("server::start: start accept task");
        let server = s2n_quic::Server::builder()
            .with_tls(self.provider)
            .unwrap()
            .with_io(self.addr)?
            .start()?;
        let server_task = Self::handle_accept(server, broker_tx, token.clone());
        let server_handle = tokio::spawn(server_task);

        // wait for tasks done
        future::join(
            // wait for broker
            wait(broker_handle, "broker task loop"),
            // wait for server
            wait(server_handle, "server task loop"),
        )
        .await;
        trace!("server::start: server loop exit");
        Ok(())
    }

    async fn handle_accept(
        mut server: s2n_quic::Server,
        broker_tx: mpsc::UnboundedSender<broker::ClientMessage>,
        token: CancellationToken,
    ) {
        let mut client_id_gen = 0;
        loop {
            select! {
                conn = server.accept() => {
                    let Some(conn) = conn else {
                        token.cancel();
                        return
                    };
                    trace!("server::handle_accept: accept a new connection");
                    let client_addr = match conn.local_addr() {
                        Ok(addr) => addr.to_string(),
                        Err(e) => {
                            error!("get client addr error: {e}");
                            continue;
                        }
                    };
                    let client_id = client_id_gen;
                    client_id_gen += 1;
                    // start client
                    let task = Self::handle_conn(client_addr, client_id, conn, broker_tx.clone());
                    tokio::spawn(task);
                }
                _ = token.cancelled() => {
                    return
                }
            }
        }
    }

    async fn handle_conn(
        local: String,
        client_id: u64,
        conn: Connection,
        broker_tx: mpsc::UnboundedSender<broker::ClientMessage>,
    ) {
        trace!("server::handle_conn: waiting for handshake");
        match Client::handshake(client_id, conn, broker_tx).await {
            Ok(client) => {
                trace!("server::handle_conn: start client");
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
