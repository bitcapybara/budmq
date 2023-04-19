use std::{fmt::Display, io, net::SocketAddr};

use bud_common::{helper::wait, mtls::MtlsProvider, storage::Storage};
use futures::{stream::FuturesUnordered, StreamExt};
use log::error;
use s2n_quic::{connection, provider, Connection};
use tokio::{
    select,
    sync::{mpsc, oneshot, watch},
};

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
}

impl Server {
    pub fn new(provider: MtlsProvider, addr: SocketAddr) -> Self {
        Self { provider, addr }
    }

    pub async fn start<S: Storage>(
        self,
        storage: S,
        shutdown_rx: oneshot::Receiver<()>,
    ) -> Result<()> {
        //
        let (close_tx, close_rx) = watch::channel(());
        // start broker loop
        let (broker_tx, broker_rx) = mpsc::unbounded_channel();
        let broker_task = Broker::new(storage).run(broker_rx, close_rx.clone());
        let broker_handle = tokio::spawn(broker_task);

        // start server loop
        // unwrap: with_tls error is infallible
        let server = s2n_quic::Server::builder()
            .with_tls(self.provider)
            .unwrap()
            .with_io(self.addr)?
            .start()?;
        let server_task = Self::handle_accept(server, broker_tx, close_rx);
        let server_handle = tokio::spawn(server_task);

        let mut futs = FuturesUnordered::from_iter(vec![
            // wait for broker
            wait(broker_handle, "broker task loop"),
            // wait for server
            wait(server_handle, "server task loop"),
        ]);

        shutdown_rx.await.ok();
        while futs.next().await.is_some() {
            close_tx.send(()).ok();
        }

        Ok(())
    }

    async fn handle_accept(
        mut server: s2n_quic::Server,
        broker_tx: mpsc::UnboundedSender<broker::ClientMessage>,
        mut close_rx: watch::Receiver<()>,
    ) {
        let mut client_id_gen = 0;
        // one connection per client
        loop {
            select! {
                conn = server.accept() => {
                    let Some(conn) = conn else {
                        return
                    };
                    let local = match conn.local_addr() {
                        Ok(addr) => addr.to_string(),
                        Err(e) => {
                            error!("get client addr error: {e}");
                            continue;
                        }
                    };
                    let client_id = client_id_gen;
                    client_id_gen += 1;
                    // start client
                    let task = Self::handle_conn(local.to_string(), client_id, conn, broker_tx.clone());
                    tokio::spawn(task);
                }
                _ = close_rx.changed() => {
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
