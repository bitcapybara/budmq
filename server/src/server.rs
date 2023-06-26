use std::{io, net::SocketAddr};

use bud_common::{
    helper::wait,
    mtls::MtlsProvider,
    storage::{MessageStorage, MetaStorage},
};
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

#[derive(Debug, thiserror::Error)]
pub enum Error {
    /// io error
    #[error("I/O error: {0}")]
    Io(#[from] io::Error),
    /// error on server start up
    #[error("QUIC start up error: {0}")]
    StartUp(String),
    /// error on connecting
    #[error("QUIC connection error: {0}")]
    Connection(#[from] connection::Error),
}

impl From<provider::StartError> for Error {
    fn from(e: provider::StartError) -> Self {
        Self::StartUp(e.to_string())
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

    pub async fn start<M: MetaStorage, S: MessageStorage>(
        self,
        meta_storage: M,
        message_storage: S,
    ) -> Result<()> {
        let token = self.token.child_token();
        // start broker loop
        trace!("server::start: start broker task");
        let (broker_tx, broker_rx) = mpsc::unbounded_channel();
        let broker_task =
            Broker::new(&self.addr, meta_storage, message_storage, token.clone()).run(broker_rx);
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
