use std::{io, net::SocketAddr};

use bud_common::{
    helper::wait,
    storage::{MessageStorage, MetaStorage},
    types::BrokerAddress,
};

use bud_common::mtls::Certs;
use log::{error, trace};
use s2n_quic::{
    connection,
    provider::{self, tls},
    Connection,
};
use tokio::{select, sync::mpsc};
use tokio_util::sync::CancellationToken;

use crate::{
    broker::{self, Broker},
    client,
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

impl From<tls::default::error::Error> for Error {
    fn from(e: tls::default::error::Error) -> Self {
        Self::StartUp(e.to_string())
    }
}

impl From<provider::StartError> for Error {
    fn from(value: provider::StartError) -> Self {
        Self::StartUp(value.to_string())
    }
}

pub struct Server {
    certs: Certs,
    addr: SocketAddr,
    broker_addr: BrokerAddress,
    token: CancellationToken,
}

impl Server {
    pub fn new(
        certs: Certs,
        listen_addr: &SocketAddr,
        broker_addr: &BrokerAddress,
    ) -> (CancellationToken, Self) {
        let token = CancellationToken::new();
        (
            token.clone(),
            Self {
                certs,
                broker_addr: broker_addr.clone(),
                token,
                addr: *listen_addr,
            },
        )
    }

    pub async fn start<M: MetaStorage, S: MessageStorage>(
        self,
        meta_storage: M,
        message_storage: S,
    ) -> Result<()> {
        let token = self.token.child_token();

        let tls = tls::default::Server::builder()
            .with_trusted_certificate(self.certs.ca_cert.as_path())?
            .with_certificate(
                self.certs.endpoint_cert.ca(),
                self.certs.endpoint_cert.key(),
            )?
            .with_client_authentication()?
            .build()?;

        // start server loop
        // unwrap: with_tls error is infallible
        trace!("server::start: start accept task");
        let server = s2n_quic::Server::builder()
            .with_tls(tls)
            .unwrap()
            .with_io(self.addr)?
            .start()?;
        // start broker loop
        let broker = Broker::new(
            &self.broker_addr,
            meta_storage,
            message_storage,
            token.clone(),
        );
        let server_task = Self::handle_accept(server, broker, token.clone());
        let server_handle = tokio::spawn(server_task);

        wait(server_handle, "server", token.clone()).await;
        Ok(())
    }

    async fn handle_accept<M, S>(
        mut server: s2n_quic::Server,
        broker: Broker<M, S>,
        token: CancellationToken,
    ) where
        M: MetaStorage,
        S: MessageStorage,
    {
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
                    // start a broker for each client
                    let (broker_tx, broker_rx) = mpsc::unbounded_channel();
                    let broker = broker.clone();
                    tokio::spawn(broker.run(broker_rx));
                    // start client
                    let task = Self::handle_conn(client_addr, client_id, conn, broker_tx);
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
        if let Err(e) = client::Client::new(client_id, conn, broker_tx)
            .start()
            .await
        {
            error!("handle connection from {local} error: {e}");
        }
    }
}
