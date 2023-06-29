mod reader;
mod writer;

use bud_common::{helper::wait, io::SharedError, protocol};
use futures::future;
use log::{error, trace};
use s2n_quic::{connection, Connection};
use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;

use crate::broker::ClientMessage;

use self::{reader::Reader, writer::Writer};

pub type Result<T> = std::result::Result<T, Error>;

#[derive(Debug, thiserror::Error)]
pub enum Error {
    /// miss handshake packet
    #[error("Miss Connect packet")]
    MissConnectPacket,
    /// connection or broker disconnect
    #[error("Client has disconnected")]
    Disconnect,
    /// error from quic connection
    #[error("Connection error: {0}")]
    Connection(#[from] connection::Error),
    /// error when reading from stream
    #[error("Protocol error: {0}")]
    Protocol(#[from] protocol::Error),
    /// wait for packet timeout
    #[error("Time out error")]
    Timeout,
}

impl From<tokio::time::error::Elapsed> for Error {
    fn from(_: tokio::time::error::Elapsed) -> Self {
        Self::Timeout
    }
}

pub struct Client {
    id: u64,
    conn: Connection,
    /// packet send to broker
    broker_tx: mpsc::UnboundedSender<ClientMessage>,
}

impl Client {
    pub fn new(id: u64, conn: Connection, broker_tx: mpsc::UnboundedSender<ClientMessage>) -> Self {
        Self {
            id,
            conn,
            broker_tx,
        }
    }

    pub async fn start(self) -> Result<()> {
        let local = self.conn.local_addr()?.to_string();
        let (handle, acceptor) = self.conn.split();

        let error = SharedError::new();
        let token = CancellationToken::new();

        let (client_tx, client_rx) = mpsc::unbounded_channel();

        // read
        trace!("client::start: start read task");
        let read_runner = tokio::spawn(
            Reader::new(
                self.id,
                &local,
                self.broker_tx,
                acceptor,
                error.clone(),
                token.clone(),
            )
            .run(client_tx),
        );

        // write
        trace!("client::start: start write task");
        let write_runner =
            tokio::spawn(Writer::new(&local, handle, client_rx, error, token.clone()).run());

        future::join(
            wait(read_runner, "client read runner", token.clone()),
            wait(write_runner, "client write runner", token.clone()),
        )
        .await;
        token.cancel();
        trace!("client::start: exit");
        Ok(())
    }
}
