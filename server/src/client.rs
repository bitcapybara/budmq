mod reader;
mod writer;

use std::time::Duration;

use bud_common::{
    helper::wait,
    io::SharedError,
    protocol::{self, Packet, PacketCodec, ReturnCode},
};
use futures::{future, SinkExt, StreamExt};
use log::{error, trace};
use s2n_quic::{connection, Connection};
use tokio::{
    sync::{mpsc, oneshot},
    time::timeout,
};
use tokio_util::{codec::Framed, sync::CancellationToken};

use crate::broker::{BrokerMessage, ClientMessage};

use self::{reader::Reader, writer::Writer};

const HANDSHAKE_TIMOUT: Duration = Duration::from_secs(5);

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
    /// packet receive from broker
    client_rx: mpsc::UnboundedReceiver<BrokerMessage>,
    /// keepalive setting: ms
    keepalive: u16,
}

impl Client {
    pub async fn handshake(
        id: u64,
        mut conn: Connection,
        broker_tx: mpsc::UnboundedSender<ClientMessage>,
    ) -> Result<Self> {
        trace!("client::handshake: waiting on accepting a bi stream");
        let stream = conn
            .accept_bidirectional_stream()
            .await?
            .ok_or(Error::Disconnect)?;
        let mut framed = Framed::new(stream, PacketCodec);
        trace!("client::handshake: waiting for the first framed packet");
        let handshake = timeout(HANDSHAKE_TIMOUT, framed.next())
            .await?
            .ok_or(Error::Disconnect)??;
        match handshake {
            Packet::Connect(connect) => {
                trace!("client::handshake: receive CONNECT packet");
                let (res_tx, res_rx) = oneshot::channel();
                let (client_tx, client_rx) = mpsc::unbounded_channel();
                // send to broker
                trace!("client::handshake: send packet to broker");
                broker_tx
                    .send(ClientMessage {
                        client_id: id,
                        packet: Packet::Connect(connect),
                        res_tx: Some(res_tx),
                        client_tx: Some(client_tx),
                    })
                    .map_err(|_| Error::Disconnect)?;
                // wait for reply
                trace!("client::handshake: waiting for response from broker");
                let packet = res_rx.await.map_err(|_| Error::Disconnect)?;
                trace!("client::handshake: send response to client");
                framed.send(packet).await?;
                trace!("client::handshake: build new Client");
                Ok(Self {
                    id,
                    conn,
                    broker_tx,
                    client_rx,
                    keepalive: connect.keepalive,
                })
            }
            _ => {
                framed
                    .send(Packet::err_response(ReturnCode::UnexpectedPacket))
                    .await?;
                Err(Error::MissConnectPacket)
            }
        }
    }

    pub async fn start(self) -> Result<()> {
        let local = self.conn.local_addr()?.to_string();
        let (handle, acceptor) = self.conn.split();

        let error = SharedError::new();
        let token = CancellationToken::new();

        // read
        trace!("client::start: start read task");
        let read_runner = tokio::spawn(
            Reader::new(
                self.id,
                &local,
                self.broker_tx,
                acceptor,
                self.keepalive,
                error.clone(),
                token.clone(),
            )
            .run(),
        );

        // write
        trace!("client::start: start write task");
        let write_runner =
            tokio::spawn(Writer::new(&local, handle, self.client_rx, error, token.clone()).run());

        future::join(
            wait(read_runner, "client read runner"),
            wait(write_runner, "client write runner"),
        )
        .await;
        trace!("client::start: exit");
        Ok(())
    }
}
