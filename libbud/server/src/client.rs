mod reader;
mod writer;

use std::{fmt::Display, time::Duration};

use futures::{SinkExt, StreamExt};
use s2n_quic::{connection, Connection};
use tokio::{
    sync::{mpsc, oneshot},
    time::timeout,
};
use tokio_util::codec::Framed;

use crate::{
    broker::{BrokerMessage, ClientMessage},
    helper::wait,
    protocol::{self, Packet, PacketCodec, ReturnCode},
};

use self::{reader::Reader, writer::Writer};

const HANDSHAKE_TIMOUT: Duration = Duration::from_secs(5);

pub type Result<T> = std::result::Result<T, Error>;

#[derive(Debug)]
pub enum Error {
    HandshakeTimeout,
    MissConnectPacket,
    UnexpectedPacket,
    ClientDisconnect,
    ClientIdleTimeout,
    SendOnDroppedChannel,
    WaitOnDroppedChannel,
    Server(ReturnCode),
    Client(ReturnCode),
    StreamClosed,
    Connection(connection::Error),
    Protocol(protocol::Error),
    Timeout,
}

impl std::error::Error for Error {}

impl Display for Error {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Error::HandshakeTimeout => write!(f, "Handshake time out"),
            Error::MissConnectPacket => write!(f, "Miss Connect packet"),
            Error::UnexpectedPacket => write!(f, "Unexpected packet"),
            Error::ClientDisconnect => write!(f, "Client has disconnected"),
            Error::ClientIdleTimeout => write!(f, "Client idle time out"),
            Error::SendOnDroppedChannel => write!(f, "Send on dropped channel"),
            Error::WaitOnDroppedChannel => write!(f, "Wait reply on dropped channel"),
            Error::Server(c) => write!(f, "Server ReturnCode: {c}"),
            Error::Client(c) => write!(f, "Client ReturnCode: {c}"),
            Error::StreamClosed => write!(f, "Stream closed"),
            Error::Connection(e) => write!(f, "Connection error: {e}"),
            Error::Protocol(e) => write!(f, "Protocol error: {e}"),
            Error::Timeout => write!(f, "Time out error"),
        }
    }
}

impl From<connection::Error> for Error {
    fn from(e: connection::Error) -> Self {
        Self::Connection(e)
    }
}

impl From<protocol::Error> for Error {
    fn from(e: protocol::Error) -> Self {
        Self::Protocol(e)
    }
}

impl<T> From<mpsc::error::SendError<T>> for Error {
    fn from(_: mpsc::error::SendError<T>) -> Self {
        Self::SendOnDroppedChannel
    }
}

impl From<oneshot::error::RecvError> for Error {
    fn from(_: oneshot::error::RecvError) -> Self {
        Self::WaitOnDroppedChannel
    }
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
        let stream = conn
            .accept_bidirectional_stream()
            .await?
            .ok_or(Error::StreamClosed)?;
        let mut framed = Framed::new(stream, PacketCodec);
        let handshake = timeout(HANDSHAKE_TIMOUT, framed.next())
            .await
            .map_err(|_| Error::HandshakeTimeout)?
            .ok_or(Error::StreamClosed)??;
        match handshake {
            Packet::Connect(connect) => {
                let (res_tx, res_rx) = oneshot::channel();
                let (client_tx, client_rx) = mpsc::unbounded_channel();
                // send to broker
                broker_tx.send(ClientMessage {
                    client_id: id,
                    packet: Packet::Connect(connect),
                    res_tx: Some(res_tx),
                    client_tx: Some(client_tx),
                })?;
                // wait for reply
                let code = res_rx.await?;
                framed.send(Packet::ReturnCode(code)).await?;
                if !matches!(code, ReturnCode::Success) {
                    return Err(Error::Server(code));
                }
                Ok(Self {
                    id,
                    conn,
                    broker_tx,
                    client_rx,
                    keepalive: connect.keepalive,
                })
            }
            _ => Err(Error::MissConnectPacket),
        }
    }

    pub async fn start(self) -> Result<()> {
        let local = self.conn.local_addr()?.to_string();
        let (handle, acceptor) = self.conn.split();

        // read
        let reader = Reader::new(self.id, &local, self.broker_tx, acceptor, self.keepalive);
        let read_task = reader.read();
        let read_handle = tokio::spawn(read_task);

        // write
        let writer = Writer::new(&local, self.client_rx, handle);
        let write_task = writer.write();
        let write_handle = tokio::spawn(write_task);

        // wait until the end
        wait(read_handle, "client read").await;
        wait(write_handle, "client write").await;
        Ok(())
    }
}
