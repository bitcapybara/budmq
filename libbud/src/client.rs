use std::{fmt::Display, time::Duration};

use futures::{FutureExt, SinkExt, StreamExt};
use s2n_quic::{
    connection::{self, Handle, StreamAcceptor},
    Connection,
};
use tokio::{
    sync::{mpsc, oneshot},
    time::timeout,
};
use tokio_util::codec::Framed;

use crate::{
    broker,
    protocol::{self, Packet},
};

const HANDSHAKE_TIMOUT: Duration = Duration::from_secs(5);

type Result<T> = std::result::Result<T, Error>;

#[derive(Debug)]
pub enum Error {
    HandshakeTimeout,
    MissConnectPacket,
    UnexpectedPacket,
    ClientDisconnect,
    StreamClosed,
}

impl std::error::Error for Error {}

impl Display for Error {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        todo!()
    }
}

impl From<connection::Error> for Error {
    fn from(value: connection::Error) -> Self {
        todo!()
    }
}

impl From<protocol::Error> for Error {
    fn from(value: protocol::Error) -> Self {
        todo!()
    }
}

impl<T> From<mpsc::error::SendError<T>> for Error {
    fn from(value: mpsc::error::SendError<T>) -> Self {
        todo!()
    }
}

pub struct Client {
    id: u64,
    conn: Connection,
    /// packet send to broker
    broker_tx: mpsc::UnboundedSender<broker::Message>,
    /// packet receive from broker
    client_rx: mpsc::Receiver<protocol::Packet>,
}

impl Client {
    pub async fn handshake(
        id: u64,
        mut conn: Connection,
        broker_tx: mpsc::UnboundedSender<broker::Message>,
    ) -> Result<Self> {
        let stream = conn
            .accept_bidirectional_stream()
            .await?
            .ok_or(Error::StreamClosed)?;
        let mut framed_stream = Framed::new(stream, protocol::Codec);
        let handshake = timeout(HANDSHAKE_TIMOUT, framed_stream.next())
            .await
            .map_err(|_| Error::HandshakeTimeout)?
            .ok_or(Error::StreamClosed)??;
        match handshake {
            Packet::Connect => {
                let (res_tx, res_rx) = oneshot::channel();
                let (client_tx, client_rx) = mpsc::channel(1);
                // send to broker
                broker_tx.send(broker::Message {
                    client_id: id,
                    packet: Packet::Connect,
                    res_tx: Some(res_tx),
                    client_tx: Some(client_tx),
                })?;
                // wait for reply
                match res_rx.await.unwrap() {
                    p @ (Packet::Fail(_) | Packet::Success) => framed_stream.send(p).await?,
                    _ => unreachable!(),
                }
                Ok(Self {
                    id,
                    conn,
                    broker_tx,
                    client_rx,
                })
            }
            _ => Err(Error::MissConnectPacket),
        }
    }

    pub async fn start(self) -> Result<()> {
        let (handle, acceptor) = self.conn.split();

        // read
        let (read_task, read_handle) = Self::read(acceptor).remote_handle();
        tokio::spawn(read_task);

        // write
        let (write_task, write_handle) = Self::write(self.client_rx, handle).remote_handle();
        tokio::spawn(write_task);

        // TODO wait for the trask to end
        Ok(())
    }

    /// accept new stream to read a packet
    async fn read(mut acceptor: StreamAcceptor) -> Result<()> {
        while let Some(stream) = acceptor.accept_bidirectional_stream().await? {
            let mut framed = Framed::new(stream, protocol::Codec);
            match framed.next().await.ok_or(Error::StreamClosed)?? {
                Packet::Connect => framed.send(Packet::error("Already connected")).await?,
                Packet::Disconnect => return Err(Error::ClientDisconnect),
                _ => return Err(Error::UnexpectedPacket),
            }
        }
        todo!()
    }

    /// messages sent from server to client
    /// need to open a new stream to send messages
    /// client_rx: receive message from broker
    async fn write(
        mut client_rx: mpsc::Receiver<protocol::Packet>,
        mut handle: Handle,
    ) -> Result<()> {
        // * push message to client
        // * disconnect message (due to ping/pong timeout etc...)
        while let Some(message) = client_rx.recv().await {
            let stream = handle.open_bidirectional_stream().await?;
            let framed = Framed::new(stream, protocol::Codec);
            // send to client: framed.send(Packet) async? error log?
        }
        todo!()
    }
}
