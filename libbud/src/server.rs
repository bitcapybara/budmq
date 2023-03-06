use std::{fmt::Display, io, net::SocketAddr, time::Duration};

use futures::{FutureExt, Sink, SinkExt, Stream, StreamExt};
use log::error;
use s2n_quic::{
    connection::{self, Handle, StreamAcceptor},
    provider, Connection,
};
use tokio::{
    sync::{mpsc, oneshot},
    time::timeout,
};
use tokio_util::codec::Framed;

use crate::{
    broker,
    mtls::MtlsProvider,
    protocol::{self, Codec, Packet},
};

const HANDSHAKE_TIMOUT: Duration = Duration::from_secs(5);

type Result<T> = std::result::Result<T, Error>;

#[derive(Debug)]
pub enum Error {
    Io(io::Error),
    StartUp(String),
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

impl From<protocol::Error> for Error {
    fn from(value: protocol::Error) -> Self {
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

        // one connection per client
        while let Some(conn) = server.accept().await {
            let local = conn.local_addr()?.to_string();
            let client_id = self.client_id_gen;
            self.client_id_gen += 1;
            // start client
            tokio::spawn(async move {
                match Client::handshake(client_id, conn).await {
                    Ok(client) => {
                        if let Err(e) = client.start().await {
                            error!("handle connection from {local} error: {e}");
                        }
                    }
                    Err(e) => {
                        error!("connection from {local} handshake error: {e}");
                    }
                }
            });
        }
        Ok(())
    }
}

pub struct Client {
    id: u64,
    conn: Connection,
    /// packet send to broker
    broker_tx: mpsc::Sender<broker::Message>,
    /// packet receive from broker
    client_rx: mpsc::Receiver<protocol::Packet>,
}

impl Client {
    pub async fn handshake(
        id: u64,
        mut conn: Connection,
        broker_tx: mpsc::Sender<broker::Message>,
    ) -> Result<Self> {
        let stream = conn
            .accept_bidirectional_stream()
            .await?
            .ok_or(Error::StreamClosed)?;
        let mut framed_stream = Framed::new(stream, Codec);
        let handshake = timeout(HANDSHAKE_TIMOUT, framed_stream.next())
            .await
            .map_err(|_| Error::HandshakeTimeout)?
            .ok_or(Error::StreamClosed)??;
        match handshake {
            Packet::Connect => {
                let (res_tx, res_rx) = oneshot::channel();
                let (client_tx, client_rx) = mpsc::channel(1);
                broker_tx
                    .send(broker::Message {
                        packet: Packet::Connect,
                        res_tx: Some(res_tx),
                        client_id: id,
                        client_tx: Some(client_tx),
                    })
                    .await;
                match res_rx.await? {
                    Packet::Connect => todo!(),
                    Packet::Disconnect => todo!(),
                    Packet::Fail(_) => todo!(),
                    Packet::Success => todo!(),
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
        let (write_task, write_handle) = Self::write(handle).remote_handle();
        tokio::spawn(write_task);
        Ok(())
    }

    /// accept new stream to read a packet
    async fn read(mut acceptor: StreamAcceptor) -> Result<()> {
        while let Some(stream) = acceptor.accept_bidirectional_stream().await? {
            let mut framed = Framed::new(stream, Codec);
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
    async fn write(mut client_rx: mpsc::Receiver<()>, mut handle: Handle) -> Result<()> {
        // * push message to client
        // * disconnect message (due to ping/pong timeout etc...)
        while let Some(message) = client_rx.recv().await {
            let stream = handle.open_bidirectional_stream().await?;
            let framed = Framed::new(stream, Codec);
            // send to client: framed.send(Packet) async? error log?
        }
        todo!()
    }
}
