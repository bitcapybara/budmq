use std::{fmt::Display, time::Duration};

use futures::{future, FutureExt, SinkExt, StreamExt};
use log::error;
use s2n_quic::{
    connection::{self, Handle, StreamAcceptor},
    stream::BidirectionalStream,
    Connection,
};
use tokio::{
    sync::{mpsc, oneshot},
    time::{error::Elapsed, timeout},
};
use tokio_util::codec::Framed;

use crate::{
    broker,
    protocol::{self, Packet, PacketCodec, ReturnCode},
};

const HANDSHAKE_TIMOUT: Duration = Duration::from_secs(5);
const WAIT_REPLY_TIMEOUT: Duration = Duration::from_millis(200);

type Result<T> = std::result::Result<T, Error>;

#[derive(Debug)]
pub enum Error {
    HandshakeTimeout,
    MissConnectPacket,
    UnexpectedPacket,
    ClientDisconnect,
    ClientIdleTimeout,
    Server(ReturnCode),
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

impl From<oneshot::error::RecvError> for Error {
    fn from(value: oneshot::error::RecvError) -> Self {
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
    /// keepalive setting: ms
    keepalive: u16,
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
        let mut framed = Framed::new(stream, PacketCodec);
        let handshake = timeout(HANDSHAKE_TIMOUT, framed.next())
            .await
            .map_err(|_| Error::HandshakeTimeout)?
            .ok_or(Error::StreamClosed)??;
        match handshake {
            Packet::Connect(connect) => {
                let (res_tx, res_rx) = oneshot::channel();
                let (client_tx, client_rx) = mpsc::channel(1);
                // send to broker
                broker_tx.send(broker::Message {
                    client_id: id,
                    packet: Packet::Connect(connect),
                    res_tx: Some(res_tx),
                    client_tx: Some(client_tx),
                })?;
                // wait for reply
                let code = res_rx.await?;
                framed.send(connect.ack(code)).await?;
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
        let (handle, acceptor) = self.conn.split();

        // read
        let (read_task, read_handle) =
            read(self.id, acceptor, self.keepalive, self.broker_tx).remote_handle();
        tokio::spawn(read_task);

        // write
        let (write_task, write_handle) = write(self.client_rx, handle).remote_handle();
        tokio::spawn(write_task);

        // wait until the end
        future::try_join(read_handle, write_handle).await?;
        Ok(())
    }
}

/// accept new stream to read a packet
async fn read(
    id: u64,
    mut acceptor: StreamAcceptor,
    keepalive: u16,
    broker_tx: mpsc::UnboundedSender<broker::Message>,
) -> Result<()> {
    while let Some(stream) = timeout(
        Duration::from_millis(keepalive as u64),
        acceptor.accept_bidirectional_stream(),
    )
    .await
    .map_err(|_| Error::ClientIdleTimeout)??
    {
        let mut framed = Framed::new(stream, PacketCodec);
        match framed.next().await.ok_or(Error::StreamClosed)?? {
            Packet::Connect(c) => {
                // Do not allow duplicate connections
                framed.send(c.ack(ReturnCode::AlreadyConnected)).await?
            }
            Packet::Subscribe(sub) => {
                let req_id = sub.request_id;
                process_packet(
                    id,
                    req_id,
                    broker_tx.clone(),
                    Packet::Subscribe(sub),
                    framed,
                )
                .await?;
            }
            Packet::Disconnect => return Err(Error::ClientDisconnect),
            _ => return Err(Error::UnexpectedPacket),
        }
    }
    Ok(())
}

/// messages sent from server to client
/// need to open a new stream to send messages
/// client_rx: receive message from broker
async fn write(mut client_rx: mpsc::Receiver<protocol::Packet>, mut handle: Handle) -> Result<()> {
    // * push message to client
    // * disconnect message (due to ping/pong timeout etc...)
    while let Some(message) = client_rx.recv().await {
        let stream = handle.open_bidirectional_stream().await?;
        let framed = Framed::new(stream, PacketCodec);
        // send to client: framed.send(Packet) async? error log?
    }
    Ok(())
}

async fn process_packet(
    client_id: u64,
    req_id: u64,
    broker_tx: mpsc::UnboundedSender<broker::Message>,
    packet: Packet,
    mut framed: Framed<BidirectionalStream, PacketCodec>,
) -> Result<()> {
    let (res_tx, res_rx) = oneshot::channel();
    // send to broker
    broker_tx.send(broker::Message {
        client_id,
        packet,
        res_tx: Some(res_tx),
        client_tx: None,
    })?;
    // wait for response in coroutine
    tokio::spawn(async move {
        match timeout(WAIT_REPLY_TIMEOUT, res_rx).await {
            Ok(Ok(code)) => {
                if let Err(e) = framed.send(Packet::ack(req_id, code)).await {
                    error!("send reply to request {} error: {}", req_id, e)
                }
            }
            Ok(Err(e)) => {
                error!("request {} reply error: {}", req_id, e)
            }
            Err(_) => error!("request {} wait for reply timeout", req_id),
        }
    });
    Ok(())
}
