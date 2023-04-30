mod reader;
mod writer;

use std::{
    io,
    net::SocketAddr,
    sync::{atomic::Ordering, Arc},
};

use bud_common::{
    helper::wait,
    mtls::MtlsProvider,
    protocol::{self, Connect, ControlFlow, Packet, PacketCodec, ReturnCode, Subscribe},
};
use futures::{future, SinkExt, TryStreamExt};
use log::{error, trace};
use s2n_quic::{
    client,
    connection::{self, Handle},
    provider, Connection,
};
use tokio::{
    select,
    sync::{mpsc, oneshot, Mutex},
    time::timeout,
};
use tokio_util::{codec::Framed, sync::CancellationToken};

use crate::{
    consumer::{Consumers, Session},
    WAIT_REPLY_TIMEOUT,
};

use self::{reader::Reader, writer::Writer};

pub use reader::ConsumerSender;
pub use writer::OutgoingMessage;

pub type Result<T> = std::result::Result<T, Error>;

#[derive(Debug)]
pub enum Error {
    StreamClosed,
    UnexpectedPacket,
    WaitChannelDropped,
    FromServer(ReturnCode),
    FromQuic(String),
    Protocol(protocol::Error),
    Internal(String),
    Disconnect,
}

impl std::error::Error for Error {}

impl std::fmt::Display for Error {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Error::StreamClosed => write!(f, "quic stream closed"),
            Error::UnexpectedPacket => write!(f, "received unexpected packet"),
            Error::WaitChannelDropped => write!(f, "wait channel Dropped"),
            Error::FromServer(code) => write!(f, "receive code from server: {code}"),
            Error::FromQuic(e) => write!(f, "receive error from quic: {e}"),
            Error::Protocol(e) => write!(f, "protocol error: {e}"),
            Error::Internal(e) => write!(f, "internal error: {e}"),
            Error::Disconnect => write!(f, "receive DISCONNECT packet from Server"),
        }
    }
}

impl From<io::Error> for Error {
    fn from(e: io::Error) -> Self {
        Self::FromQuic(e.to_string())
    }
}

impl From<provider::StartError> for Error {
    fn from(e: provider::StartError) -> Self {
        Self::FromQuic(e.to_string())
    }
}

impl From<connection::Error> for Error {
    fn from(e: connection::Error) -> Self {
        Self::FromQuic(e.to_string())
    }
}

impl From<protocol::Error> for Error {
    fn from(e: protocol::Error) -> Self {
        Self::Protocol(e)
    }
}

impl<T> From<mpsc::error::SendError<T>> for Error {
    fn from(e: mpsc::error::SendError<T>) -> Self {
        Self::Internal(e.to_string())
    }
}

impl From<oneshot::error::RecvError> for Error {
    fn from(e: oneshot::error::RecvError) -> Self {
        Self::Internal(e.to_string())
    }
}

pub struct Connector {
    addr: SocketAddr,
    keepalive: u16,
    provider: MtlsProvider,
    consumers: Consumers,
    server_tx: mpsc::UnboundedSender<OutgoingMessage>,
}

impl Connector {
    pub fn new(
        addr: SocketAddr,
        keepalive: u16,
        provider: MtlsProvider,
        consumers: Consumers,
        server_tx: mpsc::UnboundedSender<OutgoingMessage>,
    ) -> Self {
        Self {
            addr,
            provider,
            consumers,
            server_tx,
            keepalive,
        }
    }

    pub async fn run(
        self,
        server_rx: mpsc::UnboundedReceiver<OutgoingMessage>,
        token: CancellationToken,
    ) -> Result<()> {
        let server_rx = Arc::new(Mutex::new(server_rx));
        loop {
            select! {
                _ = token.cancelled() => {
                    return Ok(())
                },
                _ = async {} => {
                    if let Err(e) = self.run_task(server_rx.clone(), self.keepalive, token.child_token()).await {
                        error!("client connector task error: {e}");
                        token.cancel();
                        return Err(e)
                    }
                }
            }
        }
    }

    async fn run_task(
        &self,
        server_rx: Arc<Mutex<mpsc::UnboundedReceiver<OutgoingMessage>>>,
        keepalive: u16,
        token: CancellationToken,
    ) -> Result<()> {
        let task_token = token.child_token();
        // build connection
        trace!("connector::run_task: connect to server");
        let connection = self.connect().await?;
        let (handle, acceptor) = connection.split();

        // handshake
        trace!("connector::run_task: handshake to server");
        self.handshake(handle.clone(), keepalive).await?;

        // resub
        trace!("connector::run_task: consumers subscribe");
        self.subscribe().await?;

        // writer task loop
        trace!("connector::run_task: start writer task loop");
        let writer_task = Writer::new(handle).run(server_rx, keepalive, task_token.clone());
        let writer_handle = tokio::spawn(writer_task);

        // reader task loop
        trace!("connector::run_task: start reader task loop");
        let reader_task = Reader::new(self.consumers.clone()).run(acceptor, task_token.clone());
        let reader_handle = tokio::spawn(reader_task);

        // wait for completing
        trace!("connector::run_task: waiting for tasks exit");
        future::join(
            wait(writer_handle, "client writer"),
            wait(reader_handle, "client reader"),
        )
        .await;
        token.cancel();

        trace!("connector::run_task: exit");
        Ok(())
    }

    async fn connect(&self) -> Result<Connection> {
        // unwrap safe: with_tls error is infallible
        trace!("connector::connect: create client and connect");
        let client: client::Client = client::Client::builder()
            .with_tls(self.provider.clone())
            .unwrap()
            .with_io("0.0.0.0:0")?
            .start()?;
        // TODO server name must be `localhost`?
        let connector = s2n_quic::client::Connect::new(self.addr).with_server_name("localhost");
        let mut connection = client.connect(connector).await?;
        connection.keep_alive(true)?;
        Ok(connection)
    }

    async fn subscribe(&self) -> Result<()> {
        let consumer_ids = self.consumers.get_consumer_ids().await;
        for id in consumer_ids {
            let session = self.consumers.get(id).await;
            let Some(session) = session else {
                continue;
            };
            self.subscribe_one(&session).await?;
        }

        Ok(())
    }

    async fn handshake(&self, mut handle: Handle, keepalive: u16) -> Result<()> {
        // send connect packet
        trace!("connector::new: send CONNECT packet to server");
        let stream = handle.open_bidirectional_stream().await?;
        let mut framed = Framed::new(stream, PacketCodec);
        if let Err(e) = framed.send(Packet::Connect(Connect { keepalive })).await {
            error!("client send handshake packet error: {e}");
            return Err(e)?;
        }
        trace!("connector::new: waiting for CONNECT response");
        let code = match timeout(WAIT_REPLY_TIMEOUT, framed.try_next()).await {
            Ok(Ok(Some(Packet::Response(code)))) => code,
            Ok(Ok(Some(p))) => {
                error!(
                    "client handshake: expected Packet::Response, found {}",
                    p.packet_type()
                );
                return Err(Error::UnexpectedPacket);
            }
            Ok(Ok(None)) => {
                error!("client handshake: framed stream dropped");
                return Err(Error::StreamClosed);
            }
            Ok(Err(e)) => {
                error!("client handshake: decode frame error: {e}");
                return Err(Error::Protocol(e));
            }
            Err(_) => {
                error!("client handshake: wait for reply timeout");
                return Err(Error::Internal("handshake timeout".to_string()));
            }
        };
        if !matches!(code, ReturnCode::Success) {
            error!("client handshake: receive code from server: {code}");
        }

        Ok(())
    }

    async fn subscribe_one(&self, session: &Session) -> Result<()> {
        let consumer_id = session.consumer_id;
        // subscribe first, wait for server reply
        // if server reply "duplicated consumer" error, abort
        let (sub_res_tx, sub_res_rx) = oneshot::channel();
        let sub = &session.sub_info;
        trace!("connector::subscribe_one: send SUBSCRIBE message");
        self.server_tx.send(OutgoingMessage {
            packet: Packet::Subscribe(Subscribe {
                consumer_id,
                topic: sub.topic.clone(),
                sub_name: sub.sub_name.clone(),
                sub_type: sub.sub_type,
                initial_position: sub.initial_postion,
            }),
            res_tx: sub_res_tx,
        })?;
        let code = sub_res_rx.await??;
        if !matches!(code, ReturnCode::Success) {
            return Err(Error::FromServer(code));
        }
        // send premits
        trace!("connector::subscribe_one: send CONTROLFLOW message");
        let (permits_res_tx, permits_res_rx) = oneshot::channel();
        let permits = session.sender.permits.load(Ordering::SeqCst);
        self.server_tx.send(OutgoingMessage {
            packet: Packet::ControlFlow(ControlFlow {
                consumer_id,
                permits,
            }),
            res_tx: permits_res_tx,
        })?;
        match permits_res_rx.await?? {
            ReturnCode::Success => Ok(()),
            code => Err(Error::FromServer(code)),
        }
    }
}
