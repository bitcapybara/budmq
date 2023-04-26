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
    protocol::{self, ControlFlow, Packet, ReturnCode, Subscribe},
};
use s2n_quic::{
    client::{self, Connect},
    connection, provider, Connection,
};
use tokio::sync::{mpsc, oneshot, Mutex};
use tokio_util::sync::CancellationToken;

use crate::consumer::{Consumers, Session};

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
    provider: MtlsProvider,
    consumers: Consumers,
    server_tx: mpsc::UnboundedSender<OutgoingMessage>,
}

impl Connector {
    pub fn new(
        addr: SocketAddr,
        provider: MtlsProvider,
        consumers: Consumers,
        server_tx: mpsc::UnboundedSender<OutgoingMessage>,
    ) -> Self {
        Self {
            addr,
            provider,
            consumers,
            server_tx,
        }
    }

    pub async fn run(
        self,
        server_rx: mpsc::UnboundedReceiver<OutgoingMessage>,
        keepalive: u16,
        token: CancellationToken,
    ) -> Result<()> {
        let server_rx = Arc::new(Mutex::new(server_rx));
        loop {
            // build connection
            let connection = self.connect().await?;
            let (handle, acceptor) = connection.split();

            // resub
            self.resub().await?;

            // writer task loop
            let writer_task =
                Writer::new(handle).run(server_rx.clone(), keepalive, token.child_token());
            let writer_handle = tokio::spawn(writer_task);

            // reader task loop
            let reader_task =
                Reader::new(self.consumers.clone()).run(acceptor, token.child_token());
            let reader_handle = tokio::spawn(reader_task);

            // wait for completing
            wait(writer_handle, "client writer").await;
            wait(reader_handle, "client reader").await;
        }
    }

    async fn connect(&self) -> Result<Connection> {
        // unwrap safe: with_tls error is infallible
        let client: client::Client = client::Client::builder()
            .with_tls(self.provider.clone())
            .unwrap()
            .with_io("0.0.0.0:0")?
            .start()?;
        let connector = Connect::new(self.addr);
        let mut connection = client.connect(connector).await?;
        connection.keep_alive(true)?;
        Ok(connection)
    }

    async fn resub(&self) -> Result<()> {
        let consumer_ids = self.consumers.get_consumer_ids().await;
        for id in consumer_ids {
            let session = self.consumers.get(id).await;
            let Some(session) = session else {
                continue;
            };
            self.resub_one(&session).await?;
        }

        Ok(())
    }

    async fn resub_one(&self, session: &Session) -> Result<()> {
        let consumer_id = session.consumer_id;
        // subscribe first, wait for server reply
        // if server reply "duplicated consumer" error, abort
        let (sub_res_tx, sub_res_rx) = oneshot::channel();
        let sub = &session.sub_info;
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
        match sub_res_rx.await?? {
            ReturnCode::Success => {}
            code => return Err(Error::FromServer(code)),
        }
        // send premits
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
            ReturnCode::Success => {}
            code => return Err(Error::FromServer(code)),
        }
        Ok(())
    }
}
