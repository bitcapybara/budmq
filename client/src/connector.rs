mod reader;
mod writer;

use std::{io, net::SocketAddr};

use bud_common::{
    helper::wait,
    mtls::MtlsProvider,
    protocol::{self, ReturnCode},
};
use s2n_quic::{
    client::{self, Connect},
    connection, provider,
};
use tokio::sync::{mpsc, oneshot};
use tokio_util::sync::CancellationToken;

use crate::consumer::Consumers;

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
}

impl Connector {
    pub fn new(addr: SocketAddr, provider: MtlsProvider) -> Self {
        Self { addr, provider }
    }

    pub async fn run(
        self,
        server_rx: mpsc::UnboundedReceiver<OutgoingMessage>,
        consumers: Consumers,
        keepalive: u16,
        token: CancellationToken,
    ) -> Result<()> {
        // TODO handle reconnect here
        // unwrap: with_tls error is infallible
        let client: client::Client = client::Client::builder()
            .with_tls(self.provider)
            .unwrap()
            .with_io("0.0.0.0:0")?
            .start()?;
        let connector = Connect::new(self.addr);
        let mut connection = client.connect(connector).await?;
        connection.keep_alive(true)?;

        let (handle, acceptor) = connection.split();

        // writer task loop
        let writer_task = Writer::new(handle).run(server_rx, keepalive, token.child_token());
        let writer_handle = tokio::spawn(writer_task);

        // reader task loop
        let reader_task = Reader::new(consumers).run(acceptor, token.child_token());
        let reader_handle = tokio::spawn(reader_task);

        // wait for first complete
        wait(writer_handle, "client writer").await;
        wait(reader_handle, "client reader").await;

        Ok(())
    }
}
