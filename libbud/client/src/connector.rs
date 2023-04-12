mod reader;
mod writer;

use std::{io, net::SocketAddr};

use libbud_common::{
    mtls::MtlsProvider,
    protocol::{self, ReturnCode},
};
use log::{error, info};
use s2n_quic::{
    client::{self, Connect},
    connection, provider, Connection,
};
use tokio::{
    sync::{mpsc, oneshot},
    task::JoinHandle,
};

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
    connection: Connection,
}

impl Connector {
    pub async fn new(addr: SocketAddr, provider: MtlsProvider) -> Result<Self> {
        // unwrap: with_tls error is infallible
        let client: client::Client = client::Client::builder()
            .with_tls(provider)
            .unwrap()
            .with_io("0.0.0.0:0")?
            .start()?;
        let connector = Connect::new(addr);
        let mut connection = client.connect(connector).await?;
        connection.keep_alive(true)?;

        todo!()
    }

    pub async fn run(
        self,
        server_rx: mpsc::UnboundedReceiver<OutgoingMessage>,
        consumers: Consumers,
    ) -> Result<()> {
        let (handle, acceptor) = self.connection.split();

        // producer task loop
        let producer_task = Writer::new(server_rx, handle).run();
        let producer_handle = tokio::spawn(producer_task);

        // consumer task loop
        let consumer_task = Reader::new(acceptor, consumers).run();
        let consumer_handle = tokio::spawn(consumer_task);

        Self::wait(producer_handle, "connection reader").await;
        Self::wait(consumer_handle, "connection writer").await;

        Ok(())
    }

    async fn wait(handle: JoinHandle<Result<()>>, label: &str) {
        match handle.await {
            Ok(Ok(_)) => {
                info!("{label} handle task exit successfully")
            }
            Ok(Err(e)) => {
                error!("{label} handle task exit error: {e}")
            }
            Err(e) => {
                error!("{label} handle task panic: {e}")
            }
        }
    }
}
