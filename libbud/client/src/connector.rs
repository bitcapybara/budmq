mod reader;
mod writer;

use std::{io, net::SocketAddr};

use libbud_common::{mtls::MtlsProvider, protocol};
use log::{error, info};
use s2n_quic::{
    client::{self, Connect},
    connection, provider, Connection,
};
use tokio::{sync::mpsc, task::JoinHandle};

use crate::client::Consumers;

use self::{reader::Reader, writer::Writer};

pub use writer::OutgoingMessage;

pub type Result<T> = std::result::Result<T, Error>;

#[derive(Debug)]
pub enum Error {
    StreamClosed,
    UnexpectedPacket,
    WaitChannelDropped,
}

impl std::error::Error for Error {}

impl std::fmt::Display for Error {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        todo!()
    }
}

impl From<io::Error> for Error {
    fn from(value: io::Error) -> Self {
        todo!()
    }
}

impl From<provider::StartError> for Error {
    fn from(value: provider::StartError) -> Self {
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
