use std::{collections::HashMap, io, net::SocketAddr, sync::Arc};

use libbud_common::mtls::MtlsProvider;
use s2n_quic::{
    client::{self, Connect},
    connection::{self, Handle, StreamAcceptor},
    provider, Connection,
};
use tokio::sync::{mpsc, RwLock};

use crate::{
    consumer::{self, Consumer, Subscribe},
    producer::{self, Producer},
};

type Result<T> = std::result::Result<T, Error>;

#[derive(Debug)]
pub enum Error {}

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

impl From<producer::Error> for Error {
    fn from(value: producer::Error) -> Self {
        todo!()
    }
}

impl From<consumer::Error> for Error {
    fn from(value: consumer::Error) -> Self {
        todo!()
    }
}

pub struct Client {
    dispacher: Dispatcher,
}

impl Client {
    pub async fn new(addr: SocketAddr, provider: MtlsProvider) -> Result<Self> {
        let (producer_tx, producer_rx) = mpsc::unbounded_channel();
        let connector = Connector::new(addr, provider).await?;
        let connector_handle = tokio::spawn(connector.run(producer_rx));
        todo!()
    }

    pub async fn new_producer(&self, topic: &str) -> Result<Producer> {
        let producer = Producer::new(topic, self.dispacher.cloen_producer().await);

        Ok(producer)
    }

    pub async fn new_consumer(&mut self, subscribe: &Subscribe) -> Result<Consumer> {
        let (consumer_tx, consumer_rx) = mpsc::unbounded_channel();
        let consumer = Consumer::new(subscribe, consumer_rx).await?;
        self.dispacher.add_consumer(0, consumer_tx).await;

        Ok(consumer)
    }
}

struct Connector {
    connection: Connection,
    dispatcher: Dispatcher,
}

impl Connector {
    async fn new(addr: SocketAddr, provider: MtlsProvider) -> Result<Self> {
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

    async fn run(self, producer_rx: mpsc::UnboundedReceiver<()>) -> Result<()> {
        let (handle, acceptor) = self.connection.split();

        let producer_handle = tokio::spawn(Self::run_producer(producer_rx, handle));

        let consumer_handle = tokio::spawn(Self::run_consumer(acceptor));

        Ok(())
    }

    async fn run_producer(
        mut produce_rx: mpsc::UnboundedReceiver<()>,
        handle: Handle,
    ) -> Result<()> {
        while let Some(msg) = produce_rx.recv().await {
            // send message to connection
            todo!()
        }
        Ok(())
    }

    async fn run_consumer(mut acceptor: StreamAcceptor) -> Result<()> {
        while let Some(stream) = acceptor.accept_bidirectional_stream().await? {
            todo!()
        }
        Ok(())
    }
}

struct Dispatcher {
    /// used to clone on new producer
    producer_tx: mpsc::UnboundedSender<()>,
    /// key=consumer_id, value=consumer_tx
    consumers: Arc<RwLock<HashMap<u64, mpsc::UnboundedSender<()>>>>,
}

impl Dispatcher {
    async fn add_consumer(&self, consumer_id: u64, consumer_tx: mpsc::UnboundedSender<()>) {
        let mut consumers = self.consumers.write().await;
        consumers.insert(consumer_id, consumer_tx);
    }

    async fn cloen_producer(&self) -> mpsc::UnboundedSender<()> {
        self.producer_tx.clone()
    }
}
