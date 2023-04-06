use std::{collections::HashMap, io, net::SocketAddr, sync::Arc};

use futures::{SinkExt, TryStreamExt};
use libbud_common::{
    mtls::MtlsProvider,
    protocol::{self, Packet, PacketCodec, ReturnCode},
};
use s2n_quic::{
    client::{self, Connect},
    connection::{self, Handle, StreamAcceptor},
    provider, Connection,
};
use tokio::sync::{mpsc, RwLock};
use tokio_util::codec::Framed;

use crate::{
    consumer::{self, Consumer, Subscribe},
    producer::{self, Producer},
};

type Result<T> = std::result::Result<T, Error>;

#[derive(Debug)]
pub enum Error {
    StreamClosed,
    UnexpectedPacket,
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
    dispacher: Dispatcher,
}

impl Client {
    pub async fn new(addr: SocketAddr, provider: MtlsProvider) -> Result<Self> {
        let (producer_tx, producer_rx) = mpsc::unbounded_channel();
        let connector = Connector::new(addr, provider).await?;

        let consumers = Consumers::new();
        let connector_handle = tokio::spawn(connector.run(producer_rx, consumers.clone()));

        Ok(Self {
            dispacher: Dispatcher::new(producer_tx, consumers),
        })
    }

    pub async fn new_producer(&self, topic: &str) -> Result<Producer> {
        let producer = Producer::new(topic, self.dispacher.get_producer().await);
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

    async fn run(
        self,
        producer_rx: mpsc::UnboundedReceiver<()>,
        consumers: Consumers,
    ) -> Result<()> {
        let (handle, acceptor) = self.connection.split();

        let producer_handle = tokio::spawn(Self::run_producer(producer_rx, handle));

        let consumer_handle = tokio::spawn(Self::run_consumer(acceptor, consumers));

        Ok(())
    }

    async fn run_producer(
        mut produce_rx: mpsc::UnboundedReceiver<()>,
        mut handle: Handle,
    ) -> Result<()> {
        while let Some(msg) = produce_rx.recv().await {
            // send message to connection
            let stream = handle.open_bidirectional_stream().await?;
            let mut framed = Framed::new(stream, PacketCodec);
            framed.send(Packet::ReturnCode(ReturnCode::Success)).await?;
            todo!()
        }
        Ok(())
    }

    async fn run_consumer(mut acceptor: StreamAcceptor, consumers: Consumers) -> Result<()> {
        // receive message from broker
        while let Some(stream) = acceptor.accept_bidirectional_stream().await? {
            let mut framed = Framed::new(stream, PacketCodec);
            match framed.try_next().await?.ok_or(Error::StreamClosed)? {
                Packet::Send(s) => {
                    let Some(consumer_tx) = consumers.get_consumer(s.consumer_id).await else {
                        continue;  
                    };
                    consumer_tx.send(())?;
                }
                _ => return Err(Error::UnexpectedPacket),
            }
        }
        Ok(())
    }
}

#[derive(Clone)]
struct Consumers(Arc<RwLock<HashMap<u64, mpsc::UnboundedSender<()>>>>);

impl Consumers {
    fn new() -> Self {
        Self(Arc::new(RwLock::new(HashMap::new())))
    }
    async fn add_consumer(&self, consumer_id: u64, consumer_tx: mpsc::UnboundedSender<()>) {
        let mut consumers = self.0.write().await;
        consumers.insert(consumer_id, consumer_tx);
    }

    async fn get_consumer(&self, consumer_id: u64) -> Option<mpsc::UnboundedSender<()>> {
        let consumers = self.0.read().await;
        consumers.get(&consumer_id).cloned()
    }
}

struct Dispatcher {
    /// used to clone on new producer
    producer_tx: mpsc::UnboundedSender<()>,
    /// key=consumer_id, value=consumer_tx
    consumers: Consumers,
}

impl Dispatcher {
    fn new(producer_tx: mpsc::UnboundedSender<()>, consumers: Consumers) -> Self {
        Self {
            producer_tx,
            consumers,
        }
    }

    async fn get_producer(&self) -> mpsc::UnboundedSender<()> {
        self.producer_tx.clone()
    }

    async fn add_consumer(&self, consumer_id: u64, consumer_tx: mpsc::UnboundedSender<()>) {
        self.consumers.add_consumer(consumer_id, consumer_tx).await
    }

    async fn get_consumer(&self, consumer_id: u64) -> Option<mpsc::UnboundedSender<()>> {
        self.consumers.get_consumer(consumer_id).await
    }
}
