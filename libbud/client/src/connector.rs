use std::{io, net::SocketAddr};

use bytes::Bytes;
use futures::{SinkExt, TryStreamExt};
use libbud_common::{
    mtls::MtlsProvider,
    protocol::{self, Packet, PacketCodec, Publish},
};
use s2n_quic::{
    client::{self, Connect},
    connection::{self, Handle, StreamAcceptor},
    provider, Connection,
};
use tokio::sync::mpsc;
use tokio_util::codec::Framed;

use crate::{client::Consumers, producer::ProducerMessage};

pub type Result<T> = std::result::Result<T, Error>;

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
        producer_rx: mpsc::UnboundedReceiver<ProducerMessage>,
        consumers: Consumers,
    ) -> Result<()> {
        let (handle, acceptor) = self.connection.split();

        let producer_handle = tokio::spawn(Self::run_producer(producer_rx, handle));

        let consumer_handle = tokio::spawn(Self::run_consumer(acceptor, consumers));

        Ok(())
    }

    async fn run_producer(
        mut produce_rx: mpsc::UnboundedReceiver<ProducerMessage>,
        mut handle: Handle,
    ) -> Result<()> {
        while let Some(msg) = produce_rx.recv().await {
            // send message to connection
            let stream = handle.open_bidirectional_stream().await?;
            let mut framed = Framed::new(stream, PacketCodec);
            // send publish message
            if let Err(e) = framed
                .send(Packet::Publish(Publish {
                    topic: msg.topic,
                    sequence_id: msg.sequence_id,
                    payload: Bytes::copy_from_slice(&msg.data),
                }))
                .await
            {
                msg.res_tx.send(Err(e.into()));
                continue;
            }
            // wait for ack
            // TODO timout
            let Packet::ReturnCode(code) = framed.try_next().await?.ok_or(Error::StreamClosed)? else {
                return Err(Error::UnexpectedPacket);
            };
            msg.res_tx.send(Ok(code));
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
