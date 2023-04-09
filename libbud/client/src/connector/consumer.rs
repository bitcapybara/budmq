use futures::TryStreamExt;
use libbud_common::protocol::{Packet, PacketCodec};
use s2n_quic::connection::StreamAcceptor;
use tokio_util::codec::Framed;

use crate::client::Consumers;

use super::{Error, Result};

pub struct ConsumerTask {
    acceptor: StreamAcceptor,
    consumers: Consumers,
}

impl ConsumerTask {
    pub fn new(acceptor: StreamAcceptor, consumers: Consumers) -> Self {
        Self {
            acceptor,
            consumers,
        }
    }

    pub async fn run(mut self) -> Result<()> {
        // receive message from broker
        while let Some(stream) = self.acceptor.accept_bidirectional_stream().await? {
            let mut framed = Framed::new(stream, PacketCodec);
            match framed.try_next().await?.ok_or(Error::StreamClosed)? {
                Packet::Send(s) => {
                    let Some(consumer_tx) = self.consumers.get_consumer(s.consumer_id).await else {
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
