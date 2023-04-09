use bytes::Bytes;
use futures::{SinkExt, TryStreamExt};
use libbud_common::protocol::{Packet, PacketCodec, Publish};
use s2n_quic::connection::Handle;
use tokio::sync::mpsc;
use tokio_util::codec::Framed;

use crate::producer::ProducerMessage;

use super::{Error, Result};

pub struct ProducerTask {
    produce_rx: mpsc::UnboundedReceiver<ProducerMessage>,
    handle: Handle,
}

impl ProducerTask {
    pub fn new(produce_rx: mpsc::UnboundedReceiver<ProducerMessage>, handle: Handle) -> Self {
        Self { produce_rx, handle }
    }

    pub async fn run(mut self) -> Result<()> {
        while let Some(msg) = self.produce_rx.recv().await {
            // send message to connection
            let stream = self.handle.open_bidirectional_stream().await?;
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
                msg.res_tx
                    .send(Err(e.into()))
                    .map_err(|_| Error::WaitChannelDropped)?;
                continue;
            }
            // wait for ack
            // TODO timout
            let Packet::ReturnCode(code) = framed.try_next().await?.ok_or(Error::StreamClosed)? else {
                return Err(Error::UnexpectedPacket);
            };
            msg.res_tx
                .send(Ok(code))
                .map_err(|_| Error::WaitChannelDropped)?;
        }
        Ok(())
    }
}
