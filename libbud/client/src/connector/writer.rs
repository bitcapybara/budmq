use futures::{SinkExt, TryStreamExt};
use libbud_common::protocol::{Packet, PacketCodec, ReturnCode};
use s2n_quic::connection::Handle;
use tokio::sync::{mpsc, oneshot};
use tokio_util::codec::Framed;

use super::{Error, Result};

pub struct OutgoingMessage {
    pub packet: Packet,
    pub res_tx: oneshot::Sender<Result<ReturnCode>>,
}

pub struct Writer {
    server_rx: mpsc::UnboundedReceiver<OutgoingMessage>,
    handle: Handle,
}

impl Writer {
    pub fn new(server_rx: mpsc::UnboundedReceiver<OutgoingMessage>, handle: Handle) -> Self {
        Self { server_rx, handle }
    }

    pub async fn run(mut self) -> Result<()> {
        while let Some(msg) = self.server_rx.recv().await {
            // send message to connection
            let stream = self.handle.open_bidirectional_stream().await?;
            let mut framed = Framed::new(stream, PacketCodec);
            // send publish message
            if let Err(e) = framed.send(msg.packet).await {
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
