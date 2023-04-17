use futures::{SinkExt, TryStreamExt};
use bud_common::protocol::{Packet, PacketCodec, ReturnCode};
use log::error;
use s2n_quic::connection::Handle;
use tokio::{
    sync::{mpsc, oneshot},
    time::timeout,
};
use tokio_util::codec::Framed;

use crate::WAIT_REPLY_TIMEOUT;

use super::Result;

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
            tokio::spawn(async move {
                let mut framed = Framed::new(stream, PacketCodec);
                // send publish message
                if let Err(e) = framed.send(msg.packet).await {
                    if msg.res_tx.send(Err(e.into())).is_err() {
                        error!("client writer wait channel dropped");
                    }
                    return;
                }
                // wait for ack
                let code = match timeout(WAIT_REPLY_TIMEOUT, framed.try_next()).await {
                    Ok(Ok(Some(Packet::ReturnCode(code)))) => code,
                    Ok(Ok(Some(p))) => {
                        error!(
                            "client writer: expected Packet::ReturnCode, found {:?}",
                            p.packet_type()
                        );
                        return;
                    }
                    Ok(Ok(None)) => {
                        error!("client writer: framed stream dropped");
                        return;
                    }
                    Ok(Err(e)) => {
                        error!("client writer frame error: {e}");
                        return;
                    }
                    Err(_) => {
                        error!("client writer wait for reply timeout");
                        return;
                    }
                };
                if msg.res_tx.send(Ok(code)).is_err() {
                    error!("client writer wait channel dropped");
                }
            });
        }
        Ok(())
    }
}
