use std::time::Duration;

use bud_common::protocol::{Packet, PacketCodec, ReturnCode};
use futures::{SinkExt, TryStreamExt};
use log::error;
use s2n_quic::connection::Handle;
use tokio::{
    select,
    sync::{mpsc, oneshot, watch},
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
    handle: Handle,
}

impl Writer {
    pub fn new(handle: Handle) -> Self {
        Self { handle }
    }
    pub async fn run(
        mut self,
        mut server_rx: mpsc::UnboundedReceiver<OutgoingMessage>,
        keepalive: u16,
        mut close_rx: watch::Receiver<()>,
    ) {
        let keepalive = Duration::from_millis(keepalive as u64);
        loop {
            select! {
                res = timeout(keepalive, server_rx.recv()) => {
                    match res {
                        Ok(res) => {
                            let Some(msg) = res else {
                                return;
                            };
                            if let Err(e) = self.write(msg).await {
                                error!("write to connection error: {e}");
                                continue;
                            }
                        }
                        Err(_) => {
                            let (res_tx, res_rx) = oneshot::channel();
                            // send ping
                            let msg = OutgoingMessage {
                                packet: Packet::Ping,
                                res_tx,
                            };
                            if let Err(e) = self.write(msg).await {
                                error!("client send ping error: {e}");
                                continue;
                            }
                            tokio::spawn(async move {
                                match res_rx.await {
                                    Ok(_) => todo!(),
                                    Err(_) => todo!(),
                                }
                            });
                        }
                    }
                }
                _ = close_rx.changed() => {
                    return
                }
            }
        }
    }

    async fn write(&mut self, msg: OutgoingMessage) -> Result<()> {
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
        Ok(())
    }
}
