use bud_common::protocol::{Packet, PacketCodec, ReturnCode};
use futures::{SinkExt, StreamExt};
use log::error;
use s2n_quic::{connection, stream::BidirectionalStream};
use tokio::{
    sync::{mpsc, oneshot},
    time::timeout,
};
use tokio_util::codec::Framed;

use super::{Error, Result};
use crate::{broker::BrokerMessage, WAIT_REPLY_TIMEOUT};

pub struct Writer {
    local_addr: String,
}

impl Writer {
    pub fn new(local_addr: &str) -> Self {
        Self {
            local_addr: local_addr.to_string(),
        }
    }

    /// messages sent from server to client
    /// need to open a new stream to send messages
    /// client_rx: receive message from broker
    pub async fn run(
        self,
        mut client_rx: mpsc::UnboundedReceiver<BrokerMessage>,
        mut handle: connection::Handle,
    ) {
        // * push message to client
        while let Some(message) = client_rx.recv().await {
            let stream = match handle.open_bidirectional_stream().await {
                Ok(stream) => stream,
                Err(e) => {
                    error!("open stream error: {e}");
                    continue;
                }
            };
            let framed = Framed::new(stream, PacketCodec);
            // send to client: framed.send(Packet) async? error log?
            match message.packet {
                p @ Packet::Send(_) => {
                    if let Err(e) = self.send(message.res_tx, framed, p).await {
                        error!("send message to client error: {e}");
                    }
                }
                p => error!(
                    "received unexpected packet from broker: {:?}",
                    p.packet_type()
                ),
            }
        }
    }

    async fn send(
        &self,
        res_tx: Option<oneshot::Sender<Result<()>>>,
        mut framed: Framed<BidirectionalStream, PacketCodec>,
        packet: Packet,
    ) -> Result<()> {
        let local = self.local_addr.clone();
        match res_tx {
            Some(tx) => {
                // sync send, wait for reply
                tokio::spawn(async move {
                    if let Err(e) = framed.send(packet).await {
                        error!("send packet to client {local} error: {e}")
                    }
                    match timeout(WAIT_REPLY_TIMEOUT, framed.next()).await {
                        Ok(Some(Ok(Packet::ReturnCode(code)))) => {
                            let res = match code {
                                ReturnCode::Success => Ok(()),
                                _ => Err(Error::Client(code)),
                            };
                            if let Err(e) = tx.send(res) {
                                error!("send SEND reply to broker error: {e:?}")
                            }
                        }
                        Ok(Some(Ok(_))) => {
                            if tx.send(Err(Error::UnexpectedPacket)).is_err() {
                                error!("recv unexpected packet from client {local}")
                            }
                        }
                        Ok(Some(Err(e))) => {
                            if let Err(e) = tx.send(Err(e.into())) {
                                error!("recv reply from client {local} error: {e:?}")
                            }
                        }
                        Ok(None) => {
                            if tx.send(Err(Error::StreamClosed)).is_err() {
                                error!("client {local} stream closed")
                            }
                        }
                        Err(e) => {
                            if tx.send(Err(e.into())).is_err() {
                                error!("wait for client {local} reply timout")
                            }
                        }
                    }
                });
            }
            None => {
                // async send, log error
                tokio::spawn(async move {
                    if let Err(e) = framed.send(packet).await {
                        error!("send packet to client {local} error: {e}")
                    }
                });
            }
        }
        Ok(())
    }
}