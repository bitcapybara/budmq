use bud_common::protocol::{Packet, PacketCodec, ReturnCode};
use futures::{SinkExt, StreamExt};
use log::{error, trace};
use s2n_quic::{connection, stream::BidirectionalStream};
use tokio::{
    select,
    sync::{mpsc, oneshot},
    time::timeout,
};
use tokio_util::{codec::Framed, sync::CancellationToken};

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
        token: CancellationToken,
    ) {
        loop {
            select! {
                res = client_rx.recv() => {
                    let Some(message) = res else {
                        token.cancel();
                        return
                    };
                    trace!("client:writer: receive a new packet task, open stream");
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
                            trace!("client::writer: Send SEND packet to client");
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
                _ = token.cancelled() => {
                    return
                }
            }
        }
    }

    async fn send(
        &self,
        res_tx: oneshot::Sender<Result<()>>,
        mut framed: Framed<BidirectionalStream, PacketCodec>,
        packet: Packet,
    ) -> Result<()> {
        let local = self.local_addr.clone();
        tokio::spawn(async move {
            trace!("client::writer[spawn]: send packet to client");
            if let Err(e) = framed.send(packet).await {
                error!("send packet to client {local} error: {e}")
            }
            trace!("client::writer[spawn]: waiting for response from client");
            match timeout(WAIT_REPLY_TIMEOUT, framed.next()).await {
                Ok(Some(Ok(Packet::Response(code)))) => {
                    let res = match code {
                        ReturnCode::Success => Ok(()),
                        _ => Err(Error::Client(code)),
                    };
                    if let Err(e) = res_tx.send(res) {
                        error!("send SEND reply to broker error: {e:?}")
                    }
                }
                Ok(Some(Ok(_))) => {
                    if res_tx.send(Err(Error::UnexpectedPacket)).is_err() {
                        error!("recv unexpected packet from client {local}")
                    }
                }
                Ok(Some(Err(e))) => {
                    if let Err(e) = res_tx.send(Err(e.into())) {
                        error!("recv reply from client {local} error: {e:?}")
                    }
                }
                Ok(None) => {
                    if res_tx.send(Err(Error::StreamClosed)).is_err() {
                        error!("client {local} stream closed")
                    }
                }
                Err(e) => {
                    if res_tx.send(Err(e.into())).is_err() {
                        error!("wait for client {local} reply timout")
                    }
                }
            }
            framed.close().await.ok();
        });
        Ok(())
    }
}
