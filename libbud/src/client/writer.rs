use futures::{SinkExt, StreamExt};
use log::error;
use s2n_quic::{connection, stream::BidirectionalStream};
use tokio::{
    sync::{mpsc, oneshot},
    time::timeout,
};
use tokio_util::codec::Framed;

use super::{Result, WAIT_REPLY_TIMEOUT};
use crate::{
    broker::BrokerMessage,
    protocol::{Packet, PacketCodec, ReturnCode},
};

pub struct Writer {
    local_addr: String,
    client_rx: mpsc::UnboundedReceiver<BrokerMessage>,
    handle: connection::Handle,
}

impl Writer {
    pub fn new(
        local_addr: &str,
        client_rx: mpsc::UnboundedReceiver<BrokerMessage>,
        handle: connection::Handle,
    ) -> Self {
        Self {
            local_addr: local_addr.to_string(),
            client_rx,
            handle,
        }
    }

    /// messages sent from server to client
    /// need to open a new stream to send messages
    /// client_rx: receive message from broker
    pub async fn write(mut self) -> Result<()> {
        // * push message to client
        while let Some(message) = self.client_rx.recv().await {
            let stream = self.handle.open_bidirectional_stream().await?;
            let framed = Framed::new(stream, PacketCodec);
            // send to client: framed.send(Packet) async? error log?
            match message.packet {
                p @ Packet::Publish(_) => self.send(message.res_tx, framed, p).await?,
                _ => unreachable!(),
            }
        }
        Ok(())
    }

    async fn send(
        &self,
        res_tx: Option<oneshot::Sender<ReturnCode>>,
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
                            if let Err(e) = tx.send(code) {
                                error!("send reply to broker error: {e}")
                            }
                        }
                        Ok(Some(Ok(_))) => {
                            error!("recv unexpected packet from client {local}")
                        }
                        Ok(Some(Err(e))) => {
                            error!("recv reply from client {local} error: {e}")
                        }
                        Ok(None) => {
                            error!("client {local} stream closed")
                        }
                        Err(_) => {
                            error!("wait for client {local} reply timout")
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
