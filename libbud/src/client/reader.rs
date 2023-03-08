use std::{net::SocketAddr, time::Duration};

use futures::{SinkExt, StreamExt};
use log::error;
use s2n_quic::{connection::StreamAcceptor, stream::BidirectionalStream};
use tokio::{
    sync::{mpsc, oneshot},
    time::timeout,
};
use tokio_util::codec::Framed;

use super::{Error, Result, WAIT_REPLY_TIMEOUT};
use crate::{
    broker,
    protocol::{Packet, PacketCodec, ReturnCode},
};

pub struct Reader {
    client_id: u64,
    local_addr: String,
    broker_tx: mpsc::UnboundedSender<broker::ClientMessage>,
    acceptor: StreamAcceptor,
    keepalive: u16,
}

impl Reader {
    pub fn new(
        client_id: u64,
        local_addr: &str,
        broker_tx: mpsc::UnboundedSender<broker::ClientMessage>,
        acceptor: StreamAcceptor,
        keepalive: u16,
    ) -> Self {
        Self {
            client_id,
            local_addr: local_addr.to_string(),
            broker_tx,
            acceptor,
            keepalive,
        }
    }

    /// accept new stream to read a packet
    pub async fn read(mut self) -> Result<()> {
        // 1.5 times keepalive value
        let keepalive = self.keepalive + self.keepalive / 2;
        while let Some(stream) = timeout(
            Duration::from_millis(keepalive as u64),
            self.acceptor.accept_bidirectional_stream(),
        )
        .await
        .map_err(|_| Error::ClientIdleTimeout)??
        {
            let mut framed = Framed::new(stream, PacketCodec);
            match framed.next().await.ok_or(Error::StreamClosed)?? {
                Packet::Connect(c) => {
                    // Do not allow duplicate connections
                    let code = ReturnCode::AlreadyConnected;
                    framed.send(Packet::ReturnCode(code)).await?
                }
                p @ (Packet::Subscribe(_)
                | Packet::Unsubscribe(_)
                | Packet::Publish(_)
                | Packet::ConsumeAck(_)
                | Packet::ControlFlow(_)) => {
                    self.send(p, framed).await?;
                }
                Packet::Ping => {
                    tokio::spawn(async move {
                        if let Err(e) = framed.send(Packet::Pong).await {
                            error!("send pong packet error: {e}")
                        }
                    });
                }
                Packet::Disconnect => {
                    self.broker_tx.send(broker::ClientMessage {
                        client_id: self.client_id,
                        packet: Packet::Disconnect,
                        res_tx: None,
                        client_tx: None,
                    })?;
                }
                _ => return Err(Error::UnexpectedPacket),
            }
        }
        Ok(())
    }

    async fn send(
        &self,
        packet: Packet,
        mut framed: Framed<BidirectionalStream, PacketCodec>,
    ) -> Result<()> {
        let (res_tx, res_rx) = oneshot::channel();
        // send to broker
        self.broker_tx.send(broker::ClientMessage {
            client_id: self.client_id,
            packet,
            res_tx: Some(res_tx),
            client_tx: None,
        })?;
        // wait for response in coroutine
        let local = self.local_addr.clone();
        tokio::spawn(async move {
            // wait for reply from broker
            match timeout(WAIT_REPLY_TIMEOUT, res_rx).await {
                Ok(Ok(code)) => {
                    if let Err(e) = framed.send(Packet::ReturnCode(code)).await {
                        error!("send reply to client {local} error: {e}",)
                    }
                }
                Ok(Err(e)) => {
                    error!("recv client {local} reply error: {e}")
                }
                Err(_) => error!("process client {local} timeout"),
            }
        });
        Ok(())
    }
}
