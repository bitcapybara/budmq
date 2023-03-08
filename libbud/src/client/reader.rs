use std::time::Duration;

use futures::{SinkExt, StreamExt};
use log::error;
use s2n_quic::{connection::StreamAcceptor, stream::BidirectionalStream};
use tokio::{
    sync::{mpsc, oneshot},
    time::timeout,
};
use tokio_util::codec::Framed;

use super::{Error, Result};
use crate::{
    broker,
    protocol::{Packet, PacketCodec, ReturnCode},
};

const WAIT_REPLY_TIMEOUT: Duration = Duration::from_millis(200);

pub struct Reader {
    client_id: u64,
    broker_tx: mpsc::UnboundedSender<broker::Message>,
    acceptor: StreamAcceptor,
    keepalive: u16,
}

impl Reader {
    pub fn new(
        client_id: u64,
        broker_tx: mpsc::UnboundedSender<broker::Message>,
        acceptor: StreamAcceptor,
        keepalive: u16,
    ) -> Self {
        Self {
            client_id,
            broker_tx,
            acceptor,
            keepalive,
        }
    }

    /// accept new stream to read a packet
    pub async fn read(mut self) -> Result<()> {
        // macro to simplify code
        macro_rules! send {
            ($c: expr, $d:expr, $f: expr) => {
                let req_id = $c.request_id;
                let packet = $d($c);
                self.send(req_id, packet, $f).await?;
            };
        }
        while let Some(stream) = timeout(
            Duration::from_millis(self.keepalive as u64),
            self.acceptor.accept_bidirectional_stream(),
        )
        .await
        .map_err(|_| Error::ClientIdleTimeout)??
        {
            let mut framed = Framed::new(stream, PacketCodec);
            match framed.next().await.ok_or(Error::StreamClosed)?? {
                Packet::Connect(c) => {
                    // Do not allow duplicate connections
                    framed.send(c.ack(ReturnCode::AlreadyConnected)).await?
                }
                Packet::Subscribe(s) => {
                    send!(s, Packet::Subscribe, framed);
                }
                Packet::Unsubscribe(u) => {
                    send!(u, Packet::Unsubscribe, framed);
                }
                Packet::Publish(p) => {
                    send!(p, Packet::Publish, framed);
                }
                Packet::ConsumeAck(c) => {
                    send!(c, Packet::ConsumeAck, framed);
                }
                Packet::ControlFlow(c) => {
                    send!(c, Packet::ControlFlow, framed);
                }
                Packet::Ping => {
                    tokio::spawn(async move {
                        if let Err(e) = framed.send(Packet::Pong).await {
                            error!("send pong packet error: {e}")
                        }
                    });
                }
                Packet::Disconnect => {
                    self.broker_tx.send(broker::Message {
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
        req_id: u64,
        packet: Packet,
        mut framed: Framed<BidirectionalStream, PacketCodec>,
    ) -> Result<()> {
        let (res_tx, res_rx) = oneshot::channel();
        // send to broker
        self.broker_tx.send(broker::Message {
            client_id: self.client_id,
            packet,
            res_tx: Some(res_tx),
            client_tx: None,
        })?;
        // wait for response in coroutine
        tokio::spawn(async move {
            match timeout(WAIT_REPLY_TIMEOUT, res_rx).await {
                Ok(Ok(code)) => {
                    if let Err(e) = framed.send(Packet::ack(req_id, code)).await {
                        error!("send reply to request {} error: {}", req_id, e)
                    }
                }
                Ok(Err(e)) => {
                    error!("request {} reply error: {}", req_id, e)
                }
                Err(_) => error!("request {} wait for reply timeout", req_id),
            }
        });
        Ok(())
    }
}
