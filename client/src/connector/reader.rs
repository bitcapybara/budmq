use std::sync::{
    atomic::{AtomicU32, Ordering},
    Arc,
};

use bud_common::{
    id::next_id,
    protocol::{ControlFlow, Packet, PacketCodec, Response, ReturnCode},
};
use futures::{SinkExt, TryStreamExt};
use log::{error, trace, warn};
use s2n_quic::{connection::StreamAcceptor, stream::BidirectionalStream};
use tokio::{
    select,
    sync::{mpsc, oneshot},
};
use tokio_util::{codec::Framed, sync::CancellationToken};

use crate::consumer::{ConsumeMessage, Consumers, CONSUME_CHANNEL_CAPACITY};

use super::{Error, OutgoingMessage, Result};

pub struct Reader {
    consumers: Consumers,
}

impl Reader {
    pub fn new(consumers: Consumers) -> Self {
        Self { consumers }
    }

    pub async fn run(self, mut acceptor: StreamAcceptor, token: CancellationToken) {
        loop {
            select! {
                res = acceptor.accept_bidirectional_stream() => {
                    match res {
                        Ok(Some(stream)) => {
                            trace!("connector::reader: accept a new stream");
                            let mut framed = Framed::new(stream, PacketCodec);
                            let (id, code) = match self.read(&mut framed, self.consumers.clone(), token.clone()).await {
                                Ok(id) => (Some(id), ReturnCode::Success),
                                Err(Error::ReturnCode(code)) => (None, code),
                                Err(e) => {
                                    error!("client process SEND packet error: {e}");
                                    continue;
                                }
                            };
                            if let Err(e) = framed.send(Packet::Response(Response { request_id: id.unwrap_or_else(next_id), code })).await {
                                error!("client connector reader send reponse error: {e}");
                            }
                        },
                        Ok(None) => {
                            trace!("connector::reader: accept none, exit");
                            token.cancel();
                            return
                        },
                        Err(e) => {
                            error!("connector reader accept stream error: {e}");
                            token.cancel();
                            return
                        }
                    }
                }
                _ = token.cancelled() => {
                    return
                }
            }
        }
    }

    async fn read(
        &self,
        framed: &mut Framed<BidirectionalStream, PacketCodec>,
        consumers: Consumers,
        token: CancellationToken,
    ) -> Result<u64> {
        select! {
            biased;
            _ = token.cancelled() => {
                Ok(0)
            }
            res = framed.try_next() => {
                match res {
                    Ok(Some(Packet::Send(s))) => {
                        let Some(sender) = consumers.get_consumer_sender(s.consumer_id).await else {
                            warn!("recv a message but consumer not found");
                            return Err(Error::ReturnCode(ReturnCode::ConsumerNotFound));
                        };
                        if let Err(e) = sender
                            .send(ConsumeMessage {
                                payload: s.payload,
                                id: s.message_id,
                            })
                            .await
                        {
                            error!("send message to consumer error: {e}");
                            return Err(Error::ReturnCode(ReturnCode::InternalError))
                        }
                        Ok(s.request_id)
                    }
                    Ok(Some(Packet::Disconnect)) => {
                        error!("receive DISCONNECT packet from server");
                        Err(Error::Disconnect)
                    }
                    Ok(Some(p)) => {
                        error!("client received unexpected packet: {:?}", p.packet_type());
                        Err(Error::ReturnCode(ReturnCode::UnexpectedPacket))
                    }
                    Ok(None) => {
                        error!("client reader stream closed");
                        Err(Error::StreamClosed)
                    }
                    Err(e) => {
                        error!("client reader error: {e}");
                        Err(Error::FromQuic(e.to_string()))
                    }
                }
            }
        }
    }
}

#[derive(Clone)]
pub struct ConsumerSender {
    pub consumer_id: u64,
    pub permits: Arc<AtomicU32>,
    server_tx: mpsc::UnboundedSender<OutgoingMessage>,
    consumer_tx: mpsc::UnboundedSender<ConsumeMessage>,
}

impl ConsumerSender {
    pub fn new(
        consumer_id: u64,
        permits: Arc<AtomicU32>,
        server_tx: mpsc::UnboundedSender<OutgoingMessage>,
        consumer_tx: mpsc::UnboundedSender<ConsumeMessage>,
    ) -> Self {
        Self {
            permits,
            consumer_tx,
            server_tx,
            consumer_id,
        }
    }

    pub async fn send(&self, message: ConsumeMessage) -> Result<()> {
        self.consumer_tx.send(message)?;
        let prev = self.permits.fetch_sub(1, Ordering::SeqCst);
        if prev > CONSUME_CHANNEL_CAPACITY / 2 {
            return Ok(());
        }
        let (res_tx, res_rx) = oneshot::channel();
        self.server_tx.send(OutgoingMessage {
            packet: Packet::ControlFlow(ControlFlow {
                request_id: next_id(),
                consumer_id: self.consumer_id,
                permits: self.permits.load(Ordering::SeqCst),
            }),
            res_tx,
        })?;
        match res_rx.await?? {
            ReturnCode::Success => Ok(()),
            code => Err(Error::FromServer(code)),
        }
    }
}
