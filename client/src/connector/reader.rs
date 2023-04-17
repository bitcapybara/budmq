use std::sync::{
    atomic::{AtomicU32, Ordering},
    Arc,
};

use bud_common::protocol::{ControlFlow, Packet, PacketCodec, ReturnCode};
use futures::TryStreamExt;
use log::{error, warn};
use s2n_quic::{connection::StreamAcceptor, stream::BidirectionalStream};
use tokio::sync::{mpsc, oneshot};
use tokio_util::codec::Framed;

use crate::consumer::{ConsumeMessage, Consumers, CONSUME_CHANNEL_CAPACITY};

use super::{Error, OutgoingMessage, Result};

pub struct Reader {
    acceptor: StreamAcceptor,
    consumers: Consumers,
}

impl Reader {
    pub fn new(acceptor: StreamAcceptor, consumers: Consumers) -> Self {
        Self {
            acceptor,
            consumers,
        }
    }

    pub async fn run(mut self) {
        loop {
            match self.acceptor.accept_bidirectional_stream().await {
                Ok(Some(stream)) => self.read(stream).await,
                Ok(None) => return,
                Err(e) => {
                    error!("connector reader accept stream error: {e}")
                }
            }
        }
    }

    async fn read(&self, stream: BidirectionalStream) {
        let consumers = self.consumers.clone();
        tokio::spawn(async move {
            let mut framed = Framed::new(stream, PacketCodec);
            match framed.try_next().await {
                Ok(Some(Packet::Send(s))) => {
                    let Some(sender) = consumers.get_consumer(s.consumer_id).await else {
                        warn!("recv a message but consumer not found");
                        return
                    };
                    if let Err(e) = sender
                        .send(ConsumeMessage {
                            payload: s.payload,
                            id: s.message_id,
                        })
                        .await
                    {
                        error!("send message to consumer error: {e}")
                    }
                }
                Ok(Some(p)) => {
                    error!("client received unexpected packet: {:?}", p.packet_type());
                }
                Ok(None) => {
                    error!("client reader stream closed");
                }
                Err(e) => {
                    error!("client reader error: {e}");
                }
            }
        });
    }
}

#[derive(Clone)]
pub struct ConsumerSender {
    consumer_id: u64,
    permits: Arc<AtomicU32>,
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
        let (res_tx, res_rx) = oneshot::channel();
        if prev <= CONSUME_CHANNEL_CAPACITY / 2 {
            self.server_tx.send(OutgoingMessage {
                packet: Packet::ControlFlow(ControlFlow {
                    consumer_id: self.consumer_id,
                    permits: self.permits.load(Ordering::SeqCst),
                }),
                res_tx,
            })?;
        }
        match res_rx.await?? {
            ReturnCode::Success => Ok(()),
            code => Err(Error::FromServer(code)),
        }
    }
}
