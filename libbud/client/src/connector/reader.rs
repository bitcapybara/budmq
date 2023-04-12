use std::sync::{
    atomic::{AtomicU32, Ordering},
    Arc,
};

use futures::TryStreamExt;
use libbud_common::protocol::{ControlFlow, Packet, PacketCodec, ReturnCode};
use log::warn;
use s2n_quic::connection::StreamAcceptor;
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

    pub async fn run(mut self) -> Result<()> {
        // receive message from broker
        while let Some(stream) = self.acceptor.accept_bidirectional_stream().await? {
            let mut framed = Framed::new(stream, PacketCodec);
            match framed.try_next().await?.ok_or(Error::StreamClosed)? {
                Packet::Send(s) => {
                    let Some(sender) = self.consumers.get_consumer(s.consumer_id).await else {
                        warn!("recv a message but consumer not found");
                        continue;
                    };
                    sender.send(ConsumeMessage { payload: s.payload }).await?;
                }
                _ => return Err(Error::UnexpectedPacket),
            }
        }
        Ok(())
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
