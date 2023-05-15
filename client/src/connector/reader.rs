use std::sync::{
    atomic::{AtomicU32, Ordering},
    Arc,
};

use bud_common::{
    id::next_id,
    io::reader::{self, Request},
    protocol::{ControlFlow, Packet, Response, ReturnCode},
};
use log::{error, trace, warn};
use s2n_quic::connection::StreamAcceptor;
use tokio::{
    select,
    sync::{mpsc, oneshot, Mutex},
    task::JoinSet,
};
use tokio_util::sync::CancellationToken;

use crate::consumer::{ConsumeMessage, Consumers, CONSUME_CHANNEL_CAPACITY};

use super::{Error, OutgoingMessage, Result};

pub struct ReadCloser {
    tasks: Arc<Mutex<JoinSet<()>>>,
    inner: reader::Closer,
}

impl ReadCloser {
    fn new(tasks: Arc<Mutex<JoinSet<()>>>, inner: reader::Closer) -> Self {
        Self { tasks, inner }
    }

    pub async fn close(self) {
        self.inner.close().await;
        let mut tasks = self.tasks.lock().await;
        while let Some(res) = tasks.join_next().await {
            if let Err(e) = res {
                error!("client reader task panics: {e}")
            }
        }
    }
}

pub struct Reader {
    consumers: Consumers,
    receiver: mpsc::Receiver<Request>,
    token: CancellationToken,
}

impl Reader {
    pub fn new(
        consumers: Consumers,
        acceptor: StreamAcceptor,
        token: CancellationToken,
    ) -> (Self, ReadCloser) {
        let (reader, receiver, inner_closer) = reader::Reader::new(acceptor, token.clone());
        let mut tasks = JoinSet::new();
        tasks.spawn(reader.run());
        let tasks = Arc::new(Mutex::new(tasks));
        (
            Self {
                consumers,
                receiver,
                token,
            },
            ReadCloser::new(tasks, inner_closer),
        )
    }

    pub async fn run(mut self) {
        loop {
            select! {
                res = self.receiver.recv() => {
                    match res {
                        Some(request) => {
                            trace!("connector::reader: accept a new stream");
                            if let Err(Error::Disconnect) = self.read(request).await {
                                return
                            }
                        },
                        None=> {
                            trace!("connector::reader: accept none, exit");
                            return
                        },
                    }
                }
                _ = self.token.cancelled() => {
                    return
                }
            }
        }
    }

    async fn read(&self, request: Request) -> Result<()> {
        let Request { packet, res_tx } = request;
        match packet {
            Packet::Send(s) => {
                let Some(sender) = self.consumers.get_consumer_sender(s.consumer_id).await else {
                    warn!("recv a message but consumer not found");
                    let packet = Packet::Response(Response { request_id: s.request_id, code: ReturnCode::ConsumerNotFound });
                    res_tx.send(Some(packet)).ok();
                    return Ok(());
                };
                let message = ConsumeMessage {
                    id: s.message_id,
                    payload: s.payload,
                };
                match sender.send(message).await {
                    Ok(_) => {
                        res_tx.send(Some(Packet::ok_response(s.request_id))).ok();
                    }
                    Err(e) => {
                        error!("send message to consumer error: {e}");
                        let packet = Packet::Response(Response {
                            request_id: s.request_id,
                            code: ReturnCode::ConsumerNotFound,
                        });
                        res_tx.send(Some(packet)).ok();
                    }
                }
                Ok(())
            }
            Packet::Disconnect => {
                error!("receive DISCONNECT packet from server");
                res_tx.send(None).ok();
                Err(Error::Disconnect)
            }
            p => {
                error!("client received unexpected packet: {:?}", p.packet_type());
                res_tx.send(None).ok();
                Ok(())
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
