use std::sync::Arc;

use futures::{SinkExt, StreamExt};
use log::error;
use s2n_quic::{connection::StreamAcceptor, stream::BidirectionalStream};
use tokio::{
    select,
    sync::{mpsc, oneshot, Mutex},
    task::JoinSet,
};
use tokio_util::{
    codec::{FramedRead, FramedWrite},
    sync::CancellationToken,
};

use crate::protocol::{Packet, PacketCodec};

pub struct PacketRequest {
    packet: Packet,
    res_tx: oneshot::Sender<Option<Packet>>,
}

pub struct ReaderHandler {
    tasks: Arc<Mutex<JoinSet<()>>>,
    receiver: mpsc::Receiver<PacketRequest>,
    token: CancellationToken,
}

impl ReaderHandler {
    fn new(
        tasks: Arc<Mutex<JoinSet<()>>>,
        receiver: mpsc::Receiver<PacketRequest>,
        token: CancellationToken,
    ) -> Self {
        Self {
            tasks,
            receiver,
            token,
        }
    }
    pub async fn recv(&mut self) -> Option<PacketRequest> {
        self.receiver.recv().await
    }
    pub async fn close(self) {
        self.token.cancel();
        let mut tasks = self.tasks.lock().await;
        while (tasks.join_next().await).is_some() {}
    }
}

pub struct Reader {
    acceptor: StreamAcceptor,
    tx: mpsc::Sender<PacketRequest>,
    tasks: Arc<Mutex<JoinSet<()>>>,
    token: CancellationToken,
}

impl Reader {
    pub fn new(acceptor: StreamAcceptor, token: CancellationToken) -> (Self, ReaderHandler) {
        let (tx, rx) = mpsc::channel(1);
        let tasks = Arc::new(Mutex::new(JoinSet::new()));
        (
            Self {
                acceptor,
                tx,
                tasks: tasks.clone(),
                token: token.clone(),
            },
            ReaderHandler::new(tasks, rx, token),
        )
    }

    pub async fn run(mut self) {
        loop {
            select! {
                res = self.acceptor.accept_bidirectional_stream() => {
                    let stream = match res {
                        Ok(Some(stream)) => stream,
                        Ok(None) => {
                            self.token.cancel();
                            return
                        }
                        Err(e) => {
                            error!("no stream could be accepted due to an error: {e}");
                            self.token.cancel();
                            return
                        }
                    };
                    let mut tasks = self.tasks.lock().await;
                    tasks.spawn(listen_on_stream(stream, self.tx.clone(), self.token.child_token()));
                }
                _ = self.token.cancelled() => {
                    return
                }
            }
        }
    }
}

async fn listen_on_stream(
    stream: BidirectionalStream,
    tx: mpsc::Sender<PacketRequest>,
    token: CancellationToken,
) {
    let (recv, send) = stream.split();
    let (mut recv_framed, send_framed) = (
        FramedRead::new(recv, PacketCodec),
        Arc::new(Mutex::new(FramedWrite::new(send, PacketCodec))),
    );
    loop {
        select! {
            res = recv_framed.next() => {
                let packet = match res {
                    Some(Ok(packet)) => packet,
                    Some(Err(e)) => {
                        error!("io::reader decode frame error: {e}");
                        continue;
                    }
                    None => return
                };
                let (res_tx, res_rx) = oneshot::channel();
                // async wait for response
                let send_framed = send_framed.clone();
                tokio::spawn(async move {
                    match res_rx.await {
                        Ok(Some(packet)) => {
                            let mut framed = send_framed.lock().await;
                            if let Err(e) = framed.send(packet).await {
                                error!("io::reader send response error: {e}")
                            }
                        }
                        Err(_) => {
                            error!("io::reader res_tx dropped without send");
                        }
                        Ok(None) => {/* no need to send response */}
                    }
                });
                if let Err(e) = tx.send(PacketRequest { packet, res_tx }).await {
                    error!("io::reader send packet error: {e}");
                }
            }
            _ = token.cancelled() => {
                return
            }
        }
    }
}
