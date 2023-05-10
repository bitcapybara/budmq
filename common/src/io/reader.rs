use futures::{SinkExt, StreamExt};
use log::error;
use s2n_quic::{connection::StreamAcceptor, stream::BidirectionalStream};
use tokio::{
    select,
    sync::{mpsc, oneshot},
};
use tokio_util::{codec::Framed, sync::CancellationToken};

use crate::protocol::{Packet, PacketCodec};

pub struct PacketRequest {
    packet: Packet,
    res_tx: oneshot::Sender<Option<Packet>>,
}

pub struct Reader {
    acceptor: StreamAcceptor,
    tx: mpsc::Sender<PacketRequest>,
}

impl Reader {
    pub fn new(acceptor: StreamAcceptor) -> (Self, mpsc::Receiver<PacketRequest>) {
        let (tx, rx) = mpsc::channel(1);
        (Self { acceptor, tx }, rx)
    }

    pub async fn run(mut self, token: CancellationToken) {
        loop {
            select! {
                res = self.acceptor.accept_bidirectional_stream() => {
                    let framed = match res {
                        Ok(Some(stream)) => Framed::new(stream, PacketCodec),
                        Ok(None) => {
                            token.cancel();
                            return
                        }
                        Err(e) => {
                            error!("no stream could be accepted due to an error: {e}");
                            token.cancel();
                            return
                        }
                    };
                    tokio::spawn(Self::listen_on_stream(framed, self.tx.clone(), token.child_token()));
                }
                _ = token.cancelled() => {
                    return
                }
            }
        }
    }

    async fn listen_on_stream(
        mut framed: Framed<BidirectionalStream, PacketCodec>,
        tx: mpsc::Sender<PacketRequest>,
        token: CancellationToken,
    ) {
        loop {
            select! {
                res = framed.next() => {
                    let packet = match res {
                        Some(Ok(packet)) => packet,
                        Some(Err(e)) => {
                            error!("io::reader decode frame error: {e}");
                            continue;
                        }
                        None => return
                    };
                    let (res_tx, res_rx) = oneshot::channel();
                    if let Err(e) = tx.send(PacketRequest { packet, res_tx }).await {
                        error!("io::reader send packet error: {e}");
                    }
                    // TODO sync wait
                    match res_rx.await {
                        Ok(Some(packet)) => {
                            if let Err(e) = framed.send(packet).await {
                                error!("io::reader send response error: {e}")
                            }
                        }
                        Err(_) => {
                            error!("io::reader res_tx dropped without send");
                        }
                        Ok(None) => {/* no need to send response */}
                    }
                }
                _ = token.cancelled() => {
                    return
                }
            }
        }
    }
}
