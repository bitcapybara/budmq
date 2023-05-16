use std::sync::Arc;

use futures::{SinkExt, StreamExt};
use log::error;
use s2n_quic::{connection::StreamAcceptor, stream::BidirectionalStream};
use tokio::{
    select,
    sync::{mpsc, oneshot, Mutex},
};
use tokio_util::{
    codec::{FramedRead, FramedWrite},
    sync::CancellationToken,
};

use crate::protocol::{Packet, PacketCodec};

pub struct Request {
    pub packet: Packet,
    pub res_tx: oneshot::Sender<Option<Packet>>,
}

pub struct Reader {
    acceptor: StreamAcceptor,
    tx: mpsc::Sender<Request>,
    token: CancellationToken,
}

impl Reader {
    pub fn new(
        tx: mpsc::Sender<Request>,
        acceptor: StreamAcceptor,
        token: CancellationToken,
    ) -> Self {
        Self {
            acceptor,
            tx,
            token,
        }
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
                    tokio::spawn(listen_on_stream(stream, self.tx.clone(), self.token.child_token()));
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
    tx: mpsc::Sender<Request>,
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
                    // TODO timeout
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
                if let Err(e) = tx.send(Request { packet, res_tx }).await {
                    error!("io::reader send packet error: {e}");
                }
            }
            _ = token.cancelled() => {
                return
            }
        }
    }
}
