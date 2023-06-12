use futures::{SinkExt, StreamExt};
use log::error;
use s2n_quic::{connection::StreamAcceptor, stream::BidirectionalStream};
use tokio::{
    select,
    sync::{mpsc, oneshot},
};
use tokio_util::{
    codec::{FramedRead, FramedWrite},
    sync::CancellationToken,
};

use crate::{
    io::Error,
    protocol::{Packet, PacketCodec},
};

use super::SharedError;

pub struct Request {
    pub packet: Packet,
    pub res_tx: oneshot::Sender<Option<Packet>>,
}

pub struct Reader {
    acceptor: StreamAcceptor,
    tx: mpsc::Sender<Request>,
    error: SharedError,
    token: CancellationToken,
}

impl Reader {
    pub fn new(
        tx: mpsc::Sender<Request>,
        acceptor: StreamAcceptor,
        error: SharedError,
        token: CancellationToken,
    ) -> Self {
        Self {
            acceptor,
            tx,
            token,
            error,
        }
    }

    pub async fn run(mut self) {
        loop {
            select! {
                res = self.acceptor.accept_bidirectional_stream() => {
                    let stream = match res {
                        Ok(Some(stream)) => stream,
                        Ok(None) => {
                            self.error.set(Error::ConnectionDisconnect).await;
                            return
                        }
                        Err(e) => {
                            self.error.set(Error::ConnectionDisconnect).await;
                            error!("no stream could be accepted due to an error: {e}");
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
    let (mut recv_framed, mut send_framed) = (
        // read packet from server
        FramedRead::new(recv, PacketCodec),
        // send response packet to server
        FramedWrite::new(send, PacketCodec),
    );
    loop {
        select! {
            res = recv_framed.next() => {
                let packet = match res {
                    Some(Ok(packet)) => packet,
                    Some(Err(e)) => {
                        error!("io reader decode packet error: {e}");
                        return;
                    }
                    None => return
                };
                let (res_tx, res_rx) = oneshot::channel();
                // send packet to client
                let token = token.clone();
                if let Err(e) = tx.send(Request { packet, res_tx }).await {
                    error!("io::reader send packet error: {e}");
                }
                // wait for reply and send to server
                select! {
                    // TODO timeout?
                    res = res_rx => {
                        match res {
                            Ok(Some(packet)) => {
                                if let Err(e) = send_framed.send(packet).await {
                                    error!("io::reader send response error: {e}")
                                }
                            }
                            Err(_) => {
                                error!("io::reader res_tx dropped without send");
                            }
                            Ok(None) => {/* no need to send response */}
                        }
                    }
                    _ = token.cancelled() => {}
                }
            }
            _ = token.cancelled() => {
                return
            }
        }
    }
}
