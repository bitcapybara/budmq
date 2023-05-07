use futures::StreamExt;
use log::error;
use s2n_quic::{connection::StreamAcceptor, stream::BidirectionalStream};
use tokio::{select, sync::mpsc};
use tokio_util::{codec::Framed, sync::CancellationToken};

use crate::protocol::{Packet, PacketCodec};

pub struct Reader {
    acceptor: StreamAcceptor,
    tx: mpsc::Sender<Packet>,
}

impl Reader {
    pub fn new(acceptor: StreamAcceptor) -> (Self, mpsc::Receiver<Packet>) {
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
        tx: mpsc::Sender<Packet>,
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
                    if let Err(e) = tx.send(packet).await {
                        error!("io::reader send packet error: {e}");
                    }
                }
                _ = token.cancelled() => {
                    return
                }
            }
        }
    }
}
