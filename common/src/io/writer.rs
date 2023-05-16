use log::error;
use s2n_quic::connection::Handle;
use tokio::{select, sync::mpsc};
use tokio_util::sync::CancellationToken;

use super::{
    stream::{pool::PoolInner, single::SingleInner, PoolSender, Request, StreamPool},
    Result,
};

pub struct WriterBuilder {
    handle: Handle,
    ordered: bool,
    token: CancellationToken,
}

impl WriterBuilder {
    fn new(handle: Handle, token: CancellationToken) -> Self {
        Self {
            handle,
            ordered: false,
            token,
        }
    }

    pub fn ordered(mut self, ordered: bool) -> Self {
        self.ordered = ordered;
        self
    }

    pub async fn build(self) -> Result<(Writer, mpsc::Sender<Request>)> {
        let (tx, rx) = mpsc::channel(1);
        let pool_sender = if self.ordered {
            let (single, sender) =
                StreamPool::<SingleInner>::new(self.handle, self.token.child_token());
            tokio::spawn(single.run());
            sender
        } else {
            let (single, sender) =
                StreamPool::<PoolInner>::new(self.handle, self.token.child_token());
            tokio::spawn(single.run());
            sender
        };
        Ok((
            Writer {
                pool_sender,
                ordered: self.ordered,
                rx,
                token: self.token,
            },
            tx,
        ))
    }
}

pub struct Writer {
    pool_sender: PoolSender,
    ordered: bool,
    rx: mpsc::Receiver<Request>,
    token: CancellationToken,
}

impl Writer {
    pub fn builder(handle: Handle, token: CancellationToken) -> WriterBuilder {
        WriterBuilder::new(handle, token)
    }

    pub async fn run(mut self) {
        loop {
            select! {
                res = self.rx.recv() => {
                    let Some(request) = res else {
                        self.token.cancel();
                        return
                    };
                    if let Err(e) = self.pool_sender.send(request).await {
                        error!("error occurs while sending packet: {e}")
                    }
                }
                _ = self.token.cancelled() => {
                    return
                }
            }
        }
    }
}
