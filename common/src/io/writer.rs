use log::error;
use s2n_quic::connection::Handle;
use tokio::{select, sync::mpsc, task::JoinSet};
use tokio_util::sync::CancellationToken;

use super::{
    stream::{self, pool::PoolInner, single::SingleInner, PoolSender, Request, StreamPool},
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

    pub async fn build(self) -> Result<(Writer, mpsc::Sender<Request>, Closer)> {
        let (tx, rx) = mpsc::channel(1);
        let mut tasks = JoinSet::new();
        let (pool_sender, pool_closer) = if self.ordered {
            let (single, sender, closer) =
                StreamPool::<SingleInner>::new(self.handle, self.token.child_token());
            tasks.spawn(single.run());
            (sender, closer)
        } else {
            let (single, sender, closer) =
                StreamPool::<PoolInner>::new(self.handle, self.token.child_token());
            tasks.spawn(single.run());
            (sender, closer)
        };
        let inner_closer = Closer::new(tasks, pool_closer, self.token.clone());
        Ok((
            Writer {
                pool_sender,
                ordered: self.ordered,
                rx,
                token: self.token,
            },
            tx,
            inner_closer,
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

pub struct Closer {
    tasks: JoinSet<()>,
    pool_closer: stream::Closer,
    token: CancellationToken,
}

impl Closer {
    fn new(tasks: JoinSet<()>, pool_closer: stream::Closer, token: CancellationToken) -> Self {
        Self {
            token,
            tasks,
            pool_closer,
        }
    }

    pub async fn close(mut self) {
        self.token.cancel();
        self.pool_closer.close().await;
        while self.tasks.join_next().await.is_some() {}
    }
}
