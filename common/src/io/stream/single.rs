use std::sync::Arc;

use futures::SinkExt;
use log::error;
use parking_lot::Mutex;
use s2n_quic::connection::Handle;
use tokio::sync::mpsc;
use tokio_util::codec::{FramedRead, FramedWrite};

use crate::{io::Result, protocol::PacketCodec};

use super::{
    helper::start_recv, IdleStream, PoolRecycle, PoolSender, PooledStream, Request, ResMap,
};

pub struct Single {
    inner: SingleInner,
    request_rx: mpsc::Receiver<Request>,
}

impl Single {
    pub fn new(handle: Handle) -> (Self, PoolSender) {
        let (request_tx, request_rx) = mpsc::channel(1);
        (
            Self {
                inner: SingleInner::new(handle),
                request_rx,
            },
            PoolSender(request_tx),
        )
    }

    pub async fn run(self) {
        self.inner.run(self.request_rx).await
    }
}

#[derive(Clone)]
pub struct SingleInner {
    handle: Handle,
    stream: Arc<Mutex<Option<IdleStream>>>,
    res_map: ResMap,
}

impl SingleInner {
    fn new(handle: Handle) -> Self {
        Self {
            handle,
            stream: Arc::new(Mutex::new(None)),
            res_map: ResMap::default(),
        }
    }

    async fn run(mut self, mut request_rx: mpsc::Receiver<Request>) {
        while let Some(request) = request_rx.recv().await {
            let Request { packet, res_tx } = request;
            // TODO get id from packet
            let id = 0;
            self.res_map.add_res_tx(id, res_tx).await;
            let mut framed = match self.get().await {
                Ok(stream) => stream,
                Err(e) => {
                    error!("pool get stream error: {e}");
                    continue;
                }
            };
            if let Err(e) = framed.send(packet).await {
                if let Some(res_tx) = self.res_map.remove_res_tx(id).await {
                    res_tx.send(Err(e.into())).ok();
                }
            }
        }
    }

    async fn get(&mut self) -> Result<PooledStream<SingleInner>> {
        match self.get_inner() {
            Some(stream) => Ok(PooledStream::new_idle(self.clone(), stream)),
            None => {
                let stream = self.handle.open_bidirectional_stream().await?;
                let (recv_stream, send_stream) = stream.split();
                tokio::spawn(start_recv(
                    self.res_map.clone(),
                    FramedRead::new(recv_stream, PacketCodec),
                ));
                let framed = FramedWrite::new(send_stream, PacketCodec);
                Ok(PooledStream::new(self.clone(), framed))
            }
        }
    }

    fn get_inner(&self) -> Option<IdleStream> {
        let mut stream = self.stream.lock();
        stream.take()
    }
}

impl PoolRecycle for SingleInner {
    fn put(&self, idle_stream: super::IdleStream) {
        let mut stream = self.stream.lock();
        stream.replace(idle_stream);
    }
}
