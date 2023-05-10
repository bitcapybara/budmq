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

pub struct Pool {
    inner: PoolInner,
    request_rx: mpsc::Receiver<Request>,
}

impl Pool {
    pub fn new(handle: Handle) -> (Self, PoolSender) {
        let (request_tx, request_rx) = mpsc::channel(1);
        (
            Self {
                inner: PoolInner::new(handle),
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
struct PoolInner {
    handle: Handle,
    idle_streams: Arc<Mutex<Vec<IdleStream>>>,
    res_map: ResMap,
}

impl PoolInner {
    fn new(handle: Handle) -> Self {
        Self {
            handle,
            idle_streams: Arc::new(Mutex::new(Vec::with_capacity(10))),
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
            // framed will recycled after send packet
            if let Err(e) = framed.send(packet).await {
                if let Some(res_tx) = self.res_map.remove_res_tx(id).await {
                    res_tx.send(Err(e.into())).ok();
                }
            }
        }
    }

    async fn get(&mut self) -> Result<PooledStream<PoolInner>> {
        match self.get_one() {
            Some(stream) => Ok(PooledStream::new_idle(self.clone(), stream)),
            None => {
                let stream = self.handle.open_bidirectional_stream().await?;
                let (recv_stream, send_stream) = stream.split();
                let framed = FramedWrite::new(send_stream, PacketCodec);
                tokio::spawn(start_recv(
                    self.res_map.clone(),
                    FramedRead::new(recv_stream, PacketCodec),
                ));
                Ok(PooledStream::new(self.clone(), framed))
            }
        }
    }

    fn get_one(&mut self) -> Option<IdleStream> {
        let mut streams = self.idle_streams.lock();
        streams.pop()
    }
}

impl PoolRecycle for PoolInner {
    fn put(&self, stream: IdleStream) {
        let mut streams = self.idle_streams.lock();
        streams.push(stream)
    }
}
