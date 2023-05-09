use std::{
    collections::HashMap,
    ops::{Deref, DerefMut},
    sync::Arc,
};

use futures::{SinkExt, StreamExt};
use log::error;
use parking_lot::Mutex;
use s2n_quic::{
    connection::Handle,
    stream::{ReceiveStream, SendStream},
};
use tokio::sync::{mpsc, oneshot};
use tokio_util::codec::{FramedRead, FramedWrite};

use crate::{
    io::{Error, Result},
    protocol::{Packet, PacketCodec, ReturnCode},
};

use super::Request;

pub struct PoolSender(mpsc::Sender<Request>);

impl PoolSender {
    pub async fn send(&self, request: Request) -> Result<()> {
        self.0.send(request).await?;
        Ok(())
    }
}

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
    res_map: Arc<tokio::sync::Mutex<HashMap<u64, oneshot::Sender<Result<()>>>>>,
}

impl PoolInner {
    fn new(handle: Handle) -> Self {
        Self {
            handle,
            idle_streams: Arc::new(Mutex::new(Vec::with_capacity(10))),
            res_map: Arc::new(tokio::sync::Mutex::new(HashMap::new())),
        }
    }

    async fn run(mut self, mut request_rx: mpsc::Receiver<Request>) {
        while let Some(request) = request_rx.recv().await {
            let Request { packet, res_tx } = request;
            // TODO get id from packet
            let id = 0;
            self.add_res_tx(id, res_tx).await;
            let mut framed = match self.get().await {
                Ok(stream) => stream,
                Err(e) => {
                    error!("pool get stream error: {e}");
                    continue;
                }
            };
            if let Err(e) = framed.send(packet).await {
                let res_tx = self.remove_res_tx(id).await.unwrap();
                res_tx.send(Err(e.into())).ok();
            }
        }
    }

    async fn add_res_tx(&self, id: u64, res_tx: oneshot::Sender<Result<()>>) {
        let mut map = self.res_map.lock().await;
        map.insert(id, res_tx);
    }

    async fn remove_res_tx(&self, id: u64) -> Option<oneshot::Sender<Result<()>>> {
        let mut map = self.res_map.lock().await;
        map.remove(&id)
    }

    async fn get(&mut self) -> Result<PooledStream> {
        match self.get_one() {
            Some(stream) => Ok(PooledStream::new_idle(self.clone(), stream)),
            None => {
                let stream = self.handle.open_bidirectional_stream().await?;
                let (recv_stream, send_stream) = stream.split();
                let framed = FramedWrite::new(send_stream, PacketCodec);
                tokio::spawn(
                    self.clone()
                        .start_recv(FramedRead::new(recv_stream, PacketCodec)),
                );
                Ok(PooledStream::new(self.clone(), framed))
            }
        }
    }

    async fn start_recv(self, mut framed: FramedRead<ReceiveStream, PacketCodec>) {
        while let Some(packet_res) = framed.next().await {
            // TODO get id from packet
            let id = 0;
            let Some(res_tx) = self.remove_res_tx(id).await else {
                continue;
            };
            let resp = match packet_res {
                Ok(Packet::Response(ReturnCode::Success)) => Ok(()),
                Ok(Packet::Response(code)) => Err(Error::FromServer(code)),
                Ok(_) => Err(Error::ReceivedUnexpectedPacket),
                Err(e) => Err(Error::Protocol(e)),
            };
            res_tx.send(resp).ok();
        }
    }

    fn get_one(&mut self) -> Option<IdleStream> {
        let mut streams = self.idle_streams.lock();
        streams.pop()
    }

    fn put(&self, stream: IdleStream) {
        let mut streams = self.idle_streams.lock();
        streams.push(stream)
    }
}

/// used in pool
pub struct IdleStream {
    framed: FramedWrite<SendStream, PacketCodec>,
    /// statistics
    used_count: u64,
}

impl IdleStream {
    fn new(framed: FramedWrite<SendStream, PacketCodec>) -> Self {
        Self {
            framed,
            used_count: 0,
        }
    }
}

/// use by users
pub struct PooledStream {
    pool: PoolInner,
    stream: Option<IdleStream>,
}

impl PooledStream {
    fn new(pool: PoolInner, framed: FramedWrite<SendStream, PacketCodec>) -> Self {
        Self::new_idle(pool, IdleStream::new(framed))
    }

    fn new_idle(pool: PoolInner, stream: IdleStream) -> Self {
        Self {
            pool,
            stream: Some(stream),
        }
    }
}

impl Drop for PooledStream {
    fn drop(&mut self) {
        self.pool.put(self.stream.take().unwrap())
    }
}

impl Deref for PooledStream {
    type Target = FramedWrite<SendStream, PacketCodec>;

    fn deref(&self) -> &Self::Target {
        &self.stream.as_ref().unwrap().framed
    }
}

impl DerefMut for PooledStream {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.stream.as_mut().unwrap().framed
    }
}
