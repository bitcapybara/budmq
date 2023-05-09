use std::{
    ops::{Deref, DerefMut},
    sync::Arc,
    time::Duration,
};

use futures::{SinkExt, StreamExt};
use parking_lot::Mutex;
use s2n_quic::{connection::Handle, stream::BidirectionalStream};
use tokio::time::timeout;
use tokio_util::codec::Framed;

use crate::{
    io::{Error, Result},
    protocol::{Packet, PacketCodec, ReturnCode},
};

use super::Request;

#[derive(Clone)]
pub struct Pool(PoolInner);

impl Pool {
    pub fn new(handle: Handle) -> Self {
        Self(PoolInner::new(handle))
    }

    pub async fn send_timeout(&mut self, duration: Duration, request: Request) -> Result<()> {
        let framed = self.0.get().await?;
        tokio::spawn(Self::send_and_wait(framed, duration, request));
        Ok(())
    }

    async fn send_and_wait(mut framed: PooledStream, duration: Duration, request: Request) {
        let Request { packet, res_tx } = request;
        if let Err(e) = framed.send(packet).await {
            res_tx.send(Err(e.into())).ok();
            return;
        }
        let reply = match timeout(duration, framed.next()).await {
            Ok(Some(Ok(Packet::Response(ReturnCode::Success)))) => Ok(()),
            Ok(Some(Ok(Packet::Response(code)))) => Err(Error::FromServer(code)),
            Ok(Some(Ok(_))) => Err(Error::ReceivedUnexpectedPacket),
            Ok(Some(Err(e))) => Err(Error::Protocol(e)),
            Ok(None) => Err(Error::StreamClosed),
            Err(_) => Err(Error::WaitReplyTimeout),
        };
        res_tx.send(reply).ok();
    }
}

#[derive(Clone)]
struct PoolInner {
    handle: Handle,
    idle_streams: Arc<Mutex<Vec<IdleStream>>>,
}

impl PoolInner {
    fn new(handle: Handle) -> Self {
        Self {
            handle,
            idle_streams: Arc::new(Mutex::new(Vec::with_capacity(10))),
        }
    }

    async fn get(&mut self) -> Result<PooledStream> {
        match self.get_one() {
            Some(stream) => Ok(PooledStream::new_idle(self.clone(), stream)),
            None => {
                let stream = self.handle.open_bidirectional_stream().await?;
                let framed = Framed::new(stream, PacketCodec);
                Ok(PooledStream::new(self.clone(), framed))
            }
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
    framed: Framed<BidirectionalStream, PacketCodec>,
    /// statistics
    used_count: u64,
}

impl IdleStream {
    fn new(framed: Framed<BidirectionalStream, PacketCodec>) -> Self {
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
    fn new(pool: PoolInner, framed: Framed<BidirectionalStream, PacketCodec>) -> Self {
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
    type Target = Framed<BidirectionalStream, PacketCodec>;

    fn deref(&self) -> &Self::Target {
        &self.stream.as_ref().unwrap().framed
    }
}

impl DerefMut for PooledStream {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.stream.as_mut().unwrap().framed
    }
}
