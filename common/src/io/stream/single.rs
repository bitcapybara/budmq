use std::{sync::Arc, time::Duration};

use futures::{SinkExt, StreamExt};
use s2n_quic::{connection::Handle, stream::BidirectionalStream};
use tokio::{sync::Mutex, time::timeout};
use tokio_util::codec::Framed;

use crate::{
    io::{Error, Result},
    protocol::{Packet, PacketCodec, ReturnCode},
};

use super::Request;

pub struct Single(SingleInner);

impl Single {
    pub async fn new(handle: Handle) -> Result<Self> {
        Ok(Self(SingleInner::new(handle).await?))
    }

    pub async fn send_timeout(&mut self, duration: Duration, request: Request) -> Result<()> {
        let framed = self.0.get();
        tokio::spawn(Self::send_and_wait(framed, duration, request));
        Ok(())
    }

    async fn send_and_wait(
        framed: Arc<Mutex<Framed<BidirectionalStream, PacketCodec>>>,
        duration: Duration,
        request: Request,
    ) {
        let Request { packet, res_tx } = request;
        let mut framed = framed.lock().await;
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

pub struct SingleInner {
    handle: Handle,
    stream: Arc<Mutex<Framed<BidirectionalStream, PacketCodec>>>,
}

impl SingleInner {
    async fn new(mut handle: Handle) -> Result<Self> {
        let stream = handle.open_bidirectional_stream().await?;
        let framed = Framed::new(stream, PacketCodec);
        Ok(Self {
            handle,
            stream: Arc::new(Mutex::new(framed)),
        })
    }

    fn get(&self) -> Arc<Mutex<Framed<BidirectionalStream, PacketCodec>>> {
        self.stream.clone()
    }
}
