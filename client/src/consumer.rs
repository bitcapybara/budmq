use std::sync::{
    atomic::{AtomicU32, Ordering},
    Arc,
};

use bud_common::{
    protocol::ReturnCode,
    subscription::{InitialPostion, SubType},
};
use bytes::Bytes;
use tokio::sync::{mpsc, oneshot};

use crate::connection::writer::OutgoingMessage;

pub const CONSUME_CHANNEL_CAPACITY: u32 = 1000;

type Result<T> = std::result::Result<T, Error>;

#[derive(Debug)]
pub enum Error {
    FromServer(ReturnCode),
    Internal(String),
}

impl std::error::Error for Error {}

impl std::fmt::Display for Error {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Error::FromServer(code) => write!(f, "receive from server: {code}"),
            Error::Internal(e) => write!(f, "internal error: {e}"),
        }
    }
}

impl<T> From<mpsc::error::SendError<T>> for Error {
    fn from(e: mpsc::error::SendError<T>) -> Self {
        Self::Internal(e.to_string())
    }
}

impl From<oneshot::error::RecvError> for Error {
    fn from(e: oneshot::error::RecvError) -> Self {
        Self::Internal(e.to_string())
    }
}

#[derive(Clone)]
pub struct SubscribeMessage {
    pub topic: String,
    pub sub_name: String,
    pub sub_type: SubType,
    pub initial_postion: InitialPostion,
}

pub struct ConsumeMessage {
    pub id: u64,
    pub payload: Bytes,
}

pub struct Consumer {
    pub id: u64,
    /// remaining space of the channel
    permits: Arc<AtomicU32>,
    server_tx: mpsc::UnboundedSender<OutgoingMessage>,
    consumer_rx: mpsc::UnboundedReceiver<ConsumeMessage>,
}

impl Consumer {
    pub async fn new(
        _id: u64,
        _permits: Arc<AtomicU32>,
        _sub: &SubscribeMessage,
        _server_tx: mpsc::UnboundedSender<OutgoingMessage>,
        _consumer_rx: mpsc::UnboundedReceiver<ConsumeMessage>,
    ) -> Result<Self> {
        // send subscribe message
        // let (sub_res_tx, sub_res_rx) = oneshot::channel();
        // server_tx.send(OutgoingMessage {
        //     packet: Packet::Subscribe(Subscribe {
        //         topic: sub.topic.clone(),
        //         sub_name: sub.sub_name.clone(),
        //         sub_type: sub.sub_type,
        //         consumer_id: id,
        //         initial_position: sub.initial_postion,
        //         request_id: next_id(),
        //     }),
        //     res_tx: sub_res_tx,
        // })?;
        // let code = sub_res_rx.await??;
        // if code != ReturnCode::Success {
        //     return Err(Error::FromServer(code));
        // }
        // // send permits packet on init
        // let (permits_res_tx, permits_res_rx) = oneshot::channel();
        // server_tx.send(OutgoingMessage {
        //     packet: Packet::ControlFlow(ControlFlow {
        //         consumer_id: id,
        //         permits: permits.load(Ordering::SeqCst),
        //         request_id: next_id(),
        //     }),
        //     res_tx: permits_res_tx,
        // })?;
        // let code = permits_res_rx.await??;
        // if code != ReturnCode::Success {
        //     return Err(Error::FromServer(code));
        // }
        // Ok(Self {
        //     id,
        //     permits,
        //     server_tx,
        //     consumer_rx,
        // })
        todo!()
    }

    pub async fn next(&mut self) -> Option<ConsumeMessage> {
        let msg = self.consumer_rx.recv().await;
        if msg.is_some() {
            self.permits.fetch_add(1, Ordering::SeqCst);
        }
        msg
    }

    pub async fn ack(&self, _message_id: u64) -> Result<()> {
        // let (res_tx, res_rx) = oneshot::channel();
        // self.server_tx.send(OutgoingMessage {
        //     packet: Packet::ConsumeAck(ConsumeAck {
        //         consumer_id: self.id,
        //         message_id,
        //         request_id: next_id(),
        //     }),
        //     res_tx,
        // })?;
        // match res_rx.await?? {
        //     ReturnCode::Success => Ok(()),
        //     code => Err(Error::FromServer(code)),
        // }
        todo!()
    }
}
