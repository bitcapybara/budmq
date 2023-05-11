use s2n_quic::connection;
use tokio::{
    sync::{mpsc, oneshot},
    time,
};

use crate::protocol::{self, ReturnCode};

mod reader;
mod stream;
mod writer;

pub type Result<T> = std::result::Result<T, Error>;

#[derive(Debug)]
pub enum Error {
    WaitReplyTimeout,
    StreamClosed,
    FromServer(ReturnCode),
}

impl std::error::Error for Error {}

impl std::fmt::Display for Error {
    fn fmt(&self, _f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        todo!()
    }
}

impl<T> From<mpsc::error::SendError<T>> for Error {
    fn from(_e: mpsc::error::SendError<T>) -> Self {
        // sender send message while writer already dropped
        todo!()
    }
}

impl From<oneshot::error::RecvError> for Error {
    fn from(_e: oneshot::error::RecvError) -> Self {
        // res_tx dropped without send
        todo!()
    }
}

impl From<time::error::Elapsed> for Error {
    fn from(_e: time::error::Elapsed) -> Self {
        // send and wait for reply timeout
        todo!()
    }
}

impl From<connection::Error> for Error {
    fn from(_e: connection::Error) -> Self {
        // handle open stream error
        todo!()
    }
}

impl From<protocol::Error> for Error {
    fn from(_e: protocol::Error) -> Self {
        // framed.send()
        todo!()
    }
}
