use s2n_quic::connection;
use tokio::{
    sync::{mpsc, oneshot},
    time,
};

use crate::protocol::{self, ReturnCode};

pub mod conn;
pub mod reader;
pub mod stream;
pub mod writer;

pub type Result<T> = std::result::Result<T, Error>;

#[derive(Debug)]
pub enum Error {
    FromPeer(ReturnCode),
    Send(String),
    ResDropped(oneshot::error::RecvError),
    Timeout,
    Connection(connection::Error),
    Protocol(protocol::Error),
}

impl std::error::Error for Error {}

impl std::fmt::Display for Error {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Error::FromPeer(e) => write!(f, "error from peer: {e}"),
            Error::Send(e) => write!(f, "message send error: {e}"),
            Error::ResDropped(e) => write!(f, "res tx dropped without send: {e}"),
            Error::Timeout => write!(f, "wait for message timeout"),
            Error::Connection(e) => write!(f, "connection error: {e}"),
            Error::Protocol(e) => write!(f, "protocol error: {e}"),
        }
    }
}

impl<T> From<mpsc::error::SendError<T>> for Error {
    fn from(e: mpsc::error::SendError<T>) -> Self {
        Self::Send(e.to_string())
    }
}

impl From<oneshot::error::RecvError> for Error {
    fn from(e: oneshot::error::RecvError) -> Self {
        Self::ResDropped(e)
    }
}

impl From<time::error::Elapsed> for Error {
    fn from(_: time::error::Elapsed) -> Self {
        Self::Timeout
    }
}

impl From<connection::Error> for Error {
    fn from(e: connection::Error) -> Self {
        Self::Connection(e)
    }
}

impl From<protocol::Error> for Error {
    fn from(e: protocol::Error) -> Self {
        Self::Protocol(e)
    }
}
