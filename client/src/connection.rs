pub mod reader;
pub mod writer;

use std::{io, net::SocketAddr, ops::DerefMut, sync::Arc};

use bud_common::{
    io::{writer::Request, SharedError},
    mtls::MtlsProvider,
    protocol::{
        CloseConsumer, CloseProducer, ConsumeAck, ControlFlow, CreateProducer, Packet,
        ProducerReceipt, Publish, Response, ReturnCode, Subscribe,
    },
    types::{AccessMode, MessageId},
};
use bytes::Bytes;
use chrono::Utc;
use log::trace;
use s2n_quic::{
    client,
    connection::{self, Handle},
};
use tokio::sync::{mpsc, oneshot, Mutex};
use tokio_util::sync::CancellationToken;

use crate::consumer::{ConsumeMessage, SubscribeMessage};

use self::{reader::Reader, writer::Writer};

pub type Result<T> = std::result::Result<T, Error>;

#[derive(Debug, thiserror::Error)]
pub enum Error {
    /// connection disconnected
    #[error("Disconnected")]
    Disconnect,
    /// get connection failed
    #[error("Canceled")]
    Canceled,
    /// error returned by server
    #[error("Error from peer: {0}")]
    FromPeer(ReturnCode),
    /// error from common io
    #[error("Error from bud_common: {0}")]
    CommonIo(#[from] bud_common::io::Error),
    /// error when create new client
    #[error("I/O error: {0}")]
    Io(#[from] io::Error),
    /// s2n quic errors
    #[error("QUIC error: {0}")]
    Quic(String),
}

impl From<s2n_quic::provider::StartError> for Error {
    fn from(e: s2n_quic::provider::StartError) -> Self {
        Self::Quic(e.to_string())
    }
}

impl From<s2n_quic::connection::Error> for Error {
    fn from(e: s2n_quic::connection::Error) -> Self {
        Self::Quic(e.to_string())
    }
}

pub enum Event {
    /// register consumer to client reader
    AddConsumer {
        consumer_id: u64,
        /// sender held by client reader
        /// receiver held by client consumer
        sender: mpsc::UnboundedSender<ConsumeMessage>,
    },
    /// unregister consumer from client reader
    DelConsumer { consumer_id: u64 },
}

pub enum ConnectionState {
    Connected(Arc<Connection>),
    Connecting(Vec<oneshot::Sender<Result<Arc<Connection>>>>),
}

/// held by producer/consumer
pub struct Connection {
    /// send add/del consumer events
    register_tx: mpsc::Sender<Event>,
    /// send request
    request_tx: mpsc::UnboundedSender<Request>,
    /// error to indicate weather producer/consumer need to get a new connection
    error: SharedError,
    /// connection handle
    handle: Handle,
    /// self-managed token
    token: CancellationToken,
    /// keepalive
    keepalive: u16,
}

impl Connection {
    fn new(conn: connection::Connection, ordered: bool, keepalive: u16) -> Self {
        let (handle, acceptor) = conn.split();
        let token = CancellationToken::new();
        let error = SharedError::new();
        let (register_tx, register_rx) = mpsc::channel(1);
        // create reader from s2n_quic::Acceptor
        tokio::spawn(Reader::new(register_rx, acceptor, error.clone(), token.child_token()).run());
        // create writer from s2n_quic::Handle
        let (request_tx, request_rx) = mpsc::unbounded_channel();
        tokio::spawn(
            Writer::new(
                handle.clone(),
                ordered,
                error.clone(),
                token.child_token(),
                request_rx,
                keepalive,
            )
            .run(),
        );
        Self {
            error,
            register_tx,
            token,
            handle,
            keepalive,
            request_tx,
        }
    }

    fn clone_one(&self, ordered: bool) -> Self {
        // create writer from s2n_quic::Handle
        let (request_tx, request_rx) = mpsc::unbounded_channel();
        tokio::spawn(
            Writer::new(
                self.handle.clone(),
                ordered,
                self.error.clone(),
                self.token.child_token(),
                request_rx,
                self.keepalive,
            )
            .run(),
        );
        Self {
            register_tx: self.register_tx.clone(),
            request_tx,
            error: self.error.clone(),
            handle: self.handle.clone(),
            token: self.token.clone(),
            keepalive: self.keepalive,
        }
    }

    pub fn is_valid(&self) -> bool {
        !self.error.is_set()
    }

    pub async fn error(&self) -> Option<bud_common::io::Error> {
        self.error.remove().await
    }

    pub async fn create_producer(
        &self,
        name: &str,
        id: u64,
        topic: &str,
        access_mode: AccessMode,
    ) -> Result<u64> {
        match self
            .send(Packet::CreateProducer(CreateProducer {
                producer_name: name.to_string(),
                topic_name: topic.to_string(),
                access_mode,
                producer_id: id,
            }))
            .await?
        {
            Packet::ProducerReceipt(ProducerReceipt { sequence_id }) => Ok(sequence_id),
            _ => Err(Error::FromPeer(ReturnCode::UnexpectedPacket)),
        }
    }

    pub async fn publish(
        &self,
        producer_id: u64,
        topic: &str,
        sequence_id: u64,
        data: &[u8],
    ) -> Result<()> {
        self.send_ok(Packet::Publish(Publish {
            topic: topic.to_string(),
            sequence_id,
            payload: Bytes::copy_from_slice(data),
            producer_id,
            produce_time: Utc::now(),
        }))
        .await
    }

    pub async fn close_producer(&self, producer_id: u64) -> Result<()> {
        self.send_async(Packet::CloseProducer(CloseProducer { producer_id }))
            .await
    }

    pub async fn close_consumer(&self, consumer_id: u64) -> Result<()> {
        self.send_async(Packet::CloseConsumer(CloseConsumer { consumer_id }))
            .await?;
        self.register_tx
            .send(Event::DelConsumer { consumer_id })
            .await
            .map_err(|_| Error::Disconnect)?;
        Ok(())
    }

    pub async fn subscribe(
        &self,
        consumer_id: u64,
        consumer_name: &str,
        sub: &SubscribeMessage,
        tx: mpsc::UnboundedSender<ConsumeMessage>,
    ) -> Result<()> {
        self.send_ok(Packet::Subscribe(Subscribe {
            consumer_id,
            topic: sub.topic.to_string(),
            sub_name: sub.sub_name.to_string(),
            sub_type: sub.sub_type,
            initial_position: sub.initial_postion,
            consumer_name: consumer_name.to_string(),
        }))
        .await?;
        self.register_tx
            .send(Event::AddConsumer {
                consumer_id,
                sender: tx,
            })
            .await
            .map_err(|_| Error::Disconnect)?;
        Ok(())
    }

    pub async fn unsubscribe(&self, consumer_id: u64) -> Result<()> {
        self.send_ok(Packet::Unsubscribe(bud_common::protocol::Unsubscribe {
            consumer_id,
        }))
        .await?;
        self.register_tx
            .send(Event::DelConsumer { consumer_id })
            .await
            .map_err(|_| Error::Disconnect)?;
        Ok(())
    }

    pub async fn ack(&self, consumer_id: u64, message_id: &MessageId) -> Result<()> {
        self.send_ok(Packet::ConsumeAck(ConsumeAck {
            consumer_id,
            message_id: *message_id,
        }))
        .await
    }

    pub async fn control_flow(&self, consumer_id: u64, permits: u32) -> Result<()> {
        self.send_ok(Packet::ControlFlow(ControlFlow {
            consumer_id,
            permits,
        }))
        .await
    }

    async fn send_ok(&self, packet: Packet) -> Result<()> {
        match self.send(packet).await? {
            Packet::Response(Response { code, .. }) => match code {
                ReturnCode::Success => Ok(()),
                code => Err(Error::FromPeer(code)),
            },
            _ => Err(Error::FromPeer(ReturnCode::UnexpectedPacket)),
        }
    }

    async fn send(&self, packet: Packet) -> Result<Packet> {
        let (res_tx, res_rx) = oneshot::channel();
        self.request_tx
            .send(Request {
                packet,
                res_tx: Some(res_tx),
            })
            .map_err(|_| Error::Disconnect)?;
        match res_rx.await {
            Ok(Ok(packet)) => Ok(packet),
            Ok(Err(bud_common::io::Error::ConnectionDisconnect)) => Err(Error::Disconnect),
            Ok(Err(e)) => Err(e)?,
            Err(_) => Err(Error::Disconnect)?,
        }
    }

    async fn send_async(&self, packet: Packet) -> Result<()> {
        self.request_tx
            .send(Request {
                packet,
                res_tx: None,
            })
            .map_err(|_| Error::Disconnect)
    }
}

/// connection manager held client
#[derive(Clone)]
pub struct ConnectionHandle {
    addr: SocketAddr,
    server_name: String,
    provider: MtlsProvider,
    conn: Arc<Mutex<Option<ConnectionState>>>,
    keepalive: u16,
}

impl ConnectionHandle {
    pub fn new(
        addr: &SocketAddr,
        server_name: &str,
        provider: MtlsProvider,
        keepalive: u16,
    ) -> Self {
        Self {
            addr: addr.to_owned(),
            server_name: server_name.to_string(),
            provider,
            conn: Arc::new(Mutex::new(None)),
            keepalive,
        }
    }

    /// get the connection in manager, setup a new writer
    pub async fn get_connection(&self, ordered: bool) -> Result<Arc<Connection>> {
        let rx = {
            let mut conn = self.conn.lock().await;
            match conn.deref_mut() {
                Some(ConnectionState::Connected(c)) => {
                    if c.is_valid() {
                        let cloned = c.clone_one(ordered);
                        return Ok(Arc::new(cloned));
                    } else {
                        None
                    }
                }
                Some(ConnectionState::Connecting(waiters)) => {
                    let (tx, rx) = oneshot::channel();
                    waiters.push(tx);
                    Some(rx)
                }
                None => None,
            }
        };

        match rx {
            Some(rx) => match rx.await {
                Ok(conn) => conn,
                Err(_) => Err(Error::Canceled),
            },
            None => self.connect(ordered).await,
        }
    }

    async fn connect(&self, ordered: bool) -> Result<Arc<Connection>> {
        let new_conn = match self.connect_inner(ordered).await {
            Ok(c) => c,
            Err(e) => {
                let mut conn = self.conn.lock().await;
                if let Some(ConnectionState::Connecting(waiters)) = conn.deref_mut() {
                    for tx in waiters.drain(..) {
                        tx.send(Err(Error::Canceled)).ok();
                    }
                }
                return Err(e);
            }
        };
        let mut conn = self.conn.lock().await;
        match conn.deref_mut() {
            Some(ConnectionState::Connected(c)) => *c = new_conn.clone(),
            Some(ConnectionState::Connecting(waiters)) => {
                for tx in waiters.drain(..) {
                    tx.send(Ok(new_conn.clone())).ok();
                }
            }
            None => *conn = Some(ConnectionState::Connected(new_conn.clone())),
        }
        Ok(new_conn)
    }

    async fn connect_inner(&self, ordered: bool) -> Result<Arc<Connection>> {
        // TODO loop retry backoff
        // unwrap safe: with_tls error is infallible
        trace!("connector::connect: create client and connect");
        let client: client::Client = client::Client::builder()
            .with_tls(self.provider.clone())
            .unwrap()
            .with_io("0.0.0.0:0")?
            .start()?;
        let connector =
            s2n_quic::client::Connect::new(self.addr).with_server_name(self.server_name.as_str());
        let mut connection = client.connect(connector).await?;
        connection.keep_alive(true)?;
        Ok(Arc::new(Connection::new(
            connection,
            ordered,
            self.keepalive,
        )))
    }
}

/// cancel token, don't need to wait all spawned reader/writer tasks exit
impl Drop for Connection {
    fn drop(&mut self) {
        self.token.cancel()
    }
}
