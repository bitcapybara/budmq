use std::collections::HashMap;

use bud_common::{
    io::{
        reader::{self, Request},
        SharedError,
    },
    protocol::{Packet, Response, ReturnCode},
};
use chrono::Utc;
use log::{error, trace, warn};
use s2n_quic::connection::StreamAcceptor;
use tokio::{select, sync::mpsc};
use tokio_util::sync::CancellationToken;

use crate::consumer::ConsumeMessage;

use super::{Error, Event, Result};

pub struct Reader {
    /// hold sender to send message to consumer
    consumers: HashMap<u64, mpsc::UnboundedSender<ConsumeMessage>>,
    /// receive messages from io::reader
    receiver: mpsc::Receiver<Request>,
    /// receive consumer events
    register_rx: mpsc::Receiver<Event>,
    token: CancellationToken,
}

impl Reader {
    pub fn new(
        register_rx: mpsc::Receiver<Event>,
        acceptor: StreamAcceptor,
        error: SharedError,
        token: CancellationToken,
    ) -> Self {
        let (sender, receiver) = mpsc::channel(1);
        tokio::spawn(reader::Reader::new(sender, acceptor, error, token.clone()).run());
        let consumers = HashMap::new();
        Self {
            consumers,
            receiver,
            register_rx,
            token,
        }
    }

    /// TODO impl Future trait
    pub async fn run(mut self) {
        loop {
            select! {
                res = self.receiver.recv() => {
                    match res {
                        Some(request) => {
                            trace!("connector::reader: new reqeust");
                            if let Err(Error::Disconnect) = self.read(request).await {
                                return
                            }
                        },
                        None=> {
                            trace!("connector::reader: accept none, exit");
                            return
                        },
                    }
                }
                res = self.register_rx.recv() => {
                    let Some(event) = res else {
                        return
                    };
                    match event {
                        Event::AddConsumer { consumer_id, sender } => {
                            self.consumers.insert(consumer_id, sender);
                        },
                        Event::DelConsumer { consumer_id } => {
                            self.consumers.remove(&consumer_id);
                        },
                    }
                }
                _ = self.token.cancelled() => {
                    return
                }
            }
        }
    }

    async fn read(&self, request: Request) -> Result<()> {
        let Request { packet, res_tx } = request;
        match packet {
            Packet::Send(s) => {
                let Some(sender) = self.consumers.get(&s.consumer_id) else {
                    warn!("recv a message but consumer not found");
                    let packet = Packet::Response(Response {  code: ReturnCode::ConsumerNotFound });
                    res_tx.send(Some(packet)).ok();
                    return Ok(());
                };
                let message = ConsumeMessage {
                    id: s.message_id,
                    payload: s.payload,
                    produce_time: s.produce_time,
                    send_time: s.send_time,
                    receive_time: Utc::now(),
                };
                match sender.send(message) {
                    Ok(_) => {
                        res_tx.send(Some(Packet::ok_response())).ok();
                    }
                    Err(e) => {
                        error!("send message to consumer error: {e}");
                        let packet = Packet::Response(Response {
                            code: ReturnCode::ConsumerNotFound,
                        });
                        res_tx.send(Some(packet)).ok();
                    }
                }
                Ok(())
            }
            Packet::Disconnect => {
                error!("receive DISCONNECT packet from server");
                res_tx.send(None).ok();
                Err(Error::Disconnect)
            }
            p => {
                error!("client received unexpected packet: {:?}", p.packet_type());
                res_tx.send(None).ok();
                Ok(())
            }
        }
    }
}
