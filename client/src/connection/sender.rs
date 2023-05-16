//! used by producer/consumer to send messages

use bud_common::{id::SerialId, protocol::Packet};
use tokio::sync::mpsc;

pub enum Event {
    /// register consumer to client reader
    AddConsumer {
        consumer_id: u64,
        sender: mpsc::UnboundedSender<Packet>,
    },
    /// unregister consumer from client reader
    DelConsumer { consumer_id: u64 },
}

pub struct Sender {
    request_id: SerialId,
    event_tx: mpsc::UnboundedSender<Event>,
}
