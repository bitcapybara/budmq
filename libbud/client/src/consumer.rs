use tokio::sync::mpsc;

type Result<T> = std::result::Result<T, Error>;

#[derive(Debug)]
pub enum Error {}

impl std::error::Error for Error {}

impl std::fmt::Display for Error {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        todo!()
    }
}

pub enum SubType {
    Exclusive,
    Shared,
}

pub struct Subscribe {
    topic: String,
    sub_name: String,
    sub_type: SubType,
}

pub struct Consumer {
    permits: u64,
}

impl Consumer {
    pub async fn new(sub: &Subscribe, consumer_rx: mpsc::UnboundedReceiver<()>) -> Result<Self> {
        // send permits packet
        todo!()
    }
}