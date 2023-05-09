use std::time::Duration;

use s2n_quic::connection::Handle;
use tokio::sync::oneshot;

use self::{
    pool::{Pool, PoolSender},
    single::Single,
};

use super::Result;
use crate::protocol::Packet;

pub mod pool;
mod single;

pub struct Request {
    pub packet: Packet,
    pub res_tx: oneshot::Sender<Result<()>>,
}

pub enum StreamManager {
    Pool(PoolSender),
    Single(Single),
}

impl StreamManager {
    pub async fn new(handle: Handle, ordered: bool) -> Result<Self> {
        if ordered {
            Ok(Self::Single(Single::new(handle).await?))
        } else {
            let (pool, sender) = Pool::new(handle);
            tokio::spawn(pool.run());
            Ok(Self::Pool(sender))
        }
    }

    pub async fn send_timeout(&mut self, duration: Duration, request: Request) -> Result<()> {
        match self {
            StreamManager::Pool(sender) => sender.send(request).await,
            StreamManager::Single(single) => single.send_timeout(duration, request).await,
        }
    }
}
