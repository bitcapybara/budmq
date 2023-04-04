use std::error::Error;

use log::{error, info};
use tokio::task::JoinHandle;

type Result<E> = std::result::Result<(), E>;

pub async fn wait<E: Error>(handle: JoinHandle<Result<E>>, label: &str) {
    match handle.await {
        Ok(Ok(_)) => {
            info!("{label} task loop exit successfully")
        }
        Ok(Err(e)) => {
            error!("{label} task loop exit error: {e}")
        }
        Err(e) => {
            error!("{label} task loop panic: {e}")
        }
    }
}
