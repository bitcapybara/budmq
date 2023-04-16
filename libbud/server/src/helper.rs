use std::error::Error;

use log::{error, info};
use tokio::task::JoinHandle;

type Result<E> = std::result::Result<(), E>;

pub async fn wait_result<E: Error>(handle: JoinHandle<Result<E>>, label: &str) {
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

pub async fn wait(handle: JoinHandle<()>, label: &str) {
    match handle.await {
        Ok(_) => {
            info!("{label} task loop exit successfully")
        }
        Err(e) => {
            error!("{label} task loop panic: {e}")
        }
    }
}
