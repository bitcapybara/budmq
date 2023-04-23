use std::error::Error;

use log::{error, info};
use tokio::task::JoinHandle;

pub async fn wait_result<E>(handle: JoinHandle<Result<(), E>>, label: &str)
where
    E: Error,
{
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