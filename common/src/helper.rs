use log::{error, info};
use tokio::task::JoinHandle;

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
