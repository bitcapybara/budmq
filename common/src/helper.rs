use log::{error, info};
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;

pub async fn wait(handle: JoinHandle<()>, label: &str, token: CancellationToken) {
    match handle.await {
        Ok(_) => {
            info!("{label} task loop exit successfully")
        }
        Err(e) => {
            error!("{label} task loop panic: {e}")
        }
    }
    token.cancel();
}
