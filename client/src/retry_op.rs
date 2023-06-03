use std::{cmp::max, sync::Arc, time::Duration};

use bud_common::types::AccessMode;
use tokio::{sync::mpsc, time::sleep};

use crate::{
    client::RetryOptions,
    connection::{self, Connection, ConnectionHandle},
    consumer::{ConsumeMessage, SubscribeMessage, CONSUME_CHANNEL_CAPACITY},
};

pub async fn consumer_reconnect(
    consumer_id: u64,
    consumer_name: &str,
    sub_message: &SubscribeMessage,
    retry_opts: &Option<RetryOptions>,
    conn_handle: &ConnectionHandle,
) -> connection::Result<(Arc<Connection>, mpsc::UnboundedReceiver<ConsumeMessage>)> {
    let Some(retry_opts) = retry_opts else {
        return Err(connection::Error::Disconnect);
    };
    let mut delay = retry_opts.min_retry_delay;
    let mut count = 0;
    loop {
        match conn_handle.get_connection(false).await {
            Ok(conn) => {
                let (tx, rx) = mpsc::unbounded_channel();
                match conn
                    .subscribe(consumer_id, consumer_name, sub_message, tx)
                    .await
                {
                    Ok(()) => match conn
                        .control_flow(consumer_id, CONSUME_CHANNEL_CAPACITY)
                        .await
                    {
                        Ok(()) => return Ok((conn, rx)),
                        Err(e) => {
                            handle_error(e, retry_opts.max_retry_count, &mut count, &mut delay)
                                .await?;
                        }
                    },
                    Err(e) => {
                        handle_error(e, retry_opts.max_retry_count, &mut count, &mut delay).await?;
                    }
                }
            }
            Err(e) => {
                handle_error(e, retry_opts.max_retry_count, &mut count, &mut delay).await?;
            }
        }
    }
}

pub async fn producer_reconnect(
    retry_opts: &Option<RetryOptions>,
    ordered: bool,
    conn_handle: &ConnectionHandle,
    name: &str,
    id: u64,
    topic: &str,
    access_mode: AccessMode,
) -> connection::Result<(Arc<Connection>, u64)> {
    let Some(retry_opts) = retry_opts else {
        return Err(connection::Error::Disconnect);
    };

    const MAX_DELAY: Duration = Duration::from_secs(30);
    let mut delay = retry_opts.min_retry_delay;
    let mut count = 0;
    loop {
        match conn_handle.get_connection(ordered).await {
            Ok(conn) => match conn.create_producer(name, id, topic, access_mode).await {
                Ok(seq_id) => return Ok((conn, seq_id)),
                Err(e) => {
                    handle_error(e, retry_opts.max_retry_count, &mut count, &mut delay).await?;
                }
            },
            Err(e) => {
                handle_error(e, retry_opts.max_retry_count, &mut count, &mut delay).await?;
            }
        }
    }
}

async fn handle_error(
    error: connection::Error,
    max_count: usize,
    count: &mut usize,
    delay: &mut Duration,
) -> connection::Result<()> {
    match error {
        e @ connection::Error::Disconnect => {
            *count += 1;
            if *count > max_count {
                return Err(e);
            }
            sleep(*delay).await;
            *delay = max(*delay * 2, Duration::from_secs(30));
            Ok(())
        }
        e => Err(e),
    }
}
