#[path = "common/lib.rs"]
mod common;

use std::path::PathBuf;

use clap::Parser;
use flexi_logger::{colored_detailed_format, Logger};
use futures::StreamExt;
use signal_hook::consts::{SIGINT, SIGQUIT, SIGTERM};
use signal_hook_tokio::Signals;
use tokio::select;

use crate::common::client_certs;
use bud_client::{client::ClientBuilder, consumer::SubscribeBuilder};

#[derive(clap::Parser)]
struct Args {
    #[arg(short, default_value = "./certs")]
    certs: PathBuf,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let args = Args::parse();
    // logger init
    Logger::try_with_str("trace, mio=off, rustls=off")
        .unwrap()
        .format(colored_detailed_format)
        .start()
        .unwrap();
    let mut client = ClientBuilder::new(
        "127.0.0.1:9080".parse()?,
        "localhost",
        client_certs(args.certs),
    )
    .keepalive(10000)
    .build()
    .await?;

    let mut consumer = client
        .new_consumer(
            "test-consumer",
            SubscribeBuilder::new("test-topic", "test-subscription").build(),
        )
        .await?;

    let mut signals = Signals::new([SIGINT, SIGTERM, SIGQUIT])?;
    let handle = signals.handle();
    loop {
        select! {
            msg = consumer.next() => {
                let Some(message) = msg else {
                    break
                };
                consumer.ack(&message.id).await?;
                let s = String::from_utf8(message.payload.to_vec())?;
                println!("received a message: {s}");
            }
            _ = signals.next() => {
                break
            }
        }
    }
    handle.close();
    consumer.close().await?;
    client.close().await?;
    Ok(())
}
