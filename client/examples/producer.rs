#[path = "common/lib.rs"]
mod common;

use std::path::PathBuf;

use bud_client::client::ClientBuilder;
use bud_common::types::AccessMode;
use clap::Parser;
use flexi_logger::{colored_detailed_format, Logger};
use log::error;

use crate::common::client_certs;

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

    let mut producer = client
        .new_producer("test-topic", "test-producer", AccessMode::Exclusive, true)
        .await?;
    // for _ in 0..10 {
    producer.send(b"hello, world").await?;
    producer
        .send_batch(["hello".as_bytes(), "world".as_bytes()])
        .await?;
    // }
    producer.close().await?;
    if let Err(e) = client.close().await {
        error!("close client error: {e}")
    }
    Ok(())
}
