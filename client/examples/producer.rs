use core::time;
use std::{fs, io::Read, path::Path};

use bud_client::{client::ClientBuilder, producer::Producer};
use bud_common::mtls::MtlsProvider;
use flexi_logger::{detailed_format, Logger};
use tokio::time::sleep;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    // logger init
    Logger::try_with_str("trace")
        .unwrap()
        .format(detailed_format)
        .start()
        .unwrap();
    let ca_cert = read_file("./certs/ca-cert.pem")?;
    let client_cert = read_file("./certs/client-cert.pem")?;
    let client_key_cert = read_file("./certs/client-key.pem")?;
    let provider = MtlsProvider::new(&ca_cert, &client_cert, &client_key_cert)?;
    let client = ClientBuilder::new("127.0.0.1:9080".parse()?, provider)
        .keepalive(10000)
        .build()
        .await?;

    sleep(time::Duration::from_millis(200)).await;
    let producer = client.new_producer("test-topic");
    if let Err(e) = produce(producer).await {
        println!("produce error: {e}")
    }
    client.close().await?;
    Ok(())
}

async fn produce(mut producer: Producer) -> anyhow::Result<()> {
    for _ in 0..10 {
        producer.send(b"hello, world").await?;
    }
    Ok(())
}

fn read_file(path: impl AsRef<Path>) -> anyhow::Result<Vec<u8>> {
    let mut buf = vec![];
    fs::File::open(path)?.read_to_end(&mut buf)?;
    Ok(buf)
}
