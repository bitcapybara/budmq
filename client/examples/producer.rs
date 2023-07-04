use std::{fs, io::Read, path::Path};

use bud_client::client::ClientBuilder;
use bud_common::{mtls::MtlsProvider, types::AccessMode};
use flexi_logger::{colored_detailed_format, Logger};
use log::error;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    // logger init
    Logger::try_with_str("trace, mio=off, rustls=off")
        .unwrap()
        .format(colored_detailed_format)
        .start()
        .unwrap();
    let ca_cert = read_file("./certs/ca-cert.pem")?;
    let client_cert = read_file("./certs/client-cert.pem")?;
    let client_key_cert = read_file("./certs/client-key.pem")?;
    let provider = MtlsProvider::new(&ca_cert, &client_cert, &client_key_cert)?;
    let mut client = ClientBuilder::new("127.0.0.1:9080".parse()?, "localhost", provider)
        .keepalive(10000)
        .build()
        .await?;

    let mut producer = client
        .new_producer("test-topic", "test-producer", AccessMode::Exclusive, true)
        .await?;
    // for _ in 0..10 {
    producer.send(b"hello, world").await?;
    producer
        .send_batch(vec!["hello".as_bytes(), "world".as_bytes()])
        .await?;
    // }
    producer.close().await?;
    if let Err(e) = client.close().await {
        error!("close client error: {e}")
    }
    Ok(())
}

fn read_file(path: impl AsRef<Path>) -> anyhow::Result<Vec<u8>> {
    let mut buf = vec![];
    fs::File::open(path)?.read_to_end(&mut buf)?;
    Ok(buf)
}
