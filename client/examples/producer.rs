use bud_client::client::ClientBuilder;
use bud_common::mtls::MtlsProvider;

const CA_CERT: &[u8] = include_bytes!("../../certs/ca.pem");
const CLIENT_CERT: &[u8] = include_bytes!("../../certs/client.pem");
const CLIENT_KEY_CERT: &[u8] = include_bytes!("../../certs/client-key.pem");

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let provider = MtlsProvider::new(CA_CERT, CLIENT_CERT, CLIENT_KEY_CERT)?;
    let client = ClientBuilder::new("127.0.0.1".parse()?, provider)
        .keepalive(10000)
        .build()
        .await?;

    let mut producer = client.new_producer("test-topic");

    for _ in 0..10 {
        producer.send(b"hello, world").await?;
    }

    client.close().await?;
    Ok(())
}
