use bud_client::{
    client::ClientBuilder,
    consumer::{Consumer, SubscribeMessage},
};
use bud_common::{
    mtls::MtlsProvider,
    subscription::{InitialPostion, SubType},
};

const CA_CERT: &[u8] = include_bytes!("../../certs/ca.pem");
const CLIENT_CERT: &[u8] = include_bytes!("../../certs/client.pem");
const CLIENT_KEY_CERT: &[u8] = include_bytes!("../../certs/client-key.pem");

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let provider = MtlsProvider::new(CA_CERT, CLIENT_CERT, CLIENT_KEY_CERT)?;
    let mut client = ClientBuilder::new("127.0.0.1".parse()?, provider)
        .keepalive(10000)
        .build()
        .await?;

    let consumer = client
        .new_consumer(SubscribeMessage {
            topic: "test-topic".to_string(),
            sub_name: "test-subscription".to_string(),
            sub_type: SubType::Exclusive,
            initial_postion: InitialPostion::Latest,
        })
        .await?;
    if let Err(e) = consume(consumer).await {
        println!("consume error: {e}")
    }
    client.close().await?;
    Ok(())
}

async fn consume(mut consumer: Consumer) -> anyhow::Result<()> {
    while let Some(message) = consumer.next().await {
        consumer.ack(message.id).await?;
        let s = String::from_utf8(message.payload.to_vec())?;
        println!("received a message: {s}");
    }
    Ok(())
}
