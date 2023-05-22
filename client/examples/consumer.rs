use std::{fs, io::Read, path::Path};

use bud_client::{
    client::ClientBuilder,
    consumer::{Consumer, SubscribeMessage},
};
use bud_common::{
    mtls::MtlsProvider,
    subscription::{InitialPostion, SubType},
};
use flexi_logger::{colored_detailed_format, Logger};

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    // logger init
    Logger::try_with_str("trace")
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
    Ok(())
}

async fn consume(mut consumer: Consumer) -> anyhow::Result<()> {
    while let Some(message) = consumer.next().await {
        consumer.ack(message.id).await?;
        let s = String::from_utf8(message.payload.to_vec())?;
        println!("received a message: {s}");
    }
    consumer.close().await?;
    Ok(())
}

fn read_file(path: impl AsRef<Path>) -> anyhow::Result<Vec<u8>> {
    let mut buf = vec![];
    fs::File::open(path)?.read_to_end(&mut buf)?;
    Ok(buf)
}
