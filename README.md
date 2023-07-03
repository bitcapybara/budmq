# budmq

## Features

* Server and Client are provided as crates
* Network transport layer based on QUIC protocol: connection reconnect and stream pool supported
* distributed deployment: each broker can be deployed independently
* custom storage implementation: `MetaStorage` for meta data, `MessageStorage` for topic messages

## Usage

### Server

```rust
let ca = read_file(&args.cert_dir.join("ca-cert.pem"))?;
let server_cert = read_file(&args.cert_dir.join("server-cert.pem"))?;
let server_key = read_file(&args.cert_dir.join("server-key.pem"))?;
let provider = MtlsProvider::new(&ca, &server_cert, &server_key)?;

// used to save in storage, client can use this addr to connect broker
let broker_addr = BrokerAddress {
    socket_addr: format!("{}:{}", args.broker_ip, args.port).parse()?,
    server_name: "localhost".to_string(),
};
// use to listen on an addr
let addr = format!("{}:{}", args.ip, args.port).parse()?;
// create server
let (token, server) = Server::new(provider, &addr, &broker_addr);
// use memory storage
let storage = MemoryStorage::new();
// start server
server.start(storage.clone(), storage).await?;
```

### Consumer

```rust
let ca_cert = read_file("./certs/ca-cert.pem")?;
let client_cert = read_file("./certs/client-cert.pem")?;
let client_key_cert = read_file("./certs/client-key.pem")?;
let provider = MtlsProvider::new(&ca_cert, &client_cert, &client_key_cert)?;
// create client
let mut client = ClientBuilder::new("127.0.0.1:9080".parse()?, "localhost", provider)
    .keepalive(10000)
    .build()
    .await?;
// create consumer
let mut consumer = client
    .new_consumer(
        "test-consumer",
        SubscribeMessage {
            topic: "test-topic".to_string(),
            sub_name: "test-subscription".to_string(),
            sub_type: SubType::Exclusive,
            initial_postion: InitialPostion::Latest,
        },
    )
    .await?;
// consumer waiting for messages
while let Some(message) = consumer.next().await {
    consumer.ack(&message.id).await?;
    let s = String::from_utf8(message.payload.to_vec())?;
    println!("received a message: {s}");
}
// close consumer
consumer.close().await?;
// close client
client.close().await?;
```

### Producer

```rust
let ca_cert = read_file("./certs/ca-cert.pem")?;
let client_cert = read_file("./certs/client-cert.pem")?;
let client_key_cert = read_file("./certs/client-key.pem")?;
let provider = MtlsProvider::new(&ca_cert, &client_cert, &client_key_cert)?;
// create client
let mut client = ClientBuilder::new("127.0.0.1:9080".parse()?, "localhost", provider)
    .keepalive(10000)
    .build()
    .await?;
// create producer
let mut producer = client
    .new_producer("test-topic", "test-producer", AccessMode::Exclusive, true)
    .await?;
// producer publish message
for _ in 0..10 {
    producer.send(b"hello, world").await?;
}
// close producer
producer.close().await?;
// close client
client.close().await?;
```

## Custom storage

### MetaStorage (common/src/storage.rs)

```rust
#[async_trait]
pub trait MetaStorage: Clone + Send + Sync + 'static {
    type Error: std::error::Error;

    async fn register_topic(
        &self,
        topic_name: &str,
        broker_addr: &BrokerAddress,
    ) -> Result<(), Self::Error>;

    async fn get_topic_owner(&self, topic_name: &str)
        -> Result<Option<BrokerAddress>, Self::Error>;

    async fn add_subscription(&self, info: &SubscriptionInfo) -> Result<(), Self::Error>;

    async fn all_subscription(
        &self,
        topic_name: &str,
    ) -> Result<Vec<SubscriptionInfo>, Self::Error>;

    async fn del_subscription(&self, topic_name: &str, name: &str) -> Result<(), Self::Error>;

    async fn get_u64(&self, k: &str) -> Result<Option<u64>, Self::Error>;

    async fn put_u64(&self, k: &str, v: u64) -> Result<(), Self::Error>;

    async fn inc_u64(&self, k: &str, v: u64) -> Result<u64, Self::Error>;
}
```

### MessageStorage (common/src/storage.rs)

```rust
#[async_trait]
pub trait MessageStorage: Clone + Send + Sync + 'static {
    type Error: std::error::Error;

    async fn put_message(&self, msg: &TopicMessage) -> Result<(), Self::Error>;

    async fn get_message(&self, id: &MessageId) -> Result<Option<TopicMessage>, Self::Error>;

    async fn del_message(&self, id: &MessageId) -> Result<(), Self::Error>;

    async fn save_cursor(
        &self,
        topic_name: &str,
        sub_name: &str,
        bytes: &[u8],
    ) -> Result<(), Self::Error>;

    async fn load_cursor(
        &self,
        topic_name: &str,
        sub_name: &str,
    ) -> Result<Option<Vec<u8>>, Self::Error>;
}
```
