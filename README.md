# budmq

## Features

* Server and Client are provided as crates
* Network transport layer based on QUIC protocol
  * connection reconnect
  * stream pool
  * request pipeline
* distributed deployment
  * each broker can be deployed independently
* custom storage implementation
  * `MetaStorage` for meta data
  * `MessageStorage` for topic messages

## Usage

1. generate cert files
```bash
./certs/generate_certs.sh
```
2. see examples for details:
* [server](./server/examples/server.rs)
* [consumer](./client/examples/consumer.rs)
* [producer](./client/examples/producer.rs)

## Custom storage

need to impl two [storage trait](./common/src/storage.rs):

* `MetaStorage`: used to store all brokers' meta data
* `MessageStorage`: used to store all messages received from producers and consume cursors for each subscription
