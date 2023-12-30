use std::path::PathBuf;

#[cfg(all(feature = "bonsaidb", not(any(feature = "redis", feature = "mongodb"))))]
use bud_common::storage::bonsaidb::BonsaiDB;
#[cfg(all(
    not(feature = "redis"),
    not(feature = "mongodb"),
    not(feature = "bonsaidb")
))]
use bud_common::storage::memory::MemoryDB;
#[cfg(feature = "mongodb")]
use bud_common::storage::mongodb::MongoDB;
#[cfg(feature = "redis")]
use bud_common::storage::redis::Redis;

use bud_common::mtls::Certificate;
use bud_common::types::BrokerAddress;
use bud_server::Server;
use clap::Parser;
use flexi_logger::{colored_detailed_format, Logger};
use futures::StreamExt;
use signal_hook::consts::{SIGINT, SIGQUIT, SIGTERM};
use signal_hook_tokio::Signals;
use tokio_util::sync::CancellationToken;
use url::Url;

#[derive(Debug, clap::Parser)]
struct Args {
    /// tls 证书目录
    #[arg(short, long, default_value = "./certs", env = "BUD_SERVER_CERTS_DIR")]
    cert_dir: PathBuf,
    /// 用于客户端访问的节点地址
    #[arg(
        short,
        long,
        default_value = "bud://localhost:9080",
        env = "BUD_SERVER_NAME"
    )]
    url: String,
    /// 节点监听的 ip 地址
    #[arg(short, long, default_value = "0.0.0.0", env = "BUD_SERVER_IP")]
    ip: String,
    /// 节点监听的端口
    #[arg(short, long, default_value_t = 9080, env = "BUD_SERVER_PORT")]
    port: u16,
}

fn main() -> anyhow::Result<()> {
    // parse command line args
    let args = Args::parse();
    println!("args: {args:?}");
    // logger init
    Logger::try_with_str("trace, mio=off, rustls=off")
        .unwrap()
        .format(colored_detailed_format)
        .start()
        .unwrap();

    // start server
    let server_url = Url::parse(&args.url)?;
    let broker_addr = BrokerAddress {
        socket_addr: server_url.socket_addrs(|| Some(args.port))?[0],
        server_name: server_url.domain().unwrap().to_string(),
    };
    let addr = format!("{}:{}", args.ip, args.port).parse()?;
    let (token, server) = Server::new(
        bud_common::mtls::Certs {
            ca_cert: args.cert_dir.join("ca-cert.pem"),
            endpoint_cert: bud_common::mtls::EndpointCerts::Server(Certificate {
                certificate: args.cert_dir.join("server-cert.pem"),
                private_key: args.cert_dir.join("server-key.pem"),
            }),
        },
        &addr,
        &broker_addr,
    );
    run(token, server)?;
    Ok(())
}

#[tokio::main]
async fn run(token: CancellationToken, server: Server) -> anyhow::Result<()> {
    let mut signals = Signals::new([SIGINT, SIGTERM, SIGQUIT])?;
    let handle = signals.handle();
    tokio::spawn(async move {
        signals.next().await;
        token.cancel();
        handle.close();
    });
    let (meta, message) = storage().await?;
    // start server
    server.start(meta, message).await?;
    Ok(())
}

#[cfg(all(feature = "redis", feature = "mongodb"))]
async fn storage() -> anyhow::Result<(Redis, MongoDB)> {
    log::trace!("redis and mongodb storage loaded");
    let redis_client = redis::Client::open("redis://127.0.0.1:6379")?;
    let meta = Redis::new(redis_client);
    let mongo_client = mongodb::Client::with_uri_str("mongodb://127.0.0.1:27017").await?;
    let mongo_database = mongo_client.database("budmq");
    let message = MongoDB::new(mongo_database);
    Ok((meta, message))
}

#[cfg(all(feature = "bonsaidb", not(any(feature = "redis", feature = "mongodb"))))]
async fn storage() -> anyhow::Result<(BonsaiDB, BonsaiDB)> {
    log::trace!("bonsaidb storage loaded");
    let meta = BonsaiDB::new("/tmp").await?;
    let message = meta.clone();
    Ok((meta, message))
}

#[cfg(all(
    not(feature = "redis"),
    not(feature = "mongodb"),
    not(feature = "bonsaidb")
))]
async fn storage() -> anyhow::Result<(MemoryDB, MemoryDB)> {
    log::trace!("memorydb storage loaded");
    let meta = MemoryDB::new();
    let message = meta.clone();
    Ok((meta, message))
}
