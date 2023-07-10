use std::{
    fs,
    io::Read,
    path::{Path, PathBuf},
};

use bud_common::{
    storage::{mongodb::MongoDB, redis::Redis},
    types::BrokerAddress,
};
use bud_server::{common::mtls::MtlsProvider, Server};
use clap::Parser;
use flexi_logger::{colored_detailed_format, Logger};
use futures::StreamExt;
use signal_hook::consts::{SIGINT, SIGQUIT, SIGTERM};
use signal_hook_tokio::Signals;
use tokio_util::sync::CancellationToken;
use url::Url;

#[derive(Debug, clap::Parser)]
struct Args {
    #[arg(short, long, default_value = "./certs", env = "BUD_SERVER_CERTS_DIR")]
    cert_dir: PathBuf,
    #[arg(short, long, default_value = "0.0.0.0", env = "BUD_SERVER_IP")]
    ip: String,
    #[arg(
        short,
        long,
        default_value = "bud://localhost:9080",
        env = "BUD_SERVER_NAME"
    )]
    url: String,
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
    let ca = read_file(&args.cert_dir.join("ca-cert.pem"))?;
    let server_cert = read_file(&args.cert_dir.join("server-cert.pem"))?;
    let server_key = read_file(&args.cert_dir.join("server-key.pem"))?;
    let provider = MtlsProvider::new(&ca, &server_cert, &server_key)?;

    let server_url = Url::parse(&args.url)?;
    let broker_addr = BrokerAddress {
        socket_addr: server_url.socket_addrs(|| Some(args.port))?[0],
        server_name: server_url.domain().unwrap().to_string(),
    };
    let addr = format!("{}:{}", args.ip, args.port).parse()?;
    let (token, server) = Server::new(provider, &addr, &broker_addr);
    run(token, server)?;
    Ok(())
}

fn read_file(path: &Path) -> anyhow::Result<Vec<u8>> {
    let mut buf = vec![];
    fs::File::open(path)?.read_to_end(&mut buf)?;
    Ok(buf)
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

    // => use memory storage
    // let storage = MemoryStorage::new();
    // => use bonsaidb storage
    // let storage = BonsaiDB::new("/tmp").await?;
    // => use redis storage
    let redis_client = redis::Client::open("redis://127.0.0.1:6379")?;
    let meta = Redis::new(redis_client);
    // => use mongo storage
    let mongo_client = mongodb::Client::with_uri_str("mongodb://127.0.0.1:27017").await?;
    let mongo_database = mongo_client.database("budmq");
    let message = MongoDB::new(mongo_database);
    // start server
    server.start(meta, message).await?;
    Ok(())
}
