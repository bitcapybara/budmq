use std::{
    fs,
    io::Read,
    path::{Path, PathBuf},
};

use bud_common::types::BrokerAddress;
use bud_server::{
    common::{mtls::MtlsProvider, storage::memory::MemoryStorage},
    Server,
};
use clap::Parser;
use flexi_logger::{colored_detailed_format, Logger};
use futures::StreamExt;
use signal_hook::consts::{SIGINT, SIGQUIT, SIGTERM};
use signal_hook_tokio::Signals;
use tokio_util::sync::CancellationToken;

#[derive(Debug, clap::Parser)]
struct Args {
    #[arg(short, long, default_value = "./certs", env = "BUD_SERVER_CERTS_DIR")]
    cert_dir: PathBuf,
    #[arg(short, long, default_value = "0.0.0.0", env = "BUD_SERVER_IP")]
    ip: String,
    #[arg(short, long, default_value = "127.0.0.1", env = "BUD_SERVER_BROKER_IP")]
    broker_ip: String,
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

    let broker_addr = BrokerAddress {
        socket_addr: format!("{}:{}", args.broker_ip, args.port).parse()?,
        server_name: "localhost".to_string(),
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

    // use memory storage
    let storage = MemoryStorage::new();
    // start server
    server.start(storage.clone(), storage).await?;
    Ok(())
}
