use std::{
    fs,
    io::Read,
    net::SocketAddr,
    path::{Path, PathBuf},
};

use clap::Parser;
use flexi_logger::{detailed_format, Age, Cleanup, Criterion, FileSpec, Logger, Naming};
use futures::StreamExt;
use libbud_common::mtls::MtlsProvider;
use libbud_server::Server;
use signal_hook::consts::{SIGINT, SIGQUIT, SIGTERM};
use signal_hook_tokio::Signals;
use tokio::sync::watch;

#[derive(Debug, clap::Parser)]
struct Args {
    #[arg(short, long, default_value = "../certs", env = "BUD_SERVER_CERTS_DIR")]
    cert_dir: PathBuf,
    #[arg(short, long, default_value = "0.0.0.0", env = "BUD_SERVER_ADDR")]
    addr: String,
    #[arg(short, long, default_value_t = 9080, env = "BUD_SERVER_PORT")]
    port: u16,
    #[arg(short, long, default_value = "info", env = "BUD_SERVER_LOG_LEVEL")]
    log_level: String,
}

fn main() -> anyhow::Result<()> {
    // parse command line args
    let args = Args::parse();
    println!("args: {args:?}");
    // logger init
    Logger::try_with_str(args.log_level)
        .unwrap()
        .format(detailed_format)
        .log_to_file(FileSpec::default().directory("logs"))
        .rotate(
            Criterion::AgeOrSize(Age::Day, 30 * 1024 * 1024),
            Naming::Timestamps,
            Cleanup::KeepLogAndCompressedFiles(1, 30),
        )
        .append()
        .start()
        .unwrap();

    // start server
    let ca = read_file(&args.cert_dir.join("ca.pem"))?;
    let server_cert = read_file(&args.cert_dir.join("ca_server.pem"))?;
    let server_key = read_file(&args.cert_dir.join("server-key.pem"))?;
    let provider = MtlsProvider::new(&ca, &server_cert, &server_key)?;
    let server = Server::new(provider, SocketAddr::new(args.addr.parse()?, args.port));
    run(server)?;
    Ok(())
}

fn read_file(path: &Path) -> anyhow::Result<Vec<u8>> {
    let mut buf = vec![];
    fs::File::open(path)?.read_to_end(&mut buf)?;
    Ok(buf)
}

#[tokio::main]
async fn run(server: Server) -> anyhow::Result<()> {
    let mut signals = Signals::new([SIGINT, SIGTERM, SIGQUIT])?;
    let handle = signals.handle();
    let (close_tx, close_rx) = watch::channel(());
    tokio::spawn(async move {
        signals.next().await;
        drop(close_tx);
        handle.close();
    });

    server.start(close_rx).await?;
    Ok(())
}
