use std::net::SocketAddr;

use clap::Parser;
use flexi_logger::{detailed_format, Age, Cleanup, Criterion, FileSpec, Logger, Naming};
use libbud_common::mtls::MtlsProvider;
use libbud_server::Server;

#[derive(Debug, clap::Parser)]
struct Args {
    #[arg(short, long, default_value_t = String::from("0.0.0.0"), env = "BUD_SERVER_ADDR")]
    addr: String,
    #[arg(short, long, default_value_t = 9080, env = "BUD_SERVER_PORT")]
    port: u16,
    #[arg(short, long, default_value_t = String::from("info"), env = "BUD_SERVER_LOG_LEVEL")]
    log_level: String,
}

fn main() -> anyhow::Result<()> {
    // parse command line args
    let args = Args::parse();
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
    let provider = MtlsProvider::new(todo!(), todo!(), todo!())?;
    let server = Server::new(provider, SocketAddr::new(args.addr.parse()?, args.port));

    Ok(())
}

#[tokio::main]
async fn run(server: Server) -> anyhow::Result<()> {
    Ok(())
}
