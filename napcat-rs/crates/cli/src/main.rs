//! Command line entry for NapCatQQ-RS.

use clap::Parser;
use napcat_api::run;
use napcat_config::AppConfig;
use std::process;

#[derive(Debug, Parser)]
#[command(name = "napcat-rs", version = "0.1.0", about = "NapCatQQ-RS bootstrap")]
struct Args {
    /// Override host from configuration.
    #[arg(long)]
    host: Option<String>,

    /// Override port from configuration.
    #[arg(long)]
    port: Option<u16>,

    /// Enable debug logging level.
    #[arg(long)]
    debug: bool,
}

#[tokio::main]
async fn main() {
    if let Err(error) = run_app(Args::parse()).await {
        eprintln!("failed to start napcat-rs: {error}");
        process::exit(1);
    }
}

async fn run_app(args: Args) -> Result<(), String> {
    let mut config = AppConfig::load().map_err(|error| format!("load config failed: {error}"))?;

    if let Some(host) = args.host {
        config.host = host;
    }

    if let Some(port) = args.port {
        config.port = port;
    }

    if args.debug {
        config.log_level = String::from("debug");
    }

    let addr = format!("{}:{}", config.host, config.port);
    run(&addr)
        .await
        .map_err(|error| format!("api runtime failed: {error}"))
}
