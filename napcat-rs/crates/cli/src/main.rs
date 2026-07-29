//! Command line entry for NapCatQQ-RS.

use clap::Parser;

#[derive(Debug, Parser)]
#[command(name = "napcat-rs", version = "0.1.0")]
struct Args {
    /// Enable debug logging output.
    #[arg(long)]
    debug: bool,
}

/// Main CLI entrypoint.
fn main() {
    let _args = Args::parse();
}
