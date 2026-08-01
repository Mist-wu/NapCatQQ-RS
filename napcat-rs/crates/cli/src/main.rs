//! Command line entry for NapCatQQ-RS.

use clap::Parser;
use napcat_api::run_with_protocol;
use napcat_config::AppConfig;
use napcat_protocol::{
    OneBotBackendConfig, OneBotHttpBackend, ProtocolBackend, QQClientBackend, QQClientBackendConfig,
};
use napcat_qq_client::{MockQQClient, TcpQQClient};
use std::{process, sync::Arc};

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

    /// Protocol backend mode.
    #[arg(long)]
    protocol_mode: Option<String>,

    /// Protocol backend endpoint.
    #[arg(long)]
    protocol_endpoint: Option<String>,

    /// Protocol access token.
    #[arg(long)]
    protocol_access_token: Option<String>,
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

    if let Some(protocol_mode) = args.protocol_mode {
        config.protocol_mode = protocol_mode;
    }

    if let Some(protocol_endpoint) = args.protocol_endpoint {
        config.protocol_endpoint = protocol_endpoint;
    }

    if let Some(protocol_access_token) = args.protocol_access_token {
        config.protocol_access_token = Some(protocol_access_token);
    }

    let protocol = match config.protocol_mode.as_str() {
        "onebot" | "onebot-http" | "onebot_http" => {
            if config.protocol_endpoint.trim().is_empty() {
                return Err(String::from(
                    "protocol endpoint is required for onebot mode",
                ));
            }
            let mut backend_config = OneBotBackendConfig::new(config.protocol_endpoint.clone())
                .with_listener_settings(
                    config.protocol_listen_timeout_ms,
                    config.protocol_listen_max_events,
                );
            if let Some(token) = config.protocol_access_token.clone() {
                backend_config = backend_config.with_access_token(token);
            }
            let backend = Arc::new(OneBotHttpBackend::new(backend_config));
            backend
                .connect(&config.protocol_endpoint)
                .await
                .map_err(|error| format!("protocol connect failed: {error}"))?;
            Some(backend as Arc<dyn ProtocolBackend>)
        }
        "qq" | "qq-client" | "qq_client" => {
            if config.protocol_endpoint.trim().is_empty() {
                return Err(String::from("protocol endpoint is required for qq mode"));
            }
            let account = config
                .qq_account
                .clone()
                .ok_or_else(|| String::from("qq account is required for qq mode"))?;
            let password = config
                .qq_password
                .clone()
                .ok_or_else(|| String::from("qq password is required for qq mode"))?;

            let backend_config = QQClientBackendConfig::new(config.protocol_endpoint.clone())
                .with_credentials(account, password)
                .with_connect_timeout_ms(2_000)
                .with_listen_poll_ms(config.protocol_listen_timeout_ms);

            let client = if config.protocol_endpoint.trim_start().starts_with("tcp://") {
                Arc::new(TcpQQClient::default()) as Arc<dyn napcat_qq_client::QQClient>
            } else {
                Arc::new(MockQQClient::default()) as Arc<dyn napcat_qq_client::QQClient>
            };
            let backend = Arc::new(QQClientBackend::new(client, backend_config));
            backend
                .connect(&config.protocol_endpoint)
                .await
                .map_err(|error| format!("protocol connect failed: {error}"))?;
            Some(backend as Arc<dyn ProtocolBackend>)
        }
        _ => None,
    };

    let addr = format!("{}:{}", config.host, config.port);
    run_with_protocol(&addr, protocol)
        .await
        .map_err(|error| format!("api runtime failed: {error}"))
}
