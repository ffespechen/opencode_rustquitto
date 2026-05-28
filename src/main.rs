mod broker;
mod config;
mod error;
mod protocol;
mod server;

use std::sync::Arc;

use clap::Parser;
use tracing::info;
use tracing_subscriber::EnvFilter;

use crate::broker::Broker;
use crate::config::Config;
use crate::error::Result;

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| EnvFilter::new("info")),
        )
        .init();

    let config = Config::parse();

    info!(
        "Starting MQTT broker on {} (TCP: {}, WS: {})",
        config.bind_addr, config.tcp_port, config.ws_port
    );

    let broker = Broker::new();

    let tcp_broker = Arc::clone(&broker);
    let tcp_config = config.clone();
    let tcp_handle = tokio::spawn(async move {
        if let Err(e) = server::tcp::start(&tcp_config, tcp_broker).await {
            tracing::error!("TCP server error: {e}");
        }
    });

    let ws_broker = Arc::clone(&broker);
    let ws_config = config.clone();
    let ws_handle = tokio::spawn(async move {
        if let Err(e) = server::ws::start(&ws_config, ws_broker).await {
            tracing::error!("WebSocket server error: {e}");
        }
    });

    tokio::select! {
        res = tcp_handle => {
            if let Err(e) = res {
                tracing::error!("TCP server panicked: {e}");
            }
        }
        res = ws_handle => {
            if let Err(e) = res {
                tracing::error!("WS server panicked: {e}");
            }
        }
    }

    Ok(())
}
