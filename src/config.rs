use clap::Parser;

#[derive(Debug, Clone, Parser)]
#[command(name = "rust_mqtt", version, about = "MQTT 3.1.1 broker written in Rust")]
pub struct Config {
    #[arg(long, default_value = "1883", help = "TCP port for MQTT connections")]
    pub tcp_port: u16,

    #[arg(long, default_value = "9001", help = "WebSocket port for MQTT-over-WS connections")]
    pub ws_port: u16,

    #[arg(long, default_value = "0.0.0.0", help = "Bind address for all listeners")]
    pub bind_addr: String,
}
