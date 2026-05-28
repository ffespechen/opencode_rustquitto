# Rustquitto — MQTT 3.1.1 Broker in Rust

[![Rust](https://img.shields.io/badge/rust-1.85+-orange.svg)](https://www.rust-lang.org)
[![MQTT](https://img.shields.io/badge/mqtt-3.1.1-blue.svg)](https://mqtt.org)
[![License](https://img.shields.io/badge/license-MIT-green.svg)](LICENSE)

A high-performance, async-first MQTT 3.1.1 message broker written entirely in Rust. Built with [Tokio](https://tokio.rs) and [Axum](https://github.com/tokio-rs/axum), Rustquitto provides a lightweight, embeddable, and fast broker for IoT, messaging, and pub/sub workflows.

---

## Quick Start

```bash
docker run -d --name rustquitto -p 1883:1883 -p 9001:9001 rustquitto:latest
```

```bash
# Or with custom ports
docker run -d --name rustquitto \
  -p 1883:1883 -p 9001:9001 \
  rustquitto:latest \
  --tcp-port 1883 --ws-port 9001 --bind-addr 0.0.0.0
```

---

## Features

### QoS Levels (0, 1, 2)

| QoS | Name | Behavior |
|-----|------|----------|
| 0 | At most once | Fire and forget, no acknowledgment |
| 1 | At least once | Guaranteed delivery with PUBACK confirmation |
| 2 | Exactly once | Four-way handshake (PUBLISH → PUBREC → PUBREL → PUBCOMP) |

### Dual Transport

- **TCP** — Port `1883` for native MQTT clients (`mosquitto_pub`, Paho, embedded devices)
- **WebSocket** — Port `9001` for browser and web-based clients via Axum WS upgrade

### Topic Matching

Full support for MQTT 3.1.1 wildcards:
- `+` (single-level wildcard) — `sensor/+/temp` matches `sensor/room1/temp`
- `#` (multi-level wildcard) — `sensor/#` matches `sensor/room1/temp` and `sensor/humidity`

### Architecture Highlights

- **Async runtime** via Tokio with per-connection spawned tasks
- **Zero-copy parsing** on packet boundaries with `bytes`
- **Concurrent subscription tree** with `RwLock<dashmap>` for lock-free reads
- **Structured logging** via `tracing` (filterable with `RUST_LOG`)
- **No unsafe code** — `#![deny(unsafe_code)]` enforced

---

## Configuration

### CLI Arguments

```
Usage: rustquitto [OPTIONS]

Options:
  --tcp-port <TCP_PORT>      TCP port for MQTT connections [default: 1883]
  --ws-port <WS_PORT>        WebSocket port for MQTT-over-WS [default: 9001]
  --bind-addr <BIND_ADDR>    Bind address for all listeners [default: 0.0.0.0]
  -h, --help                 Print help
  -V, --version              Print version
```

### Environment Variables

| Variable | Default | Description |
|----------|---------|-------------|
| `RUST_LOG` | `info` | Log level (`trace`, `debug`, `info`, `warn`, `error`) |

Example with debug logging:
```bash
docker run -d --name rustquitto \
  -p 1883:1883 -p 9001:9001 \
  -e RUST_LOG=debug \
  rustquitto:latest
```

---

## Docker Compose

```yaml
version: "3.8"
services:
  mqtt-broker:
    image: rustquitto:latest
    container_name: rustquitto
    ports:
      - "1883:1883"
      - "9001:9001"
    restart: unless-stopped
    environment:
      RUST_LOG: info
```

---

## Testing with MQTT Clients

```bash
# Subscribe to a topic (run in one terminal)
mosquitto_sub -h localhost -p 1883 -t "sensor/+/temp" -v

# Publish a message (run in another terminal)
mosquitto_pub -h localhost -p 1883 -t "sensor/room1/temp" -m "22.5"
```

---

## Tech Stack

| Crate | Purpose |
|-------|---------|
| `tokio` 1 | Async runtime, TCP/networking, channels |
| `axum` 0.7 | WebSocket server on port 9001 |
| `tungstenite` 0.24 | WebSocket protocol |
| `bytes` 1 | Efficient byte buffer manipulation |
| `thiserror` 2 | Typed error definitions |
| `dashmap` 6 | Concurrent HashMap for sessions |
| `tracing` 0.1 | Structured logging |
| `clap` 4 | CLI argument parsing |

---

## Current Scope & Limitations

### Implemented
- CONNECT / CONNACK, PUBLISH (QoS 0, 1, 2), SUBSCRIBE / SUBACK
- UNSUBSCRIBE / UNSUBACK, PINGREQ / PINGRESP, DISCONNECT
- Wildcard topic matching (`+`, `#`)
- TCP and WebSocket transport (unencrypted, no auth)

### On the Roadmap
- Retained messages
- Last Will and Testament (LWT)
- TLS/SSL encryption
- Username/password authentication
- ACL-based access control
- Session persistence
- Prometheus metrics endpoint

---

## License

MIT

---

**Repository:** [GitHub](https://github.com/example/rust_mqtt)
**Image:** `docker pull <usuario>/rustquitto:latest`
