# Rust MQTT Broker

MQTT 3.1.1 broker de alto rendimiento implementado en Rust, usando Tokio, Axum y bibliotecas del ecosistema async de Rust.

## Características

- **MQTT 3.1.1** - Implementación completa del estándar OASIS MQTT 3.1.1
- **QoS 0, 1 y 2** - Soporte para los tres niveles de calidad de servicio:
  - **QoS 0** (At Most Once) - Entrega sin confirmación
  - **QoS 1** (At Least Once) - Entrega con confirmación PUBACK
  - **QoS 2** (Exactly Once) - Protocolo de 4 vías (PUBLISH → PUBREC → PUBREL → PUBCOMP)
- **Doble transporte**:
  - **TCP sin encriptación** en puerto `1883` para clientes MQTT nativos
  - **WebSocket sin encriptación** en puerto `9001` para clientes web (navegadores, PWA)
- **Matching de tópicos** con soporte para wildcards `+` (single-level) y `#` (multi-level)
- **Arquitectura async** basada en Tokio para máxima concurrencia
- **Logging estructurado** con `tracing`
- **Cero dependencias de unsafe code**

## Requisitos

- Rust 1.85+ (edición 2021)
- Cargo (incluido con Rust)

## Instalación

```bash
# Clonar el repositorio
git clone <repo-url>
cd rust_mqtt

# Compilar en modo release
cargo build --release
```

## Uso

```bash
# Iniciar con valores por defecto (TCP:1883, WS:9001)
cargo run --release

# Personalizar puertos y dirección de bind
cargo run --release -- \
  --tcp-port 1883 \
  --ws-port 9001 \
  --bind-addr 0.0.0.0
```

### Opciones CLI

| Opción | Default | Descripción |
|--------|---------|-------------|
| `--tcp-port` | `1883` | Puerto para conexiones MQTT TCP |
| `--ws-port` | `9001` | Puerto para conexiones WebSocket |
| `--bind-addr` | `0.0.0.0` | Dirección de escucha |

### Logging

Controlar nivel de logs con variable de entorno `RUST_LOG`:

```bash
RUST_LOG=debug cargo run --release
RUST_LOG=rust_mqtt=trace cargo run --release
```

## Docker

### Construir imagen

```bash
docker build -t rustquitto:latest .
```

### Ejecutar con Docker

```bash
docker run -d --name rustquitto -p 1883:1883 -p 9001:9001 rustquitto:latest
```

### Docker Compose

```bash
docker-compose up -d
docker-compose logs -f
docker-compose down
```

### Publicar en Docker Hub

```bash
docker tag rustquitto:latest <usuario>/rustquitto:latest
docker push <usuario>/rustquitto:latest
```

## Testing

### Pruebas unitarias

```bash
cargo test
cargo test -- --nocapture   # Ver output de los tests
```

### Linting

```bash
cargo clippy --all-targets --all-features --locked -- -D warnings
```

### Conectar un cliente MQTT

```bash
# Usando mosquitto_pub (requiere mosquitto-clients)
mosquitto_pub -h localhost -p 1883 -t "test/topic" -m "Hello, MQTT!"

# Usando mosquitto_sub en otra terminal
mosquitto_sub -h localhost -p 1883 -t "test/topic"
```

## Estructura del proyecto

```
src/
├── main.rs            # Entry point: inicialización y arranque de servidores
├── config.rs          # Configuración CLI con Clap
├── error.rs           # Tipos de error con thiserror
├── broker/
│   ├── mod.rs         # Broker central: manejo de mensajes y enrutamiento
│   ├── session.rs     # Gestión de sesiones por cliente
│   └── subscriptions.rs  # Árbol de tópicos con wildcards
├── protocol/
│   ├── mod.rs         # Re-exports
│   ├── types.rs       # Tipos del protocolo MQTT (paquetes, QoS, etc.)
│   ├── encode.rs      # Serialización de paquetes a bytes
│   └── decode.rs      # Deserialización de bytes a paquetes
└── server/
    ├── mod.rs         # Módulo de servidores
    ├── tcp.rs         # Servidor TCP en puerto 1883
    └── ws.rs          # Servidor WebSocket/Axum en puerto 9001
```

## Licencia

MIT
