# ARCHITECTURE.md - Rust MQTT Broker

## Descripción General

Rust MQTT es un broker MQTT 3.1.1 implementado en Rust, diseñado para alto rendimiento con operaciones asíncronas. Soporta dos transportes: TCP en puerto 1883 y WebSocket en puerto 9001 mediante Axum.

## Stack Tecnológico

### Dependencias Principales

| Crate | Versión | Propósito |
|-------|---------|-----------|
| `tokio` | 1 | Runtime asíncrono, manejo de red TCP, canales mpsc |
| `axum` | 0.7 | Web framework - exclusivamente para WebSocket upgrade en puerto 9001 |
| `tokio-tungstenite` | 0.24 | Abstracción de WebSocket sobre Tokio |
| `tungstenite` | 0.24 | Protocolo WebSocket (utilizado por Axum internamente) |
| `bytes` | 1 | Buffers eficientes para serialización/deserialización |
| `thiserror` | 2 | Derivación de `std::error::Error` con mensajes descriptivos |
| `tracing` | 0.1 | Logging estructurado con spans y niveles |
| `tracing-subscriber` | 0.3 | Recolector de eventos tracing con filtro por entorno |
| `dashmap` | 6 | HashMap concurrente para estado compartido del broker |
| `clap` | 4 | Parseo de argumentos CLI con derive macros |
| `futures-util` | 0.3 | Extensiones de Stream/Sink para WebSocket |

### Dependencias de Desarrollo

| Crate | Versión | Propósito |
|-------|---------|-----------|
| `tokio-test` | 0.4 | Utilidades para testing de código asíncrono |
| `anyhow` | 1 | Manejo ergonómico de errores en tests |

## Arquitectura de Módulos

```
┌─────────────────────────────────────────────────────────────────┐
│                           main.rs                                │
│  Inicializa tracing, parsea config, arranca servidores          │
└───────────┬─────────────────────────┬───────────────────────────┘
            │                         │
     ┌──────▼──────┐          ┌──────▼──────┐
     │ server::tcp │          │ server::ws  │
     │  TCP:1883   │          │  WS:9001    │
     └──────┬──────┘          └──────┬──────┘
            │                         │
            │    ┌─────────────────┐  │
            └────►   broker::*     ◄──┘
                 │  Broker (Arc)   │
                 │  Session        │
                 │  TopicTree      │
                 └────────┬────────┘
                          │
                 ┌────────▼────────┐
                 │  protocol::*    │
                 │  encode/decode  │
                 │  Packet types   │
                 └─────────────────┘
```

### Capa de Protocolo (`src/protocol/`)

#### Tipos (`types.rs`)
Define todos los tipos del protocolo MQTT 3.1.1:
- `PacketType`: Enum con los 14 tipos de paquete MQTT
- `QoS`: Niveles de calidad de servicio (0, 1, 2)
- `PacketId`: Identificador de paquete de 2 bytes
- `FixedHeader`: Cabecera fija con tipo, flags y remaining length
- `Packet`: Enum que contiene todos los tipos de paquete
- `ConnectPacket`, `PublishPacket`, `SubscribePacket`, etc.

#### Codificación (`encode.rs`)
Serializa paquetes MQTT a bytes según el estándar:
- Variable-length encoding para remaining length
- Strings UTF-8 con prefijo de longitud (2 bytes big-endian)
- Enteros multi-byte en big-endian
- Solo codifica paquetes server-to-client (CONNACK, PUBLISH, SUBACK, etc.)

#### Decodificación (`decode.rs`)
Deserializa bytes a paquetes MQTT:
- Decodifica cabecera fija y remaining length
- Lee y decodifica payloads según tipo de paquete
- Valida UTF-8 en strings
- Maneja todos los tipos de paquete client-to-server

### Capa de Broker (`src/broker/`)

#### Broker (`mod.rs`)
Núcleo del sistema. Mantiene el estado global compartido:
- `sessions: DashMap<String, Arc<Session>>` - Registro de clientes conectados
- `subscriptions: RwLock<TopicTree>` - Árbol de suscripciones a tópicos

Métodos principales:
- `handle_connect()` - Registra nueva sesión, envía CONNACK
- `handle_publish()` - Encuentra suscriptores y entrega mensajes según QoS
- `handle_subscribe()` - Agrega suscripciones al árbol, devuelve SUBACK
- `handle_unsubscribe()` - Elimina suscripciones, envía UNSUBACK
- `handle_disconnect()` - Limpia sesión y suscripciones
- `handle_puback/rec/rel/comp()` - Gestiona máquina de estados QoS

#### Sesión (`session.rs`)
Estado por cliente conectado:
- `client_id`: Identificador único del cliente
- `clean_session`: Si la sesión se limpia al desconectar
- `keep_alive`: Intervalo de keep-alive en segundos
- `sender`: Canal mpsc para enviar paquetes al cliente
- `next_packet_id`: Contador atómico para IDs de paquete
- `pending_qos1`: Mensajes QoS 1 esperando PUBACK (retransmisión futura)
- `pending_qos2`: Mensajes QoS 2 con su estado actual en la máquina de estados

#### Árbol de Suscripciones (`subscriptions.rs`)
Estructura de datos tipo trie para matching eficiente de tópicos:
- Cada nodo contiene hijos exactos, un hijo `+` (single wildcard), y suscriptores `#` (multi wildcard)
- `add(filter, subscriber)` - Agrega suscriptor a una rama del árbol
- `matches(topic)` - Encuentra todos los suscriptores que matchean un tópico
- `remove(filter, client_id)` - Elimina suscriptor específico
- `remove_all_for_client(client_id)` - Limpia todas las suscripciones de un cliente

Ejemplo: el filtro `sensor/+/temp` crea la rama:
```
root → "sensor" → "+" → "temp" → [subscribers]
```

### Capa de Servidores (`src/server/`)

#### TCP (`tcp.rs`)
Escucha conexiones en puerto 1883 usando `tokio::net::TcpListener`:
- Por cada conexión, spawn de un task Tokio
- Buffering de bytes hasta completar paquetes MQTT (maneja lecturas parciales)
- Select loop: leer del socket y enviar respuestas desde el canal de sesión
- Decodifica el fixed header primero para determinar el tamaño total del paquete

#### WebSocket (`ws.rs`)
Escucha conexiones en puerto 9001 usando Axum:
- Route `/mqtt` que acepta upgrade a WebSocket
- Cada frame binario de WebSocket es un paquete MQTT completo
- Maneja frames Ping/Pong automáticamente
- Misma lógica de dispatch que TCP pero con API de Stream/Sink

### Módulo de Configuración (`config.rs`)
Usa `clap` con derive macros para argumentos de línea de comandos:
- `--tcp-port` (default: 1883)
- `--ws-port` (default: 9001)
- `--bind-addr` (default: 0.0.0.0)

### Manejo de Errores (`error.rs`)
Usa `thiserror` para definir una jerarquía de errores tipada:
- `BrokerError::Io` - Errores de I/O
- `BrokerError::Protocol` - Violaciones de protocolo
- `BrokerError::Decode/Encode` - Errores de serialización
- `BrokerError::Config` - Errores de configuración
- `Result<T>` alias para `std::result::Result<T, BrokerError>`

## Flujo de Datos

### Conexión de Cliente TCP

```
1. TcpListener::accept() → nuevo TcpStream
2. Cliente envía CONNECT → decode_connect()
3. Broker::handle_connect() → crea Session, almacena en DashMap
4. Envía CONNACK al cliente
5. Loop principal: leer bytes → extraer paquetes → dispatch
```

### Publicación QoS 2 (Flujo Completo)

```
Publisher                        Broker                        Subscriber
   │                               │                               │
   │──── PUBLISH(QoS2, PktId)─────►│                               │
   │                               │── PUBLISH(QoS2, PktId)───────►│
   │                               │◄────── PUBREC(PktId) ────────│
   │◄────── PUBREC(PktId) ────────│                               │
   │                               │                               │
   │────── PUBREL(PktId) ────────►│                               │
   │                               │────── PUBREL(PktId) ────────►│
   │                               │◄────── PUBCOMP(PktId) ───────│
   │◄────── PUBCOMP(PktId) ───────│                               │
   │                               │                               │
```

### Matching de Tópicos

```
Subscripción: "building/+/temperature"
Publicación:  "building/floor1/temperature" → MATCH ✓
Publicación:  "building/floor1/humidity"    → NO MATCH
Publicación:  "sensor/temp"                 → NO MATCH

Subscripción: "sensor/#"
Publicación:  "sensor/temp"                 → MATCH ✓
Publicación:  "sensor/room1/temp"           → MATCH ✓
Publicación:  "other/topic"                 → NO MATCH
```

## Concurrencia y Estado Compartido

| Componente | Mecanismo | Justificación |
|------------|-----------|---------------|
| Sesiones | `DashMap<String, Arc<Session>>` | Lectura concurrente sin bloqueo, escritura thread-safe |
| Suscripciones | `Arc<RwLock<TopicTree>>` | Múltiples lectores concurrentes, escritura exclusiva |
| Canal de salida | `mpsc::UnboundedChannel` | Entrega de paquetes sin backpressure |
| Packet ID | `AtomicU16` | Incremento atómico sin locks |
| Estado QoS | `Mutex<HashMap<PacketId, _>>` | Mutación por sesión, sin contención entre sesiones |

## Alcances y Limitaciones

### Implementado
- [x] CONNECT / CONNACK
- [x] PUBLISH / PUBACK / PUBREC / PUBREL / PUBCOMP (QoS 0, 1, 2)
- [x] SUBSCRIBE / SUBACK
- [x] UNSUBSCRIBE / UNSUBACK
- [x] PINGREQ / PINGRESP
- [x] DISCONNECT
- [x] Matching de tópicos con wildcards `+` y `#`
- [x] TCP sin encriptación (puerto 1883)
- [x] WebSocket sin encriptación (puerto 9001)
- [x] Sin autenticación (conexiones abiertas)
- [x] Métricas de logging con tracing

### Pendiente / Futuras Mejoras
- [ ] Retained Messages (mensajes retenidos por tópico)
- [ ] Last Will and Testament (mensajes de última voluntad)
- [ ] Keep-alive timeout (desconexión por inactividad)
- [ ] Retransmisión de mensajes QoS 1/2 no confirmados
- [ ] TLS/SSL (encriptación de transporte)
- [ ] Autenticación (username/password)
- [ ] Control de acceso (ACL por tópico)
- [ ] Persistencia de sesiones
- [ ] Puente MQTT (bridge a otros brokers)
- [ ] Métricas HTTP (endpoint /metrics con Prometheus)

## Convenciones de Código

Basado en [Apollo GraphQL Rust Best Practices](https://github.com/apollographql/rust-best-practices):

- **Manejo de errores**: `thiserror` para errores tipados, `?` para propagación, sin `unwrap()`/`expect()` en producción
- **Borrowing**: Uso de referencias (`&[u8]`, `&str`) en parámetros; clonación solo cuando es necesario
- **Copy types**: Tipos pequeños (`QoS`, `PacketId`, `PacketType`) implementan `Copy`
- **Concurrencia**: `Arc` para estado compartido, `DashMap` para mapas concurrentes
- **Linting**: Clippy con `-D warnings`, tests con `#[expect(clippy::unwrap_used)]`
- **Nombrado**: Tests descriptivos con formato `unit_estado_comportamiento`
