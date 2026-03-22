# Iris

**Iris — Programmable CDC pipelines with WASM**

Iris is a lightweight Change Data Capture (CDC) pipeline that reads database change events, transforms them using WebAssembly, and streams to message systems.

```
Postgres → Iris → WASM Transform → Redis/Kafka/NATS
```

## Features

- **Lightweight CDC engine** - Replaces heavy stacks like Debezium + Kafka Connect for small use cases
- **WASM transforms** - Custom event filtering and transformation via WebAssembly (Rust, TinyGo)
- **Transform chaining** - Apply multiple WASM modules sequentially
- **Redis List and Stream sinks** - Push events to Redis Lists (LPUSH) or Streams (XADD) with automatic trimming
- **Kafka sink** - Stream events to Apache Kafka topics
- **NATS JetStream sink** - Publish events to NATS JetStream subjects
- **Table-to-stream mapping** - Route table changes to dedicated Redis Stream keys, Kafka topics, or NATS subjects
- **Dead letter queue** - Failed events routed to a DLQ sink after retry exhaustion
- **Retry with backoff** - Configurable retry attempts and delay for transform/sink operations
- **Prometheus metrics** - Events/sec, replication lag, transform/sink latency histograms, error counters
- **OpenTelemetry tracing** - Distributed traces through the pipeline (OTLP/gRPC)
- **Health endpoints** - Kubernetes-ready `/healthz` and `/readyz` probes
- **Structured logging** - Configurable log level (debug, info, warn, error) and format (text, json)
- **Single binary deployment** - No external dependencies required

## Quick Start

### Installation

```bash
go build -o bin/iris ./cmd/iris
```

### Configuration

Create a `config.yaml` file:

```yaml
source:
  type: postgres
  dsn: postgres://user:password@localhost:5432/mydb
  tables:
    - users
    - orders
  slot_name: iris_slot

transform:
  enabled: false
  type: wasm
  # Option 1: Single WASM module (backward compatible)
  path: ./transforms/filter.wasm

  # Option 2: Multiple WASM modules (applied sequentially)
  # modules:
  #   - path: ./transforms/filter.wasm
  #   - path: ./transforms/enrich.wasm

# Option 1: Redis List sink
sink:
  type: redis
  addr: localhost:6379
  key: cdc:events
  max_len: 10000

# Option 2: Redis Stream sink
# sink:
#   type: redis_stream
#   addr: localhost:6379
#   max_len: 10000
# mapping:
#   table_stream_map:
#     users: cdc:users
#     orders: cdc:orders

# Option 3: Kafka sink
# sink:
#   type: kafka
#   brokers:
#     - localhost:9092
#   # Optional: map tables to topics
# mapping:
#   table_stream_map:
#     users: users-topic
#     orders: orders-topic

# Option 4: NATS JetStream sink
# sink:
#   type: nats
#   url: nats://localhost:4222
# mapping:
#   table_stream_map:
#     users: app.users
#     orders: app.orders

# Dead letter queue (optional)
dlq:
  enabled: true
  sink:
    type: redis_stream
    addr: localhost:6379
    key: cdc:dlq
    max_len: 10000

# Retry configuration (optional)
retry:
  max_attempts: 3
  backoff_ms: 100

# Logger configuration (optional)
logger:
  level: info  # debug, info, warn, error
  format: text # text, json

# Observability (optional)
observability:
  metrics:
    enabled: true
    port: 9090
  tracing:
    enabled: false
    endpoint: localhost:4317
    service_name: iris
    sample_rate: 1.0
```

### Run Pipeline

```bash
./bin/iris -config config.yaml
```

## Architecture

```
┌─────────────┐
│  Datasource │
│  PostgreSQL  │
└──────┬──────┘
       │  Logical Replication (pgoutput)
       v
┌─────────────┐
│  Iris CDC   │
│  Connector  │
└──────┬──────┘
       │
       v
┌─────────────┐
│ WASM Engine │  (optional transform/filter)
└──────┬──────┘
       │
       v
┌─────────────┐
│ Stream Sink │
│ Redis List  │  LPUSH + LTRIM
│ Redis Stream│  XADD + XTRIM
│ Kafka       │  Produce to topic
│ NATS JS     │  Publish to subject
└─────────────┘
```

## Event Format

Iris normalizes CDC events into a unified format:

```json
{
  "source": "postgres",
  "table": "users",
  "op": "insert",
  "ts": 1710000000,
  "before": null,
  "after": {
    "id": 1,
    "name": "alice"
  }
}
```

## WASM Transform

Write custom transform/filter logic in Rust or Go (TinyGo), compiled to WebAssembly.

### ABI Contract

Modules export two functions:

| Function | Signature | Description |
|----------|-----------|-------------|
| `alloc`  | `(size: u32) -> u32` | Allocate memory for input |
| `handle` | `(ptr: u32, len: u32) -> u32` | Process event, return pointer to result |

The result is a 16-byte struct: `[data_ptr:u32][data_len:u32][err_ptr:u32][err_len:u32]`
- Return `data_len=0` to drop (filter out) the event
- Return `err_len>0` to signal an error

### Example: Table Filter (Rust)

```rust
#[no_mangle]
pub extern "C" fn handle(ptr: u32, len: u32) -> u32 {
    let input = unsafe { std::slice::from_raw_parts(ptr as *const u8, len as usize) };
    let event: CdcEvent = serde_json::from_slice(input).unwrap();

    // Only allow users and orders tables
    if event.table != "users" && event.table != "orders" {
        return write_drop(); // data_len=0
    }
    write_data(input) // passthrough
}
```

### Example: Table Filter (TinyGo)

```go
//export handle
func handle(ptr, size uint32) uint32 {
    input := unsafe.Slice((*byte)(unsafe.Pointer(uintptr(ptr))), size)
    var event cdcEvent
    json.Unmarshal(input, &event)

    if !allowedTables[event.Table] {
        return writeDrop() // data_len=0
    }
    return writeData(input) // passthrough
}
```

### Building WASM Modules

```bash
# Rust
cargo build --target wasm32-unknown-unknown --release

# TinyGo (requires Go <=1.25)
tinygo build -o filter.wasm -target=wasi -no-debug .

# Or use the Makefile (Rust only)
make build-wasm
```

See `examples/transforms/` for complete working examples.

## Makefile Commands

| Command                            | Description                                        |
| ---------------------------------- | -------------------------------------------------- |
| `make build`                       | Build the binary                                   |
| `make test`                        | Run all tests                                      |
| `make test-unit`                   | Run unit tests only                                |
| `make test-integration`            | Run integration tests (requires local PG + Redis)  |
| `make test-integration-containers` | Run integration tests with testcontainers (Docker) |
| `make test-coverage`               | Run tests with coverage report                     |
| `make test-race`                   | Run tests with race detector                       |
| `make lint`                        | Run linter                                         |
| `make fmt` / `make format`         | Format code                                        |
| `make build-wasm`                  | Build example WASM modules (requires Rust)         |
| `make clean`                       | Clean build artifacts                              |

## Project Structure

```
iris/
├── cmd/iris/              # CLI entrypoint (urfave/cli)
├── pkg/
│   ├── cdc/               # Core CDC interfaces and types
│   ├── config/            # Configuration loading
│   ├── logger/            # Structured logger (slog)
│   └── observability/     # Prometheus metrics, OTel tracing, health endpoints
├── internal/
│   ├── source/postgres/   # PostgreSQL CDC connector (pglogrepl)
│   ├── transform/
│   │   ├── wasm/          # WASM runtime (wazero)
│   │   ├── nop/           # No-op passthrough transform
│   │   └── chain/         # Transform chain (multiple WASM modules)
│   ├── sink/
│   │   ├── factory.go     # Registry-based sink factory
│   │   ├── redis/         # Redis List + Stream sinks
│   │   ├── kafka/         # Kafka sink (franz-go)
│   │   └── nats/          # NATS JetStream sink (nats.go)
│   ├── dlq/               # Dead letter queue
│   ├── offset/
│   │   └── file/          # File-based offset store
│   └── pipeline/          # Pipeline orchestration
├── examples/transforms/   # Example WASM modules (Rust + TinyGo)
├── tests/
│   └── e2e/               # End-to-end tests
├── docs/                  # Documentation
└── plans/                 # Implementation plans
```

## Observability

When `observability.metrics.enabled: true`, Iris exposes a separate HTTP server (default `:9090`) with:

| Endpoint | Description |
|----------|-------------|
| `/metrics` | Prometheus metrics (events processed, replication lag, transform/sink duration, errors) |
| `/healthz` | Liveness probe (always 200) |
| `/readyz` | Readiness probe (checks component health) |

### Prometheus Metrics

| Metric | Type | Labels |
|--------|------|--------|
| `iris_events_processed_total` | Counter | table, op, status |
| `iris_replication_lag_seconds` | Gauge | — |
| `iris_transform_duration_seconds` | Histogram | — |
| `iris_sink_write_duration_seconds` | Histogram | — |
| `iris_pipeline_errors_total` | Counter | component, error_type |

### OpenTelemetry Tracing

When `observability.tracing.enabled: true`, Iris exports traces via OTLP/gRPC with three spans per event:

```
pipeline.process_event → transform.process → sink.write
```

Compatible with Jaeger, Tempo, and any OTLP-compatible collector.

## Use Cases

- **Cache sync** - Postgres → Iris → Redis List
- **Event streaming** - Postgres → Iris → Redis Stream → Consumers
- **NATS event bus** - Postgres → Iris → NATS JetStream → Microservices
- **Table-specific routing** - Postgres (users, orders) → Iris → Separate Redis Streams/Kafka topics/NATS subjects per table

## License

MIT
