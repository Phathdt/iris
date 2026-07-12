# Iris

**Iris — Programmable CDC pipelines with WASM**

Iris is a lightweight Change Data Capture (CDC) pipeline that reads database change events, transforms them using WebAssembly, and streams to message systems.

```
Postgres → Iris → WASM Transform → Kafka
```

## Features

- **Lightweight CDC engine** - Replaces heavy stacks like Debezium + Kafka Connect for small use cases
- **WASM transforms** - Custom event filtering and transformation via WebAssembly (Rust, TinyGo)
- **Transform chaining** - Apply multiple WASM modules sequentially
- **Kafka sink** - Stream events to Apache Kafka topics, with optional outbox column shaping (key/topic/body from row columns, à la Debezium Outbox Event Router)
- **Stdout and file sinks** - Write CDC events to stdout or JSON-line files (with rotation) for dev/debug
- **Table-to-topic mapping** - Route table changes to dedicated Kafka topics
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

# Option 1: Kafka sink
sink:
  type: kafka
  brokers:
    - localhost:9092
  # Optional: map tables to topics (defaults to "cdc.{table}")
mapping:
  table_stream_map:
    users: users-topic
    orders: orders-topic

# Option 1b: Kafka sink with outbox column shaping
# Derive the Kafka message key, topic, and body from columns in event.After
# (models the Debezium Outbox Event Router). Lets Iris replace a hand-rolled
# outbox-replayer while preserving per-aggregate ordering (key = aggregate_id).
# A missing/null/empty column falls back to default behavior per field
# (key=table, topic=cdc.{table}, body=full envelope).
# sink:
#   type: kafka
#   brokers:
#     - localhost:9092
#   outbox:
#     key_field: aggregate_id   # Kafka message key (per-aggregate ordering)
#     route_field: event_type   # topic = value of this column (identity routing)
#     payload_field: payload    # message body = JSON of this column

# Option 2: Stdout sink (dev/debug)
# sink:
#   type: stdout
#   pretty_print: false

# Option 3: File sink (with optional rotation)
# sink:
#   type: file
#   path: ./events.jsonl
#   max_size: 104857600
#   max_files: 5

# Dead letter queue (optional)
dlq:
  enabled: true
  sink:
    type: kafka
    brokers:
      - localhost:9092

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
│ Kafka       │  Produce to topic
│ Stdout      │  JSON to stdout
│ File        │  JSON lines with rotation
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
| `make test-integration`            | Run integration tests (requires local PG)          |
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
│   │   ├── kafka/         # Kafka sink (franz-go)
│   │   ├── stdout/        # Stdout sink (dev/debug)
│   │   └── file/          # File sink with rotation
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

- **Event streaming** - Postgres → Iris → Kafka topics → Consumers
- **Outbox pattern** - Postgres outbox table → Iris → Kafka (key/topic/body derived from row columns)
- **Table-specific routing** - Postgres (users, orders) → Iris → Separate Kafka topics per table
- **Local dev/debug** - Postgres → Iris → stdout or JSON-line file

## License

MIT
