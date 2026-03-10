# Iris

**Iris — Programmable CDC pipelines with WASM**

Iris is a lightweight Change Data Capture (CDC) pipeline that reads database change events, transforms them using WebAssembly, and streams to message systems.

```
Postgres → Iris → WASM Transform → Redis/Kafka
```

## Features

- **Lightweight CDC engine** - Replaces heavy stacks like Debezium + Kafka Connect for small use cases
- **WASM transforms** - Custom event transformation logic via WebAssembly modules
- **Redis List and Stream sinks** - Push events to Redis Lists (LPUSH) or Streams (XADD) with automatic trimming
- **Table-to-stream mapping** - Route table changes to dedicated Redis Stream keys
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
  path: ./transforms/filter.wasm

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

Attach WASM modules to transform events:

```rust
// Example: Filter only users table
fn handle(event: Event) -> Option<Event> {
    if event.table == "users" {
        return Some(event)
    }
    None
}
```

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
| `make clean`                       | Clean build artifacts                              |

## Project Structure

```
iris/
├── cmd/iris/              # CLI entrypoint (urfave/cli)
├── pkg/
│   ├── cdc/               # Core CDC interfaces and types
│   ├── config/            # Configuration loading
│   └── logger/            # Structured logger (slog)
├── internal/
│   ├── source/postgres/   # PostgreSQL CDC connector (pglogrepl)
│   ├── transform/
│   │   ├── wasm/          # WASM runtime (wazero)
│   │   └── nop/           # No-op passthrough transform
│   ├── sink/
│   │   ├── factory.go     # Registry-based sink factory
│   │   └── redis/         # Redis List + Stream sinks
│   └── pipeline/          # Pipeline orchestration
├── tests/
│   └── e2e/               # End-to-end tests
├── docs/                  # Documentation
└── plans/                 # Implementation plans
```

## Use Cases

- **Cache sync** - Postgres → Iris → Redis List
- **Event streaming** - Postgres → Iris → Redis Stream → Consumers
- **Table-specific routing** - Postgres (users, orders) → Iris → Separate Redis Streams per table

## License

MIT
