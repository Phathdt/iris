# Iris

**Iris — Programmable CDC pipelines with WASM**

Iris is a lightweight Change Data Capture (CDC) pipeline that reads database change events, transforms them using WebAssembly, and streams to message systems.

```
Postgres → Iris → WASM Transform → Redis/Kafka
```

## Features

- **Lightweight CDC engine** - Replaces heavy stacks like Debezium + Kafka Connect for small use cases
- **WASM transforms** - Custom event transformation logic via WebAssembly modules
- **Multiple stream sinks** - Support for Redis, Kafka, and more
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

sink:
  type: redis
  addr: localhost:6379
  key: cdc:events
  max_len: 10000
```

### Run Pipeline

```bash
./bin/iris -config config.yaml
```

## Architecture

```
┌─────────────┐
│  Datasource │
│  Postgres   │
└──────┬──────┘
       │
       v
┌─────────────┐
│  Iris CDC   │
│  Connector  │
└──────┬──────┘
       │
       v
┌─────────────┐
│ WASM Engine │
└──────┬──────┘
       │
       v
┌─────────────┐
│ Stream Sink │
│ Kafka/Redis │
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

| Command | Description |
|---------|-------------|
| `make build` | Build the binary |
| `make test` | Run all tests |
| `make test-unit` | Run unit tests only |
| `make test-integration` | Run integration tests |
| `make test-coverage` | Run tests with coverage report |
| `make lint` | Run linter |
| `make fmt` | Format code |
| `make clean` | Clean build artifacts |

## Project Structure

```
iris/
├── cmd/iris/          # CLI entrypoint
├── pkg/
│   ├── cdc/          # Core CDC interfaces and types
│   └── config/       # Configuration loading
├── internal/
│   ├── source/       # Source connectors (Postgres, MySQL, MongoDB)
│   ├── transform/    # WASM runtime and transforms
│   ├── sink/         # Stream sinks (Redis, Kafka)
│   ├── encoder/      # Event encoding
│   └── pipeline/     # Pipeline orchestration
├── tests/
│   └── e2e/          # End-to-end tests
├── docs/             # Documentation
└── plans/            # Implementation plans
```

## Use Cases

- **Cache sync** - Postgres → Iris → Redis
- **Audit pipeline** - MySQL → Iris → Kafka → Analytics
- **Event-driven backend** - MongoDB → Iris → RabbitMQ

## License

MIT
