# CLAUDE.md

This file provides guidance to Claude Code when working with the Iris project.

## Project Overview

Iris is a programmable Change Data Capture (CDC) pipeline powered by WebAssembly. It reads database change events, transforms them using WASM modules, and streams to message systems.

**Core Pipeline:**
```
Datasource → Iris → WASM Transform → Stream
```

**Example:**
```
Postgres → Iris → WASM filter → Kafka
```

## Key Commands

### Build & Run

```bash
make build                    # Build binary
./bin/iris -config config.yaml  # Run pipeline
```

### Testing

```bash
make test              # All tests
make test-unit         # Unit tests only
make test-integration  # Integration tests (requires PostgreSQL + Redis)
make test-coverage     # With coverage report
make test-race         # Race detector
```

### Development

```bash
make fmt               # Format code
make lint              # Run linter
make tidy              # Tidy go.mod
make deps              # Download dependencies
```

## Project Structure

```
iris/
├── cmd/iris/main.go           # CLI entrypoint
├── pkg/
│   ├── cdc/                   # Core CDC types
│   │   ├── event.go           # Event struct
│   │   ├── source.go          # Source interface
│   │   ├── transform.go       # Transform interface
│   │   ├── sink.go            # Sink interface
│   │   ├── encoder.go         # Encoder interface
│   │   └── decoder.go         # Decoder interface
│   └── config/
│       └── config.go          # Configuration loader
├── internal/
│   ├── source/postgres/       # PostgreSQL CDC connector
│   ├── transform/wasm/        # WASM runtime (wazero)
│   ├── sink/redis/            # Redis stream sink
│   ├── encoder/               # JSON encoder
│   └── pipeline/              # Pipeline orchestration
├── tests/e2e/                 # E2E tests
├── docs/                      # Documentation (PRD, etc.)
└── plans/                     # Implementation plans
```

## Configuration

Configuration is loaded from YAML files. See `config.example.yaml` for template.

```yaml
source:
  type: postgres
  dsn: postgres://user:password@localhost:5432/mydb
  tables: [users, orders]
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

## Event Format

```json
{
  "source": "postgres",
  "table": "users",
  "op": "insert|update|delete",
  "ts": 1710000000,
  "before": {},
  "after": {}
}
```

## Dependencies

- **pgx/v5** - PostgreSQL driver
- **pglogrepl** - Logical replication
- **go-redis/v9** - Redis client
- **wazero** - WASM runtime (zero dependencies)
- **yaml.v3** - YAML parsing

## Development Notes

- Go 1.26.0
- Uses logical replication for PostgreSQL CDC
- WASM transforms use wazero runtime
- Redis sink uses STREAM commands

## Testing Requirements

- Unit tests: No external dependencies
- Integration tests: Require PostgreSQL + Redis
- E2E tests: Use Docker Compose (see `tests/e2e/`)
