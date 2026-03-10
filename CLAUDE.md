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
make test                          # All tests
make test-unit                     # Unit tests only
make test-integration              # Integration tests (requires PostgreSQL + Redis)
make test-integration-containers   # Integration tests with testcontainers (auto-manages Docker)
make test-coverage                 # With coverage report
make test-race                     # Race detector
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
├── cmd/iris/main.go           # CLI entrypoint (urfave/cli)
├── pkg/
│   ├── cdc/                   # Core CDC types
│   │   ├── event.go           # Event struct
│   │   ├── source.go          # Source interface
│   │   ├── transform.go       # Transform interface
│   │   ├── sink.go            # Sink interface
│   │   ├── decoder.go         # Decoder interface
│   │   └── offset.go          # Offset tracking
│   ├── config/
│   │   └── config.go          # Configuration loader
│   └── logger/
│       └── logger.go          # Structured logger (slog)
├── internal/
│   ├── source/postgres/       # PostgreSQL CDC connector (pglogrepl)
│   ├── transform/
│   │   ├── wasm/              # WASM runtime (wazero)
│   │   └── nop/               # No-op transform (passthrough)
│   ├── sink/
│   │   ├── factory.go         # Registry-based sink factory
│   │   └── redis/             # Redis List + Stream sinks
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

# Sink option 1: Redis List (LPUSH + LTRIM)
sink:
  type: redis
  addr: localhost:6379
  key: cdc:events
  max_len: 10000

# Sink option 2: Redis Stream (XADD + XTRIM)
# sink:
#   type: redis_stream
#   addr: localhost:6379
#   max_len: 10000
# mapping:
#   table_stream_map:
#     users: cdc:users
#     orders: cdc:orders
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
- **pglogrepl** - Logical replication protocol
- **go-redis/v9** - Redis client
- **wazero** - WASM runtime (zero dependencies)
- **urfave/cli/v2** - CLI framework
- **yaml.v3** - YAML parsing
- **testcontainers-go** - Integration test containers (PostgreSQL, Redis)

## Development Notes

- Go 1.26.0
- Uses logical replication for PostgreSQL CDC (pgoutput plugin)
- WASM transforms use wazero runtime
- Two Redis sink types: List (LPUSH+LTRIM) and Stream (XADD+XTRIM)
- Sink factory pattern with registry-based builder registration
- Sinks handle JSON encoding internally

## Testing Requirements

- Unit tests: No external dependencies (`make test-unit`)
- Integration tests with testcontainers: Require Docker (`make test-integration-containers`)
  - Uses `//go:build integration` build tag
  - Auto-manages PostgreSQL and Redis containers via testcontainers-go
- E2E tests: Use Docker Compose (see `tests/e2e/`)
