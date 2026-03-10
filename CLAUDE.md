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

### WASM

```bash
make build-wasm        # Build example WASM modules (requires Rust + wasm32-unknown-unknown)
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
│   │   │   └── testdata/      # Pre-built WASM binaries for tests
│   │   └── nop/               # No-op transform (passthrough)
│   ├── sink/
│   │   ├── factory.go         # Registry-based sink factory
│   │   └── redis/             # Redis List + Stream sinks
│   └── pipeline/              # Pipeline orchestration
├── examples/transforms/       # Example WASM transform modules
│   ├── passthrough/           # TinyGo passthrough (returns events unchanged)
│   ├── filter/                # TinyGo table filter (allowlist)
│   ├── passthrough-rs/        # Rust passthrough
│   └── filter-rs/             # Rust table filter
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

## WASM Transform ABI

Modules must export two functions:

- `alloc(size: u32) -> u32` — allocate memory for host to write input
- `handle(ptr: u32, len: u32) -> u32` — process event, return pointer to WASMResult

WASMResult layout (16 bytes, little-endian):
```
[data_ptr:u32][data_len:u32][err_ptr:u32][err_len:u32]
```

- `data_len=0` → drop event (filtered out)
- `err_len>0` → error (read error string from err_ptr)
- Otherwise → transformed event JSON at data_ptr

Example modules in `examples/transforms/` (Rust and TinyGo).

## Development Notes

- Go 1.26.0
- Uses logical replication for PostgreSQL CDC (pgoutput plugin)
- WASM transforms use wazero runtime with `WithStartFunctions()` to skip `_start`
- Two Redis sink types: List (LPUSH+LTRIM) and Stream (XADD+XTRIM)
- Sink factory pattern with registry-based builder registration
- Sinks handle JSON encoding internally
- WASM examples: Rust modules via `cargo build --target wasm32-unknown-unknown`, TinyGo modules via `tinygo build -target=wasi` (requires Go <=1.25)

## Testing Requirements

- Unit tests: No external dependencies (`make test-unit`)
- Integration tests with testcontainers: Require Docker (`make test-integration-containers`)
  - Uses `//go:build integration` build tag
  - Auto-manages PostgreSQL and Redis containers via testcontainers-go
- E2E tests: Use Docker Compose (see `tests/e2e/`)
