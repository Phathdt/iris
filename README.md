# Iris

**Iris — Programmable CDC pipelines with WASM**

Iris is a lightweight Change Data Capture (CDC) pipeline that reads database change events, transforms them using WebAssembly, and streams to message systems.

```
Postgres → Iris → WASM Transform → Redis/Kafka
```

## Features

- **Lightweight CDC engine** - Replaces heavy stacks like Debezium + Kafka Connect for small use cases
- **WASM transforms** - Custom event filtering and transformation via WebAssembly (Rust, TinyGo)
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
├── examples/transforms/   # Example WASM modules (Rust + TinyGo)
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
