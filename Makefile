.PHONY: test test-unit test-integration test-e2e test-coverage format fmt lint build run clean help version build-wasm

# Go parameters
GOCMD=go
GOBUILD=$(GOCMD) build
GOTEST=$(GOCMD) test
GOGET=$(GOCMD) get
GOMOD=$(GOCMD) mod
GOFMT=gofmt

# Binary name
BINARY_NAME=iris

# Version info (set via ldflags)
VERSION=$(shell git describe --tags --always --dirty 2>/dev/null || echo "dev")
GIT_COMMIT=$(shell git rev-parse --short HEAD 2>/dev/null || echo "unknown")
BUILD_DATE=$(shell date -u +%Y-%m-%dT%H:%M:%SZ)
LDFLAGS=-s -w -X 'iris/pkg/config.version=$(VERSION)' -X 'iris/pkg/config.gitCommit=$(GIT_COMMIT)' -X 'iris/pkg/config.buildDate=$(BUILD_DATE)'

# Test flags
TEST_FLAGS=-v
TEST_SHORT=-short
TEST_TIMEOUT=120s
TEST_RACE=-race

# Coverage
COVERAGE_DIR=coverage
COVERAGE_FILE=$(COVERAGE_DIR)/coverage.out
COVERAGE_HTML=$(COVERAGE_DIR)/coverage.html

# Build with version info
build:
	$(GOBUILD) -ldflags "$(LDFLAGS)" -o bin/$(BINARY_NAME) ./cmd/iris

# Build without debug info (smaller binary)
build-release:
	$(GOBUILD) -ldflags "$(LDFLAGS) -s -w" -o bin/$(BINARY_NAME) ./cmd/iris

# Run the application
run:
	$(GOBUILD) -o bin/$(BINARY_NAME) ./cmd/iris && ./bin/$(BINARY_NAME)

# Run with config
run-config:
	./bin/$(BINARY_NAME) --config $(CONFIG)

# Format code (fix formatting)
format:
	@echo "🎨 Formatting all Go files..."
	@gofmt -w .
	@echo "📦 Organizing imports..."
	@goimports -w .
	@echo "📏 Formatting line lengths..."
	@golines -w -m 120 .
	@echo "✨ Applying gofumpt formatting..."
	@gofumpt -extra -w .
	@echo "✅ Go files formatted successfully!"


# Check formatting (don't fix, just report)
fmt-check:
	@files=$$(gofmt -l .); \
	if [ -n "$$files" ]; then \
		echo "Unformatted files found:"; \
		echo "$$files"; \
		exit 1; \
	fi
	@echo "All files properly formatted"

# Download dependencies
deps:
	$(GOMOD) download
	$(GOMOD) verify

# Tidy go.mod
tidy:
	$(GOMOD) tidy

# Lint (basic)
lint:
	@echo "Running go vet..."
	go vet ./...
	@echo "Running go fmt..."
	@$(GOFMT) -d .
	@echo "Lint complete"

# Clean build artifacts
clean:
	rm -rf bin/
	rm -rf $(COVERAGE_DIR)

# Create coverage directory
$(COVERAGE_DIR):
	mkdir -p $(COVERAGE_DIR)

# Run all tests (unit + integration)
test:
	$(GOTEST) $(TEST_FLAGS) -timeout $(TEST_TIMEOUT) ./...

# Run unit tests only (skip integration tests)
test-unit:
	$(GOTEST) $(TEST_FLAGS) $(TEST_SHORT) -timeout 60s ./pkg/... ./internal/...

# Run integration tests only (require PostgreSQL + Redis)
test-integration:
	$(GOTEST) $(TEST_FLAGS) -timeout $(TEST_TIMEOUT) -run Integration ./internal/...

# Run integration tests with testcontainers-go (auto-manages Redis)
test-integration-containers:
	$(GOTEST) $(TEST_FLAGS) -tags=integration -timeout $(TEST_TIMEOUT) ./internal/...

# Run E2E tests (requires Docker Compose)
test-e2e:
	E2E_TEST=1 $(GOTEST) $(TEST_FLAGS) -timeout 300s ./tests/e2e/...

# Run E2E tests with Docker Compose
test-e2e-docker:
	docker-compose -f tests/e2e/docker-compose.yml up --build --abort-on-container-exit

# Run tests with coverage
test-coverage: $(COVERAGE_DIR)
	$(GOTEST) $(TEST_FLAGS) -timeout $(TEST_TIMEOUT) -coverprofile=$(COVERAGE_FILE) ./...
	$(GOCMD) tool cover -html=$(COVERAGE_FILE) -o $(COVERAGE_HTML)
	@echo "Coverage report generated: $(COVERAGE_HTML)"

# Run tests with race detector
test-race:
	$(GOTEST) $(TEST_FLAGS) $(TEST_RACE) -timeout $(TEST_TIMEOUT) ./pkg/... ./internal/...

# Run benchmarks
test-bench:
	$(GOTEST) -bench=. -benchmem -run=^$$ ./...

# Generate coverage only for specific packages
test-coverage-pkg: $(COVERAGE_DIR)
	$(GOTEST) -timeout $(TEST_TIMEOUT) -coverprofile=$(COVERAGE_DIR)/pkg.out ./pkg/...
	$(GOTEST) -timeout $(TEST_TIMEOUT) -coverprofile=$(COVERAGE_DIR)/internal.out ./internal/...

# Clean coverage files
clean-coverage:
	rm -rf $(COVERAGE_DIR)

# Show version
version:
	@echo "Version: $(VERSION)"
	@echo "Git Commit: $(GIT_COMMIT)"
	@echo "Build Date: $(BUILD_DATE)"

# Build WASM example modules (requires Rust + wasm32-unknown-unknown target)
build-wasm:
	@echo "Building WASM transform modules..."
	CARGO_TARGET_DIR=/tmp/iris-wasm-build cargo build --manifest-path examples/transforms/passthrough-rs/Cargo.toml --release --target wasm32-unknown-unknown
	CARGO_TARGET_DIR=/tmp/iris-wasm-build cargo build --manifest-path examples/transforms/filter-rs/Cargo.toml --release --target wasm32-unknown-unknown
	mkdir -p internal/transform/wasm/testdata
	cp /tmp/iris-wasm-build/wasm32-unknown-unknown/release/passthrough.wasm internal/transform/wasm/testdata/passthrough.wasm
	cp /tmp/iris-wasm-build/wasm32-unknown-unknown/release/filter.wasm internal/transform/wasm/testdata/filter.wasm
	@echo "WASM modules built successfully"

# Help
help:
	@echo "Iris CDC Pipeline - Makefile Targets"
	@echo "===================================="
	@echo ""
	@echo "Build:"
	@echo "  build             - Build the binary with version info"
	@echo "  build-release     - Build optimized binary (smaller)"
	@echo "  run               - Build and run the application"
	@echo "  run-config        - Run with config (CONFIG=path)"
	@echo ""
	@echo "Test:"
	@echo "  test                      - Run all tests"
	@echo "  test-unit                 - Run unit tests (skip integration)"
	@echo "  test-integration          - Run integration tests"
	@echo "  test-integration-containers - Run integration tests (auto-manages containers)"
	@echo "  test-e2e                  - Run E2E tests"
	@echo "  test-e2e-docker           - Run E2E tests with Docker Compose"
	@echo "  test-coverage             - Run tests with coverage report"
	@echo "  test-race                 - Run tests with race detector"
	@echo "  test-bench                - Run benchmarks"
	@echo ""
	@echo "Code Quality:"
	@echo "  fmt               - Format code (fix formatting)"
	@echo "  fmt-check         - Check formatting (report only)"
	@echo "  lint              - Run linter (vet, fmt)"
	@echo ""
	@echo "Dependencies:"
	@echo "  deps              - Download dependencies"
	@echo "  tidy              - Tidy go.mod"
	@echo ""
	@echo "Clean:"
	@echo "  clean             - Clean build artifacts"
	@echo "  clean-coverage    - Remove coverage files"
	@echo ""
	@echo "WASM:"
	@echo "  build-wasm        - Build example WASM modules (requires Rust)"
	@echo ""
	@echo "Other:"
	@echo "  version           - Show version info"
	@echo "  help              - Show this help"
