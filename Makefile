.PHONY: test test-unit test-integration test-e2e test-coverage clean build

# Go parameters
GOCMD=go
GOBUILD=$(GOCMD) build
GOTEST=$(GOCMD) test
GOGET=$(GOCMD) get
GOMOD=$(GOCMD) mod
GOFMT=$(GOCMD) fmt

# Test flags
TEST_FLAGS=-v
TEST_SHORT=-short
TEST_TIMEOUT=120s
TEST_RACE=-race

# Coverage
COVERAGE_DIR=coverage
COVERAGE_FILE=$(COVERAGE_DIR)/coverage.out
COVERAGE_HTML=$(COVERAGE_DIR)/coverage.html

# Build
build:
	$(GOBUILD) -o bin/iris ./cmd/iris

# Run all tests (unit + integration)
test:
	$(GOTEST) $(TEST_FLAGS) -timeout $(TEST_TIMEOUT) ./...

# Run unit tests only (skip integration tests)
test-unit:
	$(GOTEST) $(TEST_FLAGS) $(TEST_SHORT) -timeout 60s ./pkg/... ./internal/...

# Run integration tests only (require PostgreSQL + Redis)
test-integration:
	$(GOTEST) $(TEST_FLAGS) -timeout $(TEST_TIMEOUT) -run Integration ./internal/...

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

# Format code
fmt:
	$(GOFMT) ./...

# Download dependencies
deps:
	$(GOMOD) download
	$(GOMOD) verify

# Tidy go.mod
tidy:
	$(GOMOD) tidy

# Lint (basic)
lint:
	go vet ./...
	go fmt ./...

# Clean build artifacts
clean:
	rm -rf bin/
	rm -rf $(COVERAGE_DIR)

# Create coverage directory
$(COVERAGE_DIR):
	mkdir -p $(COVERAGE_DIR)

# Help
help:
	@echo "Available targets:"
	@echo "  test              - Run all tests"
	@echo "  test-unit         - Run unit tests (skip integration)"
	@echo "  test-integration  - Run integration tests"
	@echo "  test-e2e          - Run E2E tests (set E2E_TEST=1)"
	@echo "  test-e2e-docker   - Run E2E tests with Docker Compose"
	@echo "  test-coverage     - Run tests with coverage report"
	@echo "  test-race         - Run tests with race detector"
	@echo "  test-bench        - Run benchmarks"
	@echo "  build             - Build the binary"
	@echo "  fmt               - Format code"
	@echo "  deps              - Download dependencies"
	@echo "  tidy              - Tidy go.mod"
	@echo "  lint              - Run linter (vet, fmt)"
	@echo "  clean             - Clean build artifacts"
	@echo "  clean-coverage    - Remove coverage files"
	@echo "  help              - Show this help"
