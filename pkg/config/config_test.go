package config

import (
	"os"
	"path/filepath"
	"testing"
)

func TestLoad_ValidYAML(t *testing.T) {
	content := `
source:
  type: postgres
  dsn: postgres://user:pass@localhost:5432/testdb
  slot_name: test_slot
  tables:
    - users
    - orders

sink:
  type: kafka
  brokers:
    - localhost:9092
`

	tmpFile := createTempFile(t, content)
	defer os.Remove(tmpFile)

	cfg, err := Load(tmpFile)
	if err != nil {
		t.Fatalf("Load() error = %v", err)
	}

	if cfg.Source.Type != "postgres" {
		t.Errorf("Source.Type = %q, want %q", cfg.Source.Type, "postgres")
	}
	if cfg.Source.DSN != "postgres://user:pass@localhost:5432/testdb" {
		t.Errorf("Source.DSN = %q", cfg.Source.DSN)
	}
	if cfg.Source.SlotName != "test_slot" {
		t.Errorf("Source.SlotName = %q, want %q", cfg.Source.SlotName, "test_slot")
	}
	if len(cfg.Source.Tables) != 2 {
		t.Errorf("Source.Tables count = %d, want 2", len(cfg.Source.Tables))
	}
	if cfg.Sink.Type != "kafka" {
		t.Errorf("Sink.Type = %q, want %q", cfg.Sink.Type, "kafka")
	}
	if len(cfg.Sink.Brokers) != 1 || cfg.Sink.Brokers[0] != "localhost:9092" {
		t.Errorf("Sink.Brokers = %v, want [localhost:9092]", cfg.Sink.Brokers)
	}
}

func TestLoad_ValidYAML_WithKafkaOutbox(t *testing.T) {
	content := `
source:
  type: postgres
  dsn: postgres://user:pass@localhost:5432/testdb
  slot_name: test_slot

sink:
  type: kafka
  brokers:
    - localhost:9092
  outbox:
    key_field: aggregate_id
    route_field: event_type
    payload_field: payload
`

	tmpFile := createTempFile(t, content)
	defer os.Remove(tmpFile)

	cfg, err := Load(tmpFile)
	if err != nil {
		t.Fatalf("Load() error = %v", err)
	}

	if cfg.Sink.Outbox == nil {
		t.Fatal("Sink.Outbox = nil, want parsed outbox config")
	}
	if cfg.Sink.Outbox.KeyField != "aggregate_id" {
		t.Errorf("Outbox.KeyField = %q, want %q", cfg.Sink.Outbox.KeyField, "aggregate_id")
	}
	if cfg.Sink.Outbox.RouteField != "event_type" {
		t.Errorf("Outbox.RouteField = %q, want %q", cfg.Sink.Outbox.RouteField, "event_type")
	}
	if cfg.Sink.Outbox.PayloadField != "payload" {
		t.Errorf("Outbox.PayloadField = %q, want %q", cfg.Sink.Outbox.PayloadField, "payload")
	}
}

func TestLoad_KafkaWithoutOutbox_IsNil(t *testing.T) {
	content := `
source:
  type: postgres
  dsn: postgres://user:pass@localhost:5432/testdb
  slot_name: test_slot

sink:
  type: kafka
  brokers:
    - localhost:9092
`

	tmpFile := createTempFile(t, content)
	defer os.Remove(tmpFile)

	cfg, err := Load(tmpFile)
	if err != nil {
		t.Fatalf("Load() error = %v", err)
	}

	if cfg.Sink.Outbox != nil {
		t.Errorf("Sink.Outbox = %+v, want nil when no outbox block", cfg.Sink.Outbox)
	}
}

func TestLoad_ValidYAML_WithTransform(t *testing.T) {
	content := `
source:
  type: postgres
  dsn: postgres://user:pass@localhost:5432/testdb
  slot_name: test_slot

transform:
  enabled: true
  type: wasm
  path: /path/to/module.wasm
  function_name: handle
  alloc_function_name: alloc
  enable_logging: true

sink:
  type: kafka
  brokers:
    - localhost:9092
`

	tmpFile := createTempFile(t, content)
	defer os.Remove(tmpFile)

	cfg, err := Load(tmpFile)
	if err != nil {
		t.Fatalf("Load() error = %v", err)
	}

	if cfg.Transform == nil {
		t.Fatal("Transform is nil")
	}
	if !cfg.Transform.Enabled {
		t.Error("Transform.Enabled should be true")
	}
	if cfg.Transform.Type != "wasm" {
		t.Errorf("Transform.Type = %q, want %q", cfg.Transform.Type, "wasm")
	}
	if cfg.Transform.Path != "/path/to/module.wasm" {
		t.Errorf("Transform.Path = %q", cfg.Transform.Path)
	}
	if cfg.Transform.FunctionName != "handle" {
		t.Errorf("Transform.FunctionName = %q, want %q", cfg.Transform.FunctionName, "handle")
	}
	if cfg.Transform.AllocFunctionName != "alloc" {
		t.Errorf("Transform.AllocFunctionName = %q, want %q", cfg.Transform.AllocFunctionName, "alloc")
	}
	if !cfg.Transform.EnableLogging {
		t.Error("Transform.EnableLogging should be true")
	}
}

func TestLoad_MissingFile(t *testing.T) {
	_, err := Load("/nonexistent/path/config.yaml")
	if err == nil {
		t.Fatal("Load() expected error for missing file, got nil")
	}

	if !contains(err.Error(), "read config file") {
		t.Errorf("Load() error = %v, should contain 'read config file'", err)
	}
}

func TestLoad_InvalidYAML(t *testing.T) {
	content := `
source:
  type: postgres
  dsn: postgres://localhost:5432/testdb
  slot_name: test_slot
  invalid yaml here: [unclosed
`

	tmpFile := createTempFile(t, content)
	defer os.Remove(tmpFile)

	_, err := Load(tmpFile)
	if err == nil {
		t.Fatal("Load() expected error for invalid YAML, got nil")
	}

	if !contains(err.Error(), "parse config file") {
		t.Errorf("Load() error = %v, should contain 'parse config file'", err)
	}
}

func TestLoad_EmptyFile(t *testing.T) {
	content := ""

	tmpFile := createTempFile(t, content)
	defer os.Remove(tmpFile)

	_, err := Load(tmpFile)
	if err == nil {
		t.Fatal("Load() expected error for empty file, got nil")
	}
}

func TestValidate_MissingDSN(t *testing.T) {
	cfg := &Config{
		Source: SourceConfig{
			Type:     "postgres",
			SlotName: "test_slot",
		},
		Sink: SinkConfig{
			Type:    "kafka",
			Brokers: []string{"localhost:9092"},
		},
	}

	err := cfg.Validate()
	if err == nil {
		t.Fatal("Validate() expected error for missing DSN, got nil")
	}

	if err.Error() != "source.dsn is required" {
		t.Errorf("Validate() error = %v, want 'source.dsn is required'", err)
	}
}

func TestValidate_MissingSlotName(t *testing.T) {
	cfg := &Config{
		Source: SourceConfig{
			Type: "postgres",
			DSN:  "postgres://localhost:5432/testdb",
		},
		Sink: SinkConfig{
			Type:    "kafka",
			Brokers: []string{"localhost:9092"},
		},
	}

	err := cfg.Validate()
	if err == nil {
		t.Fatal("Validate() expected error for missing slot name, got nil")
	}

	if err.Error() != "source.slot_name is required" {
		t.Errorf("Validate() error = %v, want 'source.slot_name is required'", err)
	}
}

func TestValidate_MissingSinkBrokers(t *testing.T) {
	cfg := &Config{
		Source: SourceConfig{
			Type:     "postgres",
			DSN:      "postgres://localhost:5432/testdb",
			SlotName: "test_slot",
		},
		Sink: SinkConfig{
			Type: "kafka",
		},
	}

	err := cfg.Validate()
	if err == nil {
		t.Fatal("Validate() expected error for missing sink brokers, got nil")
	}

	if err.Error() != "sink.brokers is required for kafka sink" {
		t.Errorf("Validate() error = %v, want 'sink.brokers is required for kafka sink'", err)
	}
}

func TestValidate_MissingSinkPath(t *testing.T) {
	cfg := &Config{
		Source: SourceConfig{
			Type:     "postgres",
			DSN:      "postgres://localhost:5432/testdb",
			SlotName: "test_slot",
		},
		Sink: SinkConfig{
			Type: "file",
		},
	}

	err := cfg.Validate()
	if err == nil {
		t.Fatal("Validate() expected error for missing sink path, got nil")
	}

	if err.Error() != "sink.path is required for file sink" {
		t.Errorf("Validate() error = %v, want 'sink.path is required for file sink'", err)
	}
}

func TestValidate_UnsupportedSourceType(t *testing.T) {
	cfg := &Config{
		Source: SourceConfig{
			Type:     "mysql",
			DSN:      "mysql://localhost:3306/testdb",
			SlotName: "test_slot",
		},
		Sink: SinkConfig{
			Type:    "kafka",
			Brokers: []string{"localhost:9092"},
		},
	}

	err := cfg.Validate()
	if err == nil {
		t.Fatal("Validate() expected error for unsupported source type, got nil")
	}

	if err.Error() != "unsupported source type: mysql" {
		t.Errorf("Validate() error = %v, want 'unsupported source type: mysql'", err)
	}
}

func TestValidate_UnsupportedSinkType(t *testing.T) {
	cfg := &Config{
		Source: SourceConfig{
			Type:     "postgres",
			DSN:      "postgres://localhost:5432/testdb",
			SlotName: "test_slot",
		},
		Sink: SinkConfig{
			Type: "rabbitmq",
		},
	}

	err := cfg.Validate()
	if err == nil {
		t.Fatal("Validate() expected error for unsupported sink type, got nil")
	}

	if err.Error() != "unsupported sink type: rabbitmq" {
		t.Errorf("Validate() error = %v, want 'unsupported sink type: rabbitmq'", err)
	}
}

func TestValidate_Kafka_ValidConfig(t *testing.T) {
	cfg := &Config{
		Source: SourceConfig{
			Type:     "postgres",
			DSN:      "postgres://localhost:5432/testdb",
			SlotName: "test_slot",
		},
		Sink: SinkConfig{
			Type:    "kafka",
			Brokers: []string{"localhost:9092"},
		},
	}

	err := cfg.Validate()
	if err != nil {
		t.Errorf("Validate() error = %v, want nil for valid kafka config", err)
	}
}

func TestValidate_Kafka_MissingBrokers(t *testing.T) {
	cfg := &Config{
		Source: SourceConfig{
			Type:     "postgres",
			DSN:      "postgres://localhost:5432/testdb",
			SlotName: "test_slot",
		},
		Sink: SinkConfig{
			Type: "kafka",
		},
	}

	err := cfg.Validate()
	if err == nil {
		t.Fatal("Validate() expected error for missing brokers, got nil")
	}
	if err.Error() != "sink.brokers is required for kafka sink" {
		t.Errorf("Validate() error = %v, want 'sink.brokers is required for kafka sink'", err)
	}
}

func TestValidate_TransformOptional(t *testing.T) {
	cfg := &Config{
		Source: SourceConfig{
			Type:     "postgres",
			DSN:      "postgres://localhost:5432/testdb",
			SlotName: "test_slot",
		},
		Sink: SinkConfig{
			Type:    "kafka",
			Brokers: []string{"localhost:9092"},
		},
		// Transform is nil (optional)
	}

	err := cfg.Validate()
	if err != nil {
		t.Errorf("Validate() error = %v, want nil (transform is optional)", err)
	}
}

func TestValidate_TransformEnabled_MissingPath(t *testing.T) {
	cfg := &Config{
		Source: SourceConfig{
			Type:     "postgres",
			DSN:      "postgres://localhost:5432/testdb",
			SlotName: "test_slot",
		},
		Transform: &TransformConfig{
			Enabled: true,
			Type:    "wasm",
			// Path is missing
		},
		Sink: SinkConfig{
			Type:    "kafka",
			Brokers: []string{"localhost:9092"},
		},
	}

	err := cfg.Validate()
	if err == nil {
		t.Fatal("Validate() expected error for missing transform path, got nil")
	}

	if err.Error() != "transform.path or transform.modules is required when enabled" {
		t.Errorf("Validate() error = %v, want 'transform.path or transform.modules is required when enabled'", err)
	}
}

func TestValidate_TransformEnabled_UnsupportedType(t *testing.T) {
	cfg := &Config{
		Source: SourceConfig{
			Type:     "postgres",
			DSN:      "postgres://localhost:5432/testdb",
			SlotName: "test_slot",
		},
		Transform: &TransformConfig{
			Enabled: true,
			Type:    "lua",
			Path:    "/path/to/script.lua",
		},
		Sink: SinkConfig{
			Type:    "kafka",
			Brokers: []string{"localhost:9092"},
		},
	}

	err := cfg.Validate()
	if err == nil {
		t.Fatal("Validate() expected error for unsupported transform type, got nil")
	}

	if err.Error() != "unsupported transform type: lua" {
		t.Errorf("Validate() error = %v, want 'unsupported transform type: lua'", err)
	}
}

func TestValidate_TransformDisabled_PathNotRequired(t *testing.T) {
	cfg := &Config{
		Source: SourceConfig{
			Type:     "postgres",
			DSN:      "postgres://localhost:5432/testdb",
			SlotName: "test_slot",
		},
		Transform: &TransformConfig{
			Enabled: false,
			Type:    "wasm",
			// Path not required when disabled
		},
		Sink: SinkConfig{
			Type:    "kafka",
			Brokers: []string{"localhost:9092"},
		},
	}

	err := cfg.Validate()
	if err != nil {
		t.Errorf("Validate() error = %v, want nil (path not required when disabled)", err)
	}
}

func TestValidate_ValidConfig(t *testing.T) {
	cfg := &Config{
		Source: SourceConfig{
			Type:     "postgres",
			DSN:      "postgres://localhost:5432/testdb",
			SlotName: "test_slot",
			Tables:   []string{"users", "orders"},
		},
		Sink: SinkConfig{
			Type:    "kafka",
			Brokers: []string{"localhost:9092"},
		},
	}

	err := cfg.Validate()
	if err != nil {
		t.Errorf("Validate() error = %v, want nil", err)
	}
}

func TestValidate_File_ValidConfig(t *testing.T) {
	cfg := &Config{
		Source: SourceConfig{
			Type:     "postgres",
			DSN:      "postgres://localhost:5432/testdb",
			SlotName: "test_slot",
		},
		Sink: SinkConfig{
			Type: "file",
			Path: "/var/log/cdc/events.log",
		},
	}

	err := cfg.Validate()
	if err != nil {
		t.Errorf("Validate() error = %v, want nil for valid file config", err)
	}
}

func TestValidate_File_MissingPath(t *testing.T) {
	cfg := &Config{
		Source: SourceConfig{
			Type:     "postgres",
			DSN:      "postgres://localhost:5432/testdb",
			SlotName: "test_slot",
		},
		Sink: SinkConfig{
			Type: "file",
			Path: "",
		},
	}

	err := cfg.Validate()
	if err == nil {
		t.Fatal("Validate() expected error for missing path, got nil")
	}
	if err.Error() != "sink.path is required for file sink" {
		t.Errorf("Validate() error = %v, want 'sink.path is required for file sink'", err)
	}
}

func TestValidate_Stdout_ValidConfig(t *testing.T) {
	cfg := &Config{
		Source: SourceConfig{
			Type:     "postgres",
			DSN:      "postgres://localhost:5432/testdb",
			SlotName: "test_slot",
		},
		Sink: SinkConfig{
			Type: "stdout",
		},
	}

	err := cfg.Validate()
	if err != nil {
		t.Errorf("Validate() error = %v, want nil for valid stdout config (no required fields)", err)
	}
}

func TestLoadAndValidate_Integration(t *testing.T) {
	content := `
source:
  type: postgres
  dsn: postgres://iris:iris@localhost:54321/testdb
  slot_name: iris_slot

sink:
  type: kafka
  brokers:
    - localhost:9092
`

	tmpFile := createTempFile(t, content)
	defer os.Remove(tmpFile)

	cfg, err := Load(tmpFile)
	if err != nil {
		t.Fatalf("Load() error = %v", err)
	}

	// Verify the loaded config is valid
	if err := cfg.Validate(); err != nil {
		t.Errorf("Loaded config Validate() error = %v", err)
	}
}

func TestLoadAndValidate_InvalidConfig(t *testing.T) {
	content := `
source:
  type: mysql
  dsn: mysql://localhost:3306/testdb

sink:
  type: kafka
  brokers:
    - localhost:9092
`

	tmpFile := createTempFile(t, content)
	defer os.Remove(tmpFile)

	_, err := Load(tmpFile)
	if err == nil {
		t.Fatal("Load() expected error for invalid config, got nil")
	}

	if !contains(err.Error(), "validate config") {
		t.Errorf("Load() error = %v, should contain 'validate config'", err)
	}
}

func TestValidate_DLQ_Valid(t *testing.T) {
	cfg := &Config{
		Source: SourceConfig{
			Type:     "postgres",
			DSN:      "postgres://localhost:5432/testdb",
			SlotName: "test_slot",
		},
		Sink: SinkConfig{
			Type:    "kafka",
			Brokers: []string{"localhost:9092"},
		},
		DLQ: &DLQConfig{
			Enabled: true,
			Sink: SinkConfig{
				Type: "file",
				Path: "/var/log/cdc/dlq.log",
			},
		},
	}

	err := cfg.Validate()
	if err != nil {
		t.Errorf("Validate() error = %v, want nil for valid DLQ config", err)
	}
}

func TestValidate_DLQ_MissingSinkType(t *testing.T) {
	cfg := &Config{
		Source: SourceConfig{
			Type:     "postgres",
			DSN:      "postgres://localhost:5432/testdb",
			SlotName: "test_slot",
		},
		Sink: SinkConfig{
			Type:    "kafka",
			Brokers: []string{"localhost:9092"},
		},
		DLQ: &DLQConfig{
			Enabled: true,
			Sink:    SinkConfig{Type: "rabbitmq"},
		},
	}

	err := cfg.Validate()
	if err == nil {
		t.Fatal("Validate() expected error for unsupported DLQ sink type, got nil")
	}
	if !contains(err.Error(), "dlq.sink") {
		t.Errorf("Validate() error = %v, should contain 'dlq.sink'", err)
	}
}

func TestValidate_DLQ_Disabled_NoValidation(t *testing.T) {
	cfg := &Config{
		Source: SourceConfig{
			Type:     "postgres",
			DSN:      "postgres://localhost:5432/testdb",
			SlotName: "test_slot",
		},
		Sink: SinkConfig{
			Type:    "kafka",
			Brokers: []string{"localhost:9092"},
		},
		DLQ: &DLQConfig{
			Enabled: false,
			Sink:    SinkConfig{}, // Invalid but shouldn't matter when disabled
		},
	}

	err := cfg.Validate()
	if err != nil {
		t.Errorf("Validate() error = %v, want nil (DLQ disabled)", err)
	}
}

func TestValidate_NoDLQ_BackwardCompat(t *testing.T) {
	cfg := &Config{
		Source: SourceConfig{
			Type:     "postgres",
			DSN:      "postgres://localhost:5432/testdb",
			SlotName: "test_slot",
		},
		Sink: SinkConfig{
			Type:    "kafka",
			Brokers: []string{"localhost:9092"},
		},
		// DLQ is nil — backward compatible
	}

	err := cfg.Validate()
	if err != nil {
		t.Errorf("Validate() error = %v, want nil (no DLQ = backward compat)", err)
	}
}

func TestLoad_RetryDefaults(t *testing.T) {
	content := `
source:
  type: postgres
  dsn: postgres://localhost:5432/testdb
  slot_name: test_slot
sink:
  type: kafka
  brokers:
    - localhost:9092
`
	tmpFile := createTempFile(t, content)
	defer os.Remove(tmpFile)

	cfg, err := Load(tmpFile)
	if err != nil {
		t.Fatalf("Load() error = %v", err)
	}

	if cfg.Retry.MaxAttempts != 3 {
		t.Errorf("Retry.MaxAttempts = %d, want 3 (default)", cfg.Retry.MaxAttempts)
	}
	if cfg.Retry.BackoffMs != 100 {
		t.Errorf("Retry.BackoffMs = %d, want 100 (default)", cfg.Retry.BackoffMs)
	}
}

func TestLoad_DLQ_YAML(t *testing.T) {
	content := `
source:
  type: postgres
  dsn: postgres://localhost:5432/testdb
  slot_name: test_slot
sink:
  type: kafka
  brokers:
    - localhost:9092
dlq:
  enabled: true
  sink:
    type: file
    path: /var/log/cdc/dlq.log
retry:
  max_attempts: 5
  backoff_ms: 200
`
	tmpFile := createTempFile(t, content)
	defer os.Remove(tmpFile)

	cfg, err := Load(tmpFile)
	if err != nil {
		t.Fatalf("Load() error = %v", err)
	}

	if cfg.DLQ == nil {
		t.Fatal("DLQ is nil")
	}
	if !cfg.DLQ.Enabled {
		t.Error("DLQ.Enabled should be true")
	}
	if cfg.DLQ.Sink.Type != "file" {
		t.Errorf("DLQ.Sink.Type = %q, want %q", cfg.DLQ.Sink.Type, "file")
	}
	if cfg.DLQ.Sink.Path != "/var/log/cdc/dlq.log" {
		t.Errorf("DLQ.Sink.Path = %q, want %q", cfg.DLQ.Sink.Path, "/var/log/cdc/dlq.log")
	}
	if cfg.Retry.MaxAttempts != 5 {
		t.Errorf("Retry.MaxAttempts = %d, want 5", cfg.Retry.MaxAttempts)
	}
	if cfg.Retry.BackoffMs != 200 {
		t.Errorf("Retry.BackoffMs = %d, want 200", cfg.Retry.BackoffMs)
	}
}

// Helper function to create a temporary file with content
func createTempFile(t *testing.T, content string) string {
	t.Helper()

	tmpDir := t.TempDir()
	tmpFile := filepath.Join(tmpDir, "config.yaml")

	err := os.WriteFile(tmpFile, []byte(content), 0o644)
	if err != nil {
		t.Fatalf("failed to create temp file: %v", err)
	}

	return tmpFile
}

// Helper function to check if a string contains a substring
func contains(s, substr string) bool {
	return len(s) >= len(substr) && (s == substr || len(s) > len(substr) && findSubstring(s, substr))
}

func findSubstring(s, substr string) bool {
	for i := 0; i <= len(s)-len(substr); i++ {
		if s[i:i+len(substr)] == substr {
			return true
		}
	}
	return false
}
