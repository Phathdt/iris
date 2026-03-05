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
  type: redis
  addr: localhost:6379
  password: secret
  db: 1
  key: cdc:events
  max_len: 1000
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
	if cfg.Sink.Type != "redis" {
		t.Errorf("Sink.Type = %q, want %q", cfg.Sink.Type, "redis")
	}
	if cfg.Sink.Addr != "localhost:6379" {
		t.Errorf("Sink.Addr = %q, want %q", cfg.Sink.Addr, "localhost:6379")
	}
	if cfg.Sink.Key != "cdc:events" {
		t.Errorf("Sink.Key = %q, want %q", cfg.Sink.Key, "cdc:events")
	}
	if cfg.Sink.MaxLen != 1000 {
		t.Errorf("Sink.MaxLen = %d, want 1000", cfg.Sink.MaxLen)
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
  type: redis
  addr: localhost:6379
  key: cdc:events
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
			Type: "redis",
			Addr: "localhost:6379",
			Key:  "cdc:events",
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
			Type: "redis",
			Addr: "localhost:6379",
			Key:  "cdc:events",
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

func TestValidate_MissingSinkAddress(t *testing.T) {
	cfg := &Config{
		Source: SourceConfig{
			Type:     "postgres",
			DSN:      "postgres://localhost:5432/testdb",
			SlotName: "test_slot",
		},
		Sink: SinkConfig{
			Type: "redis",
			Key:  "cdc:events",
		},
	}

	err := cfg.Validate()
	if err == nil {
		t.Fatal("Validate() expected error for missing sink address, got nil")
	}

	if err.Error() != "sink.addr is required" {
		t.Errorf("Validate() error = %v, want 'sink.addr is required'", err)
	}
}

func TestValidate_MissingSinkKey(t *testing.T) {
	cfg := &Config{
		Source: SourceConfig{
			Type:     "postgres",
			DSN:      "postgres://localhost:5432/testdb",
			SlotName: "test_slot",
		},
		Sink: SinkConfig{
			Type: "redis",
			Addr: "localhost:6379",
		},
	}

	err := cfg.Validate()
	if err == nil {
		t.Fatal("Validate() expected error for missing sink key, got nil")
	}

	if err.Error() != "sink.key is required" {
		t.Errorf("Validate() error = %v, want 'sink.key is required'", err)
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
			Type: "redis",
			Addr: "localhost:6379",
			Key:  "cdc:events",
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
			Type: "kafka",
			Addr: "localhost:9092",
		},
	}

	err := cfg.Validate()
	if err == nil {
		t.Fatal("Validate() expected error for unsupported sink type, got nil")
	}

	if err.Error() != "unsupported sink type: kafka" {
		t.Errorf("Validate() error = %v, want 'unsupported sink type: kafka'", err)
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
			Type: "redis",
			Addr: "localhost:6379",
			Key:  "cdc:events",
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
			Type: "redis",
			Addr: "localhost:6379",
			Key:  "cdc:events",
		},
	}

	err := cfg.Validate()
	if err == nil {
		t.Fatal("Validate() expected error for missing transform path, got nil")
	}

	if err.Error() != "transform.path is required when enabled" {
		t.Errorf("Validate() error = %v, want 'transform.path is required when enabled'", err)
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
			Type: "redis",
			Addr: "localhost:6379",
			Key:  "cdc:events",
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
			Type: "redis",
			Addr: "localhost:6379",
			Key:  "cdc:events",
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
			Type:     "redis",
			Addr:     "localhost:6379",
			Password: "secret",
			DB:       1,
			Key:      "cdc:events",
			MaxLen:   1000,
		},
	}

	err := cfg.Validate()
	if err != nil {
		t.Errorf("Validate() error = %v, want nil", err)
	}
}

func TestLoadAndValidate_Integration(t *testing.T) {
	content := `
source:
  type: postgres
  dsn: postgres://iris:iris@localhost:54321/testdb
  slot_name: iris_slot

sink:
  type: redis
  addr: localhost:6379
  key: iris:cdc
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
  type: redis
  addr: localhost:6379
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

// Helper function to create a temporary file with content
func createTempFile(t *testing.T, content string) string {
	t.Helper()

	tmpDir := t.TempDir()
	tmpFile := filepath.Join(tmpDir, "config.yaml")

	err := os.WriteFile(tmpFile, []byte(content), 0644)
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
