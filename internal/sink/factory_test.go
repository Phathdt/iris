package sink

import (
	"context"
	"testing"

	"iris/pkg/cdc"
)

func TestNewFactory_RegisteredTypes(t *testing.T) {
	factory := NewFactory()

	// Verify built-in types are registered
	for _, sinkType := range []string{"kafka", "stdout", "file"} {
		_, ok := factory.builders[sinkType]
		if !ok {
			t.Errorf("expected %q to be registered", sinkType)
		}
	}
}

func TestSinkRegistry_CreateSink_UnsupportedType(t *testing.T) {
	factory := NewFactory()

	_, err := factory.CreateSink(Config{Type: "unknown"})
	if err == nil {
		t.Fatal("expected error for unsupported sink type")
	}
}

func TestSinkRegistry_CreateSink_EmptyType(t *testing.T) {
	factory := NewFactory()

	_, err := factory.CreateSink(Config{Type: ""})
	if err == nil {
		t.Fatal("expected error for empty sink type")
	}
}

func TestSinkRegistry_Register_Custom(t *testing.T) {
	factory := NewFactory()

	// Register a custom sink builder
	factory.Register("mock", func(cfg Config) (cdc.Sink, error) {
		return &mockSink{}, nil
	})

	sink, err := factory.CreateSink(Config{Type: "mock"})
	if err != nil {
		t.Fatalf("CreateSink() error = %v", err)
	}
	if sink == nil {
		t.Fatal("expected non-nil sink")
	}
}

// mockSink is a minimal Sink implementation for testing
type mockSink struct{}

func (m *mockSink) Write(ctx context.Context, event *cdc.Event) error { return nil }
func (m *mockSink) Close() error                                      { return nil }
