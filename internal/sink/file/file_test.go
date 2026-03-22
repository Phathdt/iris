package file

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"

	"iris/pkg/cdc"
)

func TestFileSink_Write(t *testing.T) {
	// Create temp file
	tmpDir := t.TempDir()
	tmpFile := filepath.Join(tmpDir, "test.jsonl")

	sink, err := NewFileSink(Config{
		Path: tmpFile,
	})
	if err != nil {
		t.Fatalf("NewFileSink() error = %v", err)
	}
	defer sink.Close()

	event := &cdc.Event{
		Source: "postgres",
		Table:  "users",
		Op:     cdc.EventTypeCreate,
		TS:     time.Unix(1234567890, 0),
		After:  map[string]interface{}{"id": 1, "name": "test"},
	}

	if err := sink.Write(context.Background(), event); err != nil {
		t.Fatalf("Write() error = %v", err)
	}

	// Verify file content
	data, err := os.ReadFile(tmpFile)
	if err != nil {
		t.Fatalf("ReadFile() error = %v", err)
	}

	if len(data) == 0 {
		t.Error("expected non-empty file")
	}

	// Should end with newline
	if data[len(data)-1] != '\n' {
		t.Error("expected newline at end of line")
	}
}

func TestFileSink_WriteNilEvent(t *testing.T) {
	tmpDir := t.TempDir()
	tmpFile := filepath.Join(tmpDir, "test.jsonl")

	sink, err := NewFileSink(Config{
		Path: tmpFile,
	})
	if err != nil {
		t.Fatalf("NewFileSink() error = %v", err)
	}
	defer sink.Close()

	err = sink.Write(context.Background(), nil)
	if err == nil {
		t.Error("Write() expected error for nil event")
	}
}

func TestFileSink_Rotation(t *testing.T) {
	tmpDir := t.TempDir()
	tmpFile := filepath.Join(tmpDir, "test.jsonl")

	sink, err := NewFileSink(Config{
		Path:     tmpFile,
		MaxSize:  50, // Small size to trigger rotation
		MaxFiles: 3,
	})
	if err != nil {
		t.Fatalf("NewFileSink() error = %v", err)
	}
	defer sink.Close()

	// Write multiple events to trigger rotation
	for i := 0; i < 5; i++ {
		event := &cdc.Event{
			Source: "postgres",
			Table:  "users",
			Op:     cdc.EventTypeCreate,
			TS:     time.Unix(int64(i), 0),
			After:  map[string]interface{}{"id": i, "name": "testuser"},
		}
		if err := sink.Write(context.Background(), event); err != nil {
			t.Fatalf("Write() error = %v", err)
		}
	}

	// Check rotated files exist
	for i := 1; i <= 3; i++ {
		rotated := rotatedPath(tmpFile, i)
		if _, err := os.Stat(rotated); os.IsNotExist(err) {
			t.Errorf("expected rotated file %s to exist", rotated)
		}
	}
}

func TestFileSink_Close(t *testing.T) {
	tmpDir := t.TempDir()
	tmpFile := filepath.Join(tmpDir, "test.jsonl")

	sink, err := NewFileSink(Config{
		Path: tmpFile,
	})
	if err != nil {
		t.Fatalf("NewFileSink() error = %v", err)
	}

	if err := sink.Close(); err != nil {
		t.Errorf("Close() error = %v", err)
	}
}

func TestFileSink_MissingPath(t *testing.T) {
	_, err := NewFileSink(Config{})
	if err == nil {
		t.Error("expected error for empty path")
	}
}
