package stdout

import (
	"bytes"
	"context"
	"encoding/json"
	"os"
	"strings"
	"testing"
	"time"

	"iris/pkg/cdc"
)

func TestStdoutSink_Write(t *testing.T) {
	// Capture stdout
	oldStdout := os.Stdout
	r, w, _ := os.Pipe()
	os.Stdout = w

	sink := NewStdoutSink(false)
	event := &cdc.Event{
		Source: "postgres",
		Table:  "users",
		Op:     cdc.EventTypeCreate,
		TS:     time.Unix(1234567890, 0),
		After:  map[string]interface{}{"id": 1, "name": "test"},
	}

	err := sink.Write(context.Background(), event)
	if err != nil {
		t.Fatalf("Write() error = %v", err)
	}

	w.Close()

	// Read captured output
	var buf bytes.Buffer
	buf.ReadFrom(r)
	os.Stdout = oldStdout

	var captured map[string]interface{}
	// Skip timestamp prefix [YYYY-MM-DDTHH:MM:SSZ]
	output := buf.String()
	// Find the JSON part (after timestamp prefix)
	jsonPart := strings.TrimPrefix(output, output[:27])
	if err := json.Unmarshal([]byte(jsonPart), &captured); err != nil {
		t.Fatalf("Failed to parse output: %v, output: %s", err, output)
	}

	if captured["table"] != "users" {
		t.Errorf("expected table 'users', got %v", captured["table"])
	}
}

func TestStdoutSink_WriteNilEvent(t *testing.T) {
	sink := NewStdoutSink(false)
	err := sink.Write(context.Background(), nil)
	if err == nil {
		t.Error("Write() expected error for nil event")
	}
}

func TestStdoutSink_Close(t *testing.T) {
	sink := NewStdoutSink(false)
	if err := sink.Close(); err != nil {
		t.Errorf("Close() error = %v", err)
	}
}

func TestStdoutSink_PrettyPrint(t *testing.T) {
	// Capture stdout
	oldStdout := os.Stdout
	r, w, _ := os.Pipe()
	os.Stdout = w

	sink := NewStdoutSink(true)
	event := &cdc.Event{
		Source: "postgres",
		Table:  "users",
		Op:     cdc.EventTypeCreate,
		TS:     time.Unix(1234567890, 0),
		After:  map[string]interface{}{"id": 1, "name": "test"},
	}

	err := sink.Write(context.Background(), event)
	if err != nil {
		t.Fatalf("Write() error = %v", err)
	}

	w.Close()

	// Read captured output
	var buf bytes.Buffer
	buf.ReadFrom(r)
	os.Stdout = oldStdout

	// Pretty printed should have newlines
	output := buf.String()
	if !strings.Contains(output, "\n") {
		t.Error("Expected pretty printed output with newlines")
	}
}
