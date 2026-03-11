package dlq

import (
	"context"
	"encoding/json"
	"errors"
	"testing"
	"time"

	"iris/pkg/cdc"
)

type mockSink struct {
	writes []*cdc.Event
	err    error
	closed bool
}

func (m *mockSink) Write(ctx context.Context, event *cdc.Event) error {
	if m.err != nil {
		return m.err
	}
	m.writes = append(m.writes, event)
	return nil
}

func (m *mockSink) Close() error {
	m.closed = true
	return nil
}

func TestDLQ_Send(t *testing.T) {
	sink := &mockSink{writes: []*cdc.Event{}}
	d := NewDLQ(sink)

	event := &cdc.Event{
		Source: "postgres",
		Table:  "users",
		Op:     cdc.EventTypeCreate,
		TS:     time.Now(),
		After:  map[string]any{"id": 1, "name": "test"},
	}

	err := d.Send(context.Background(), event, errors.New("transform failed"), "transform", 3)
	if err != nil {
		t.Fatalf("Send() error = %v", err)
	}

	if len(sink.writes) != 1 {
		t.Fatalf("expected 1 write, got %d", len(sink.writes))
	}

	dlqEvent := sink.writes[0]
	if dlqEvent.Table != "dlq" {
		t.Errorf("Table = %q, want %q", dlqEvent.Table, "dlq")
	}
	if string(dlqEvent.Op) != "dlq" {
		t.Errorf("Op = %q, want %q", dlqEvent.Op, "dlq")
	}
	if dlqEvent.Source != "postgres" {
		t.Errorf("Source = %q, want %q", dlqEvent.Source, "postgres")
	}

	// Verify FailedEvent is in After field
	afterJSON, _ := json.Marshal(dlqEvent.After)
	var failed FailedEvent
	if err := json.Unmarshal(afterJSON, &failed); err != nil {
		t.Fatalf("failed to unmarshal After as FailedEvent: %v", err)
	}
	if failed.Error != "transform failed" {
		t.Errorf("FailedEvent.Error = %q, want %q", failed.Error, "transform failed")
	}
	if failed.Stage != "transform" {
		t.Errorf("FailedEvent.Stage = %q, want %q", failed.Stage, "transform")
	}
	if failed.Attempts != 3 {
		t.Errorf("FailedEvent.Attempts = %d, want 3", failed.Attempts)
	}
	if failed.FailedAt.IsZero() {
		t.Error("FailedEvent.FailedAt should not be zero")
	}
}

func TestDLQ_Send_SinkError(t *testing.T) {
	sink := &mockSink{err: errors.New("sink unavailable")}
	d := NewDLQ(sink)

	event := &cdc.Event{
		Source: "postgres",
		Table:  "users",
		Op:     cdc.EventTypeCreate,
		After:  map[string]any{"id": 1},
	}

	err := d.Send(context.Background(), event, errors.New("original error"), "sink", 3)
	if err == nil {
		t.Fatal("Send() expected error when DLQ sink fails, got nil")
	}
	if err.Error() != "sink unavailable" {
		t.Errorf("Send() error = %q, want %q", err.Error(), "sink unavailable")
	}
}

func TestDLQ_Close(t *testing.T) {
	sink := &mockSink{}
	d := NewDLQ(sink)

	err := d.Close()
	if err != nil {
		t.Fatalf("Close() error = %v", err)
	}
	if !sink.closed {
		t.Error("Close() did not close inner sink")
	}
}
