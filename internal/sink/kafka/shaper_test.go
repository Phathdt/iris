package kafka

import (
	"bytes"
	"testing"

	"iris/pkg/cdc"
)

func defaultTopic(table string) string { return "cdc." + table }

func TestShapeAllColumnsPresent(t *testing.T) {
	cfg := &OutboxConfig{KeyField: "aggregate_id", RouteField: "event_type", PayloadField: "payload"}
	event := &cdc.Event{
		Table: "outbox_events",
		After: map[string]any{
			"aggregate_id": "abc-123",
			"event_type":   "transfer.requested",
			"payload":      map[string]any{"transferId": "abc-123"},
		},
	}

	key, topic, value, err := shape(event, cfg, defaultTopic)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if string(key) != "abc-123" {
		t.Errorf("key = %q, want %q", key, "abc-123")
	}
	if topic != "transfer.requested" {
		t.Errorf("topic = %q, want %q", topic, "transfer.requested")
	}
	if want := `{"transferId":"abc-123"}`; string(value) != want {
		t.Errorf("value = %q, want %q", value, want)
	}
}

func TestShapeMissingKeyFallsBackToTable(t *testing.T) {
	cfg := &OutboxConfig{KeyField: "aggregate_id", RouteField: "event_type", PayloadField: "payload"}
	event := &cdc.Event{
		Table: "outbox_events",
		After: map[string]any{
			"event_type": "transfer.requested",
			"payload":    map[string]any{"transferId": "x"},
		},
	}

	key, topic, _, err := shape(event, cfg, defaultTopic)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if string(key) != "outbox_events" {
		t.Errorf("key = %q, want fallback %q", key, "outbox_events")
	}
	// route + payload still resolve from columns
	if topic != "transfer.requested" {
		t.Errorf("topic = %q, want %q", topic, "transfer.requested")
	}
}

func TestShapeMissingRouteFallsBackToDefaultTopic(t *testing.T) {
	cfg := &OutboxConfig{KeyField: "aggregate_id", RouteField: "event_type", PayloadField: "payload"}
	event := &cdc.Event{
		Table: "outbox_events",
		After: map[string]any{
			"aggregate_id": "abc-123",
			"payload":      map[string]any{"transferId": "x"},
		},
	}

	_, topic, _, err := shape(event, cfg, defaultTopic)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if topic != "cdc.outbox_events" {
		t.Errorf("topic = %q, want fallback %q", topic, "cdc.outbox_events")
	}
}

func TestShapeMissingPayloadFallsBackToEnvelope(t *testing.T) {
	cfg := &OutboxConfig{KeyField: "aggregate_id", RouteField: "event_type", PayloadField: "payload"}
	event := &cdc.Event{
		Source: "postgres",
		Table:  "outbox_events",
		Op:     cdc.EventTypeCreate,
		After: map[string]any{
			"aggregate_id": "abc-123",
			"event_type":   "transfer.requested",
		},
	}

	_, _, value, err := shape(event, cfg, defaultTopic)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	// Full envelope contains the source field; raw payload would not.
	if !bytes.Contains(value, []byte(`"source":"postgres"`)) {
		t.Errorf("value = %q, want full CDC envelope", value)
	}
}

func TestShapeNilAfterFallsBackEverything(t *testing.T) {
	cfg := &OutboxConfig{KeyField: "aggregate_id", RouteField: "event_type", PayloadField: "payload"}
	event := &cdc.Event{Table: "outbox_events", After: nil}

	key, topic, value, err := shape(event, cfg, defaultTopic)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if string(key) != "outbox_events" {
		t.Errorf("key = %q, want %q", key, "outbox_events")
	}
	if topic != "cdc.outbox_events" {
		t.Errorf("topic = %q, want %q", topic, "cdc.outbox_events")
	}
	if !bytes.Contains(value, []byte(`"table":"outbox_events"`)) {
		t.Errorf("value = %q, want full envelope", value)
	}
}

func TestShapeNullColumnValueFallsBack(t *testing.T) {
	cfg := &OutboxConfig{KeyField: "aggregate_id", RouteField: "event_type", PayloadField: "payload"}
	event := &cdc.Event{
		Table: "outbox_events",
		After: map[string]any{
			"aggregate_id": nil, // explicit SQL NULL
			"event_type":   "transfer.requested",
			"payload":      map[string]any{"transferId": "x"},
		},
	}

	key, _, _, err := shape(event, cfg, defaultTopic)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if string(key) != "outbox_events" {
		t.Errorf("null key column: key = %q, want fallback %q", key, "outbox_events")
	}
}

func TestShapeSameAggregateSameKey(t *testing.T) {
	cfg := &OutboxConfig{KeyField: "aggregate_id", RouteField: "event_type", PayloadField: "payload"}
	mk := func(evType string) []byte {
		e := &cdc.Event{
			Table: "outbox_events",
			After: map[string]any{"aggregate_id": "same-id", "event_type": evType, "payload": map[string]any{}},
		}
		key, _, _, err := shape(e, cfg, defaultTopic)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		return key
	}
	// Ordering invariant: two events for one aggregate must share the key,
	// so they land on the same partition regardless of event type.
	if !bytes.Equal(mk("transfer.requested"), mk("transfer.completed")) {
		t.Error("same aggregate_id produced different keys — ordering not preserved")
	}
}

func TestShapeEmptyRouteValueFallsBack(t *testing.T) {
	cfg := &OutboxConfig{KeyField: "aggregate_id", RouteField: "event_type", PayloadField: "payload"}
	event := &cdc.Event{
		Table: "outbox_events",
		After: map[string]any{
			"aggregate_id": "abc-123",
			"event_type":   "", // present but empty → invalid topic if used verbatim
			"payload":      map[string]any{"transferId": "abc-123"},
		},
	}

	_, topic, _, err := shape(event, cfg, defaultTopic)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if topic != "cdc.outbox_events" {
		t.Errorf("empty route value: topic = %q, want fallback %q", topic, "cdc.outbox_events")
	}
}

func TestShapeNonStringKeyCoerced(t *testing.T) {
	cfg := &OutboxConfig{KeyField: "aggregate_id"}
	event := &cdc.Event{
		Table: "outbox_events",
		After: map[string]any{"aggregate_id": int64(42)},
	}

	key, _, _, err := shape(event, cfg, defaultTopic)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if string(key) != "42" {
		t.Errorf("key = %q, want %q", key, "42")
	}
}
