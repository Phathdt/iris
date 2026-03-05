package cdc

import (
	"encoding/json"
	"testing"
	"time"
)

func TestEventType_Constants(t *testing.T) {
	tests := []struct {
		name     string
		expected EventType
	}{
		{"create", EventTypeCreate},
		{"update", EventTypeUpdate},
		{"delete", EventTypeDelete},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if tt.expected == "" {
				t.Errorf("EventType %s should not be empty", tt.name)
			}
		})
	}
}

func TestEvent_MarshalJSON(t *testing.T) {
	now := time.Date(2026, 3, 5, 12, 0, 0, 0, time.UTC)

	tests := []struct {
		name     string
		event    *Event
		wantErr  bool
		validate func(t *testing.T, data []byte)
	}{
		{
			name: "create event",
			event: &Event{
				Source: "postgres",
				Table:  "users",
				Op:     EventTypeCreate,
				TS:     now,
				After:  map[string]any{"id": 1, "name": "alice"},
			},
			validate: func(t *testing.T, data []byte) {
				var result map[string]any
				if err := json.Unmarshal(data, &result); err != nil {
					t.Fatalf("failed to unmarshal: %v", err)
				}
				if result["source"] != "postgres" {
					t.Errorf("expected source=postgres, got %v", result["source"])
				}
				if result["table"] != "users" {
					t.Errorf("expected table=users, got %v", result["table"])
				}
				if result["op"] != "create" {
					t.Errorf("expected op=create, got %v", result["op"])
				}
				after, ok := result["after"].(map[string]any)
				if !ok {
					t.Fatal("expected after to be a map")
				}
				if after["id"] != float64(1) {
					t.Errorf("expected after.id=1, got %v", after["id"])
				}
			},
		},
		{
			name: "update event with before and after",
			event: &Event{
				Source: "postgres",
				Table:  "users",
				Op:     EventTypeUpdate,
				TS:     now,
				Before: map[string]any{"id": 1, "name": "alice"},
				After:  map[string]any{"id": 1, "name": "alice-updated"},
			},
			validate: func(t *testing.T, data []byte) {
				var result map[string]any
				if err := json.Unmarshal(data, &result); err != nil {
					t.Fatalf("failed to unmarshal: %v", err)
				}
				if result["op"] != "update" {
					t.Errorf("expected op=update, got %v", result["op"])
				}
				before, ok := result["before"].(map[string]any)
				if !ok {
					t.Fatal("expected before to be a map")
				}
				if before["name"] != "alice" {
					t.Errorf("expected before.name=alice, got %v", before["name"])
				}
			},
		},
		{
			name: "delete event with only before",
			event: &Event{
				Source: "postgres",
				Table:  "orders",
				Op:     EventTypeDelete,
				TS:     now,
				Before: map[string]any{"id": 99},
			},
			validate: func(t *testing.T, data []byte) {
				var result map[string]any
				if err := json.Unmarshal(data, &result); err != nil {
					t.Fatalf("failed to unmarshal: %v", err)
				}
				if result["op"] != "delete" {
					t.Errorf("expected op=delete, got %v", result["op"])
				}
				// Delete should not have "after" field
				if _, exists := result["after"]; exists {
					t.Error("delete event should not have after field")
				}
			},
		},
		{
			name: "event with raw field (not serialized)",
			event: &Event{
				Source: "postgres",
				Table:  "test",
				Op:     EventTypeCreate,
				TS:     now,
				After:  map[string]any{"value": 123},
				Raw:    map[string]string{"internal": "metadata"},
			},
			validate: func(t *testing.T, data []byte) {
				// Raw field should not appear in JSON
				if contains(data, "internal") {
					t.Error("Raw field should not be serialized")
				}
			},
		},
		{
			name:  "empty event",
			event: &Event{},
			validate: func(t *testing.T, data []byte) {
				var result map[string]any
				if err := json.Unmarshal(data, &result); err != nil {
					t.Fatalf("failed to unmarshal: %v", err)
				}
				// Empty event should still marshal successfully
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			data, err := json.Marshal(tt.event)
			if (err != nil) != tt.wantErr {
				t.Fatalf("MarshalJSON() error = %v, wantErr %v", err, tt.wantErr)
			}
			if tt.validate != nil {
				tt.validate(t, data)
			}
		})
	}
}

func TestEvent_UnmarshalJSON(t *testing.T) {
	now := time.Date(2026, 3, 5, 12, 0, 0, 0, time.UTC)

	tests := []struct {
		name     string
		input    string
		wantErr  bool
		validate func(t *testing.T, event *Event)
	}{
		{
			name: "create event from JSON",
			input: `{
				"source": "postgres",
				"table": "users",
				"op": "create",
				"ts": "` + now.Format(time.RFC3339) + `",
				"after": {"id": 1, "name": "alice"}
			}`,
			validate: func(t *testing.T, event *Event) {
				if event.Source != "postgres" {
					t.Errorf("expected source=postgres, got %v", event.Source)
				}
				if event.Table != "users" {
					t.Errorf("expected table=users, got %v", event.Table)
				}
				if event.Op != EventTypeCreate {
					t.Errorf("expected op=create, got %v", event.Op)
				}
				if event.After["name"] != "alice" {
					t.Errorf("expected after.name=alice, got %v", event.After["name"])
				}
			},
		},
		{
			name: "update event with before and after",
			input: `{
				"source": "postgres",
				"table": "users",
				"op": "update",
				"before": {"id": 1, "name": "old"},
				"after": {"id": 1, "name": "new"}
			}`,
			validate: func(t *testing.T, event *Event) {
				if event.Before["name"] != "old" {
					t.Errorf("expected before.name=old, got %v", event.Before["name"])
				}
				if event.After["name"] != "new" {
					t.Errorf("expected after.name=new, got %v", event.After["name"])
				}
			},
		},
		{
			name: "event without optional fields",
			input: `{
				"source": "postgres",
				"table": "test",
				"op": "create",
				"ts": "` + now.Format(time.RFC3339) + `"
			}`,
			validate: func(t *testing.T, event *Event) {
				if event.Before != nil {
					t.Error("before should be nil")
				}
				if event.After != nil {
					t.Error("after should be nil")
				}
			},
		},
		{
			name:    "invalid JSON",
			input:   `{invalid json}`,
			wantErr: true,
		},
		{
			name:    "empty JSON",
			input:   `{}`,
			wantErr: false,
			validate: func(t *testing.T, event *Event) {
				// Empty object should unmarshal to empty event
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var event Event
			err := json.Unmarshal([]byte(tt.input), &event)
			if (err != nil) != tt.wantErr {
				t.Fatalf("UnmarshalJSON() error = %v, wantErr %v", err, tt.wantErr)
			}
			if tt.validate != nil && err == nil {
				tt.validate(t, &event)
			}
		})
	}
}

func TestEvent_MarshalUnmarshal_RoundTrip(t *testing.T) {
	now := time.Date(2026, 3, 5, 12, 0, 0, 0, time.UTC)

	original := &Event{
		Source: "postgres",
		Table:  "users",
		Op:     EventTypeUpdate,
		TS:     now,
		Before: map[string]any{"id": 1, "name": "original"},
		After:  map[string]any{"id": 1, "name": "updated"},
		Raw:    map[string]string{"ignored": "value"},
	}

	// Marshal
	data, err := json.Marshal(original)
	if err != nil {
		t.Fatalf("Marshal failed: %v", err)
	}

	// Unmarshal
	var restored Event
	if err := json.Unmarshal(data, &restored); err != nil {
		t.Fatalf("Unmarshal failed: %v", err)
	}

	// Compare fields (Raw is not serialized, so skip it)
	if restored.Source != original.Source {
		t.Errorf("Source mismatch: %v != %v", restored.Source, original.Source)
	}
	if restored.Table != original.Table {
		t.Errorf("Table mismatch: %v != %v", restored.Table, original.Table)
	}
	if restored.Op != original.Op {
		t.Errorf("Op mismatch: %v != %v", restored.Op, original.Op)
	}
	if !restored.TS.Equal(original.TS) {
		t.Errorf("TS mismatch: %v != %v", restored.TS, original.TS)
	}
	if restored.Before["name"] != original.Before["name"] {
		t.Errorf("Before.name mismatch: %v != %v", restored.Before["name"], original.Before["name"])
	}
	if restored.After["name"] != original.After["name"] {
		t.Errorf("After.name mismatch: %v != %v", restored.After["name"], original.After["name"])
	}
}

// Helper function to check if byte slice contains a substring
func contains(data []byte, substr string) bool {
	return len(data) >= len(substr) && findSubstring(string(data), substr)
}

func findSubstring(s, substr string) bool {
	for i := 0; i <= len(s)-len(substr); i++ {
		if s[i:i+len(substr)] == substr {
			return true
		}
	}
	return false
}
