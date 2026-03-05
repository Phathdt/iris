package encoder

import (
	"encoding/json"
	"testing"
	"time"

	"iris/pkg/cdc"
)

func TestJSONEncoder_New(t *testing.T) {
	enc := NewJSONEncoder()
	if enc == nil {
		t.Fatal("NewJSONEncoder() returned nil")
	}
}

func TestJSONEncoder_Encode(t *testing.T) {
	enc := NewJSONEncoder()
	now := time.Date(2026, 3, 5, 12, 0, 0, 0, time.UTC)

	tests := []struct {
		name     string
		event    *cdc.Event
		wantErr  bool
		validate func(t *testing.T, data []byte)
	}{
		{
			name: "create event",
			event: &cdc.Event{
				Source: "postgres",
				Table:  "users",
				Op:     cdc.EventTypeCreate,
				TS:     now,
				After:  map[string]any{"id": 1, "name": "alice"},
			},
			validate: func(t *testing.T, data []byte) {
				// Verify it's valid JSON
				var result map[string]any
				if err := json.Unmarshal(data, &result); err != nil {
					t.Fatalf("encoded data is not valid JSON: %v", err)
				}

				// Check key fields
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
					t.Fatal("after should be a map")
				}
				if after["id"] != float64(1) {
					t.Errorf("expected after.id=1, got %v", after["id"])
				}
			},
		},
		{
			name: "update event",
			event: &cdc.Event{
				Source: "postgres",
				Table:  "users",
				Op:     cdc.EventTypeUpdate,
				TS:     now,
				Before: map[string]any{"name": "old"},
				After:  map[string]any{"name": "new"},
			},
			validate: func(t *testing.T, data []byte) {
				var result map[string]any
				if err := json.Unmarshal(data, &result); err != nil {
					t.Fatalf("encoded data is not valid JSON: %v", err)
				}
				if result["op"] != "update" {
					t.Errorf("expected op=update, got %v", result["op"])
				}
			},
		},
		{
			name: "delete event",
			event: &cdc.Event{
				Source: "postgres",
				Table:  "orders",
				Op:     cdc.EventTypeDelete,
				TS:     now,
				Before: map[string]any{"id": 99},
			},
			validate: func(t *testing.T, data []byte) {
				var result map[string]any
				if err := json.Unmarshal(data, &result); err != nil {
					t.Fatalf("encoded data is not valid JSON: %v", err)
				}
				if result["op"] != "delete" {
					t.Errorf("expected op=delete, got %v", result["op"])
				}
			},
		},
		{
			name: "event with complex data",
			event: &cdc.Event{
				Source: "postgres",
				Table:  "products",
				Op:     cdc.EventTypeCreate,
				TS:     now,
				After: map[string]any{
					"id":          1,
					"name":        "widget",
					"price":       19.99,
					"in_stock":    true,
					"tags":        []string{"sale", "new"},
					"metadata":    map[string]any{"color": "red"},
					"description": nil,
				},
			},
			validate: func(t *testing.T, data []byte) {
				var result map[string]any
				if err := json.Unmarshal(data, &result); err != nil {
					t.Fatalf("encoded data is not valid JSON: %v", err)
				}
				after, ok := result["after"].(map[string]any)
				if !ok {
					t.Fatal("after should be a map")
				}
				if after["price"] != 19.99 {
					t.Errorf("expected price=19.99, got %v", after["price"])
				}
				if after["in_stock"] != true {
					t.Errorf("expected in_stock=true, got %v", after["in_stock"])
				}
			},
		},
		{
			name: "empty event",
			event: &cdc.Event{},
			validate: func(t *testing.T, data []byte) {
				var result map[string]any
				if err := json.Unmarshal(data, &result); err != nil {
					t.Fatalf("encoded data is not valid JSON: %v", err)
				}
			},
		},
		{
			name: "event with Raw field (should be omitted)",
			event: &cdc.Event{
				Source: "postgres",
				Table:  "test",
				Op:     cdc.EventTypeCreate,
				TS:     now,
				After:  map[string]any{"id": 1},
				Raw:    map[string]string{"secret": "value"},
			},
			validate: func(t *testing.T, data []byte) {
				// Raw field should not appear in JSON
				dataStr := string(data)
				if contains(dataStr, "secret") {
					t.Error("Raw field should not be serialized")
				}
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			data, err := enc.Encode(tt.event)
			if (err != nil) != tt.wantErr {
				t.Fatalf("Encode() error = %v, wantErr %v", err, tt.wantErr)
			}
			if tt.validate != nil && err == nil {
				tt.validate(t, data)
			}
		})
	}
}

func TestJSONEncoder_Encode_MultipleTimes(t *testing.T) {
	enc := NewJSONEncoder()
	event := &cdc.Event{
		Source: "postgres",
		Table:  "users",
		Op:     cdc.EventTypeCreate,
		TS:     time.Now(),
		After:  map[string]any{"id": 1},
	}

	// Encode multiple times should produce consistent results
	var results [][]byte
	for i := 0; i < 3; i++ {
		data, err := enc.Encode(event)
		if err != nil {
			t.Fatalf("Encode() iteration %d failed: %v", i, err)
		}
		results = append(results, data)
	}

	// All results should be valid JSON and have same structure
	for i, data := range results {
		var result map[string]any
		if err := json.Unmarshal(data, &result); err != nil {
			t.Errorf("result %d is not valid JSON: %v", i, err)
		}
		if result["source"] != "postgres" {
			t.Errorf("result %d has wrong source", i)
		}
	}
}

func BenchmarkJSONEncoder_Encode(b *testing.B) {
	enc := NewJSONEncoder()
	event := &cdc.Event{
		Source: "postgres",
		Table:  "users",
		Op:     cdc.EventTypeUpdate,
		TS:     time.Now(),
		Before: map[string]any{"id": 1, "name": "old"},
		After:  map[string]any{"id": 1, "name": "new"},
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, err := enc.Encode(event)
		if err != nil {
			b.Fatal(err)
		}
	}
}

// Helper function to check if string contains substring
func contains(s, substr string) bool {
	return len(s) >= len(substr) && (s == substr || len(s) > len(substr) &&
		(s[:len(substr)] == substr || s[len(s)-len(substr):] == substr ||
			findSubstring(s, substr)))
}

func findSubstring(s, substr string) bool {
	for i := 0; i <= len(s)-len(substr); i++ {
		if s[i:i+len(substr)] == substr {
			return true
		}
	}
	return false
}
