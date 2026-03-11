package cdc

import (
	"encoding/json"
	"time"
)

// EventType represents the type of database operation
type EventType string

const (
	EventTypeCreate EventType = "create"
	EventTypeUpdate EventType = "update"
	EventTypeDelete EventType = "delete"
)

// Event represents a CDC event from any source
type Event struct {
	// Source identifies the origin system
	Source string `json:"source"`

	// Table is the source table name
	Table string `json:"table"`

	// Op is the operation type (create/update/delete)
	Op EventType `json:"op"`

	// TS is the event timestamp
	TS time.Time `json:"ts"`

	// Before contains the row before update/delete (nil for create)
	Before map[string]any `json:"before,omitempty"`

	// After contains the row after create/update (nil for delete)
	After map[string]any `json:"after,omitempty"`

	// Raw holds source-specific metadata (WAL LSN, binlog position, etc.)
	Raw any `json:"-"`

	// Offset is the source position of this event (not serialized to sinks).
	Offset *Offset `json:"-"`
}

// MarshalJSON implements custom JSON marshaling for Event
func (e *Event) MarshalJSON() ([]byte, error) {
	type Alias Event
	return json.Marshal(&struct {
		*Alias
	}{
		Alias: (*Alias)(e),
	})
}

// UnmarshalJSON implements custom JSON unmarshaling for Event
func (e *Event) UnmarshalJSON(data []byte) error {
	type Alias Event
	aux := &struct {
		*Alias
	}{
		Alias: (*Alias)(e),
	}
	if err := json.Unmarshal(data, aux); err != nil {
		return err
	}
	return nil
}
