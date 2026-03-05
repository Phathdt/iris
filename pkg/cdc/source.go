package cdc

import "context"

// RawEvent is source-specific raw data
type RawEvent struct {
	// Data holds the raw message from the source
	Data any

	// Meta holds source-specific metadata (WAL LSN, timestamp, etc.)
	Meta map[string]string
}

// Source emits raw CDC events from a database
type Source interface {
	// Start begins replication and returns raw event channel
	Start(ctx context.Context) (<-chan RawEvent, error)

	// Ack acknowledges processed offset
	Ack(offset Offset) error

	// Close stops replication and cleans up
	Close() error
}
