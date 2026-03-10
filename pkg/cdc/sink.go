package cdc

import "context"

// Sink receives and stores events
type Sink interface {
	// Write stores an event
	Write(ctx context.Context, event *Event) error

	// Close flushes buffers and closes connection
	Close() error
}
