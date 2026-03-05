package cdc

import "context"

// Sink receives and stores encoded events
type Sink interface {
	// Write stores encoded event data
	Write(ctx context.Context, data []byte) error

	// Close flushes buffers and closes connection
	Close() error
}
