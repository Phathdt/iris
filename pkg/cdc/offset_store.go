package cdc

// OffsetStore persists CDC stream offsets across restarts.
type OffsetStore interface {
	// Load returns the last persisted offset.
	// Returns a zero-value Offset if no offset has been stored.
	Load() (Offset, error)

	// Mark records the latest processed offset in memory.
	// Does NOT write to persistent storage (use Flush for that).
	Mark(offset Offset)

	// Flush writes the current offset to persistent storage.
	Flush() error

	// Close flushes any pending offset and releases resources.
	Close() error
}
