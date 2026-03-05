package cdc

// Decoder converts RawEvent to unified Event
type Decoder interface {
	// Decode converts raw event to unified format
	Decode(raw RawEvent) (*Event, error)
}
