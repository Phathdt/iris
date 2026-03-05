package cdc

// Transform processes CDC events before they reach the encoder/sink.
// Implementations can filter, enrich, or route events.
//
// Process returns:
//   - (*Event, nil): transformed event to be processed
//   - (nil, nil): drop the event (filtered out)
//   - (nil, error): processing error
type Transform interface {
	// Process transforms a CDC event.
	// Returning nil event with nil error means the event should be dropped.
	Process(event *Event) (*Event, error)

	// Close cleans up resources (e.g., WASM runtime).
	Close() error
}
