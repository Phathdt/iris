package cdc

// Encoder converts Event to output format
type Encoder interface {
	Encode(event *Event) ([]byte, error)
}
