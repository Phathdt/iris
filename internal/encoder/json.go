package encoder

import (
	"encoding/json"

	"iris/pkg/cdc"
)

// JSONEncoder implements Encoder interface using standard library JSON
type JSONEncoder struct{}

// NewJSONEncoder creates a new JSON encoder
func NewJSONEncoder() *JSONEncoder {
	return &JSONEncoder{}
}

// Encode converts an Event to JSON bytes
func (e *JSONEncoder) Encode(event *cdc.Event) ([]byte, error) {
	return json.Marshal(event)
}
