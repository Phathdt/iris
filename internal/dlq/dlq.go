package dlq

import (
	"context"
	"encoding/json"
	"time"

	"iris/pkg/cdc"
)

// FailedEvent wraps an original event with error metadata for DLQ storage
type FailedEvent struct {
	Event    *cdc.Event `json:"event"`
	Error    string     `json:"error"`
	Stage    string     `json:"stage"` // "transform" or "sink"
	Attempts int        `json:"attempts"`
	FailedAt time.Time  `json:"failed_at"`
}

// DLQ routes failed events to a dead letter queue sink
type DLQ struct {
	sink cdc.Sink
}

// NewDLQ creates a DLQ backed by the given sink
func NewDLQ(sink cdc.Sink) *DLQ {
	return &DLQ{sink: sink}
}

// Send wraps the failed event with error metadata and writes it to the DLQ sink.
// The original event is serialized as a FailedEvent in the After field of a synthetic event.
func (d *DLQ) Send(ctx context.Context, event *cdc.Event, err error, stage string, attempts int) error {
	failed := &FailedEvent{
		Event:    event,
		Error:    err.Error(),
		Stage:    stage,
		Attempts: attempts,
		FailedAt: time.Now(),
	}

	// Serialize FailedEvent into a map for the synthetic event's After field
	data, marshalErr := json.Marshal(failed)
	if marshalErr != nil {
		return marshalErr
	}
	var afterMap map[string]any
	if marshalErr = json.Unmarshal(data, &afterMap); marshalErr != nil {
		return marshalErr
	}

	dlqEvent := &cdc.Event{
		Source: event.Source,
		Table:  "dlq",
		Op:     "dlq",
		TS:     time.Now(),
		After:  afterMap,
	}

	return d.sink.Write(ctx, dlqEvent)
}

// Close closes the underlying DLQ sink
func (d *DLQ) Close() error {
	return d.sink.Close()
}
