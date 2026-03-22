package stdout

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"time"

	"iris/pkg/cdc"
)

// Ensure StdoutSink implements cdc.Sink interface
var _ cdc.Sink = (*StdoutSink)(nil)

// StdoutSink writes events as JSON to stdout
type StdoutSink struct {
	prettyPrint bool
}

// NewStdoutSink creates a new stdout sink
func NewStdoutSink(prettyPrint bool) *StdoutSink {
	return &StdoutSink{
		prettyPrint: prettyPrint,
	}
}

// Write writes the event as JSON to stdout
func (s *StdoutSink) Write(ctx context.Context, event *cdc.Event) error {
	if event == nil {
		return fmt.Errorf("event is nil")
	}

	var data []byte
	var err error

	if s.prettyPrint {
		data, err = json.MarshalIndent(event, "", "  ")
	} else {
		data, err = json.Marshal(event)
	}

	if err != nil {
		return fmt.Errorf("marshal event: %w", err)
	}

	// Write with timestamp prefix
	ts := time.Now().Format(time.RFC3339)
	fmt.Fprintf(os.Stdout, "[%s] %s\n", ts, data)

	return nil
}

// Close is a no-op for stdout sink
func (s *StdoutSink) Close() error {
	return nil
}
