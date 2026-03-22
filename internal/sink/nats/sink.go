package nats

import (
	"context"
	"encoding/json"
	"fmt"

	"iris/pkg/cdc"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

var _ cdc.Sink = (*NATSSink)(nil)

// NATSSink implements cdc.Sink for NATS JetStream
type NATSSink struct {
	config Config
	nc     *nats.Conn
	js     jetstream.JetStream
}

// NewNATSSink creates a new NATS JetStream sink and tests the connection
func NewNATSSink(cfg Config) (*NATSSink, error) {
	if cfg.URL == "" {
		return nil, fmt.Errorf("nats url is required")
	}

	nc, err := nats.Connect(cfg.URL,
		nats.MaxReconnects(10),
	)
	if err != nil {
		return nil, fmt.Errorf("nats connect: %w", err)
	}

	// Verify connection is live (fail-fast on bad URL)
	if !nc.IsConnected() {
		nc.Close()
		return nil, fmt.Errorf("nats connect: not connected to %s", cfg.URL)
	}

	js, err := jetstream.New(nc)
	if err != nil {
		nc.Close()
		return nil, fmt.Errorf("nats jetstream: %w", err)
	}

	return &NATSSink{
		config: cfg,
		nc:     nc,
		js:     js,
	}, nil
}

// Write publishes a CDC event to a NATS JetStream subject
func (s *NATSSink) Write(ctx context.Context, event *cdc.Event) error {
	if event == nil {
		return fmt.Errorf("event is nil")
	}

	subject := s.config.GetSubject(event.Table)

	data, err := json.Marshal(event)
	if err != nil {
		return fmt.Errorf("encode event: %w", err)
	}

	if _, err := s.js.Publish(ctx, subject, data); err != nil {
		return fmt.Errorf("nats publish: %w", err)
	}

	return nil
}

// Close drains the NATS connection gracefully
func (s *NATSSink) Close() error {
	return s.nc.Drain()
}
