package kafka

import (
	"context"
	"encoding/json"
	"fmt"

	"iris/pkg/cdc"

	"github.com/twmb/franz-go/pkg/kgo"
)

var _ cdc.Sink = (*KafkaSink)(nil)

// KafkaSink implements cdc.Sink for Apache Kafka using franz-go
type KafkaSink struct {
	config Config
	client *kgo.Client
}

// NewKafkaSink creates a new Kafka sink and tests the connection
func NewKafkaSink(cfg Config) (*KafkaSink, error) {
	if len(cfg.Brokers) == 0 {
		return nil, fmt.Errorf("kafka brokers is required")
	}

	opts := []kgo.Opt{
		kgo.SeedBrokers(cfg.Brokers...),
	}

	client, err := kgo.NewClient(opts...)
	if err != nil {
		return nil, fmt.Errorf("kafka client: %w", err)
	}

	// Test connection by requesting metadata
	if err := client.Ping(context.Background()); err != nil {
		client.Close()
		return nil, fmt.Errorf("kafka ping: %w", err)
	}

	return &KafkaSink{
		config: cfg,
		client: client,
	}, nil
}

// Write produces a CDC event to Kafka synchronously (at-least-once)
func (s *KafkaSink) Write(ctx context.Context, event *cdc.Event) error {
	if event == nil {
		return fmt.Errorf("event is nil")
	}

	topic := s.config.GetTopic(event.Table)

	data, err := json.Marshal(event)
	if err != nil {
		return fmt.Errorf("encode event: %w", err)
	}

	record := &kgo.Record{
		Topic: topic,
		Key:   []byte(event.Table),
		Value: data,
	}

	if err := s.client.ProduceSync(ctx, record).FirstErr(); err != nil {
		return fmt.Errorf("kafka produce: %w", err)
	}

	return nil
}

// Close flushes pending records and closes the Kafka client
func (s *KafkaSink) Close() error {
	s.client.Close()
	return nil
}
