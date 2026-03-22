//go:build integration

package nats

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"iris/pkg/cdc"

	"github.com/nats-io/nats.go/jetstream"
	natscontainer "github.com/testcontainers/testcontainers-go/modules/nats"
)

func TestIntegration_NATSSink_WriteAndConsume(t *testing.T) {
	ctx := context.Background()

	// Start NATS container with JetStream enabled
	container, err := natscontainer.Run(ctx, "nats:2.10")
	if err != nil {
		t.Fatalf("failed to start NATS container: %v", err)
	}
	t.Cleanup(func() {
		_ = container.Terminate(ctx)
	})

	// Get connection URL
	url, err := container.ConnectionString(ctx)
	if err != nil {
		t.Fatalf("failed to get connection string: %v", err)
	}

	// Create sink
	sink, err := NewNATSSink(Config{
		URL: url,
		TableSubjectMap: map[string]string{
			"users": "test.users",
		},
	})
	if err != nil {
		t.Fatalf("NewNATSSink() error = %v", err)
	}
	defer sink.Close()

	// Create a stream to capture published messages
	stream, err := sink.js.CreateStream(ctx, jetstream.StreamConfig{
		Name:     "TEST",
		Subjects: []string{"test.>"},
	})
	if err != nil {
		t.Fatalf("create stream: %v", err)
	}

	// Write event
	event := &cdc.Event{
		Source: "postgres",
		Table:  "users",
		Op:     cdc.EventTypeCreate,
		TS:     time.Now(),
		After:  map[string]any{"id": 1, "name": "alice"},
	}

	if err := sink.Write(ctx, event); err != nil {
		t.Fatalf("Write() error = %v", err)
	}

	// Consume and verify
	consumer, err := stream.CreateConsumer(ctx, jetstream.ConsumerConfig{
		AckPolicy: jetstream.AckExplicitPolicy,
	})
	if err != nil {
		t.Fatalf("create consumer: %v", err)
	}

	msg, err := consumer.Next(jetstream.FetchMaxWait(5 * time.Second))
	if err != nil {
		t.Fatalf("consume message: %v", err)
	}

	var decoded cdc.Event
	if err := json.Unmarshal(msg.Data(), &decoded); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}

	if decoded.Table != "users" {
		t.Errorf("Table = %s, want users", decoded.Table)
	}
	if decoded.Op != cdc.EventTypeCreate {
		t.Errorf("Op = %s, want create", decoded.Op)
	}

	after, ok := decoded.After["name"]
	if !ok || after != "alice" {
		t.Errorf("After[name] = %v, want alice", after)
	}
}

func TestIntegration_NATSSink_DefaultSubject(t *testing.T) {
	ctx := context.Background()

	container, err := natscontainer.Run(ctx, "nats:2.10")
	if err != nil {
		t.Fatalf("failed to start NATS container: %v", err)
	}
	t.Cleanup(func() {
		_ = container.Terminate(ctx)
	})

	url, err := container.ConnectionString(ctx)
	if err != nil {
		t.Fatalf("failed to get connection string: %v", err)
	}

	// No TableSubjectMap — should use default "cdc.{table}"
	sink, err := NewNATSSink(Config{URL: url})
	if err != nil {
		t.Fatalf("NewNATSSink() error = %v", err)
	}
	defer sink.Close()

	// Create stream capturing default subjects
	stream, err := sink.js.CreateStream(ctx, jetstream.StreamConfig{
		Name:     "CDC",
		Subjects: []string{"cdc.>"},
	})
	if err != nil {
		t.Fatalf("create stream: %v", err)
	}

	event := &cdc.Event{
		Source: "postgres",
		Table:  "orders",
		Op:     cdc.EventTypeUpdate,
		After:  map[string]any{"id": 42},
	}

	if err := sink.Write(ctx, event); err != nil {
		t.Fatalf("Write() error = %v", err)
	}

	consumer, err := stream.CreateConsumer(ctx, jetstream.ConsumerConfig{
		AckPolicy:     jetstream.AckExplicitPolicy,
		FilterSubject: "cdc.orders",
	})
	if err != nil {
		t.Fatalf("create consumer: %v", err)
	}

	msg, err := consumer.Next(jetstream.FetchMaxWait(5 * time.Second))
	if err != nil {
		t.Fatalf("consume message: %v", err)
	}

	var decoded cdc.Event
	if err := json.Unmarshal(msg.Data(), &decoded); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}

	if decoded.Table != "orders" {
		t.Errorf("Table = %s, want orders", decoded.Table)
	}
}

func TestIntegration_NATSSink_NilEvent(t *testing.T) {
	ctx := context.Background()

	container, err := natscontainer.Run(ctx, "nats:2.10")
	if err != nil {
		t.Fatalf("failed to start NATS container: %v", err)
	}
	t.Cleanup(func() {
		_ = container.Terminate(ctx)
	})

	url, err := container.ConnectionString(ctx)
	if err != nil {
		t.Fatalf("failed to get connection string: %v", err)
	}

	sink, err := NewNATSSink(Config{URL: url})
	if err != nil {
		t.Fatalf("NewNATSSink() error = %v", err)
	}
	defer sink.Close()

	if err := sink.Write(ctx, nil); err == nil {
		t.Fatal("expected error for nil event")
	}
}
