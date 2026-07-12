package e2e

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"os"
	"strings"
	"testing"
	"time"

	"iris/internal/pipeline"
	"iris/pkg/config"
	"iris/pkg/logger"
	"iris/pkg/observability"

	_ "github.com/jackc/pgx/v5/stdlib"
	"github.com/twmb/franz-go/pkg/kgo"
)

// getPostgresDSN returns the PostgreSQL DSN from environment or default, for
// ordinary (non-replication) SQL connections used to set up/verify test data.
func getPostgresDSN() string {
	if dsn := os.Getenv("TEST_POSTGRES_DSN"); dsn != "" {
		return dsn
	}
	return "postgres://iris:iris@localhost:54321/testdb"
}

// getReplicationDSN returns the DSN iris's postgres source should use, with
// replication=database appended so pgconn.Connect opens the connection in
// logical replication mode (required for CREATE_REPLICATION_SLOT/
// START_REPLICATION; a plain DSN causes those to be parsed as invalid SQL).
func getReplicationDSN() string {
	dsn := getPostgresDSN()
	sep := "?"
	if strings.Contains(dsn, "?") {
		sep = "&"
	}
	return dsn + sep + "replication=database"
}

// getKafkaBrokers returns the Kafka broker list from environment or default
func getKafkaBrokers() []string {
	if brokers := os.Getenv("TEST_KAFKA_BROKERS"); brokers != "" {
		return strings.Split(brokers, ",")
	}
	return []string{"localhost:19092"}
}

// connectToPostgres creates a test database connection
func connectToPostgres(t *testing.T, dsn string) *sql.DB {
	t.Helper()
	db, err := sql.Open("pgx", dsn)
	if err != nil {
		t.Fatalf("failed to open postgres: %v", err)
	}
	if err := db.Ping(); err != nil {
		t.Fatalf("failed to ping postgres: %v", err)
	}
	return db
}

// newKafkaConsumer creates a franz-go client that consumes the given topics
// from the beginning of the log under a fresh, unique consumer group. This
// lets each test independently replay the full topic history and filter for
// its own records, so tests don't need to coordinate offsets with each other.
func newKafkaConsumer(t *testing.T, brokers []string, topics ...string) *kgo.Client {
	t.Helper()
	group := fmt.Sprintf("iris-e2e-%d", time.Now().UnixNano())
	client, err := kgo.NewClient(
		kgo.SeedBrokers(brokers...),
		kgo.ConsumeTopics(topics...),
		kgo.ConsumerGroup(group),
		kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()),
	)
	if err != nil {
		t.Fatalf("failed to create kafka consumer: %v", err)
	}
	t.Cleanup(client.Close)
	return client
}

// pollKafkaUntil polls the consumer, decoding each record as a CDC event and
// invoking process on it, until process reports the awaited condition is
// satisfied or the timeout elapses. Returns false on timeout.
func pollKafkaUntil(t *testing.T, client *kgo.Client, timeout time.Duration, process func(ev map[string]any) bool) bool {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()

	satisfied := false
	for !satisfied {
		fetches := client.PollFetches(ctx)
		if ctx.Err() != nil {
			return false
		}
		for _, e := range fetches.Errors() {
			t.Logf("kafka fetch error (topic=%s partition=%d): %v", e.Topic, e.Partition, e.Err)
		}
		fetches.EachRecord(func(record *kgo.Record) {
			if satisfied {
				return
			}
			var ev map[string]any
			if err := json.Unmarshal(record.Value, &ev); err != nil {
				t.Logf("failed to unmarshal kafka record: %v", err)
				return
			}
			if process(ev) {
				satisfied = true
			}
		})
	}
	return true
}

// skipIfNoE2E skips the test if E2E_TEST is not set
func skipIfNoE2E(t *testing.T) {
	if os.Getenv("E2E_TEST") == "" {
		t.Skip("set E2E_TEST=1 to run E2E tests")
	}
}

// TestE2E_PostgresToKafka_Basic tests the basic CDC pipeline without transform
func TestE2E_PostgresToKafka_Basic(t *testing.T) {
	skipIfNoE2E(t)

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	dsn := getPostgresDSN()
	brokers := getKafkaBrokers()
	usersTopic := "cdc.users"

	// 1. Create pipeline config
	cfg := config.Config{
		Source: config.SourceConfig{
			Type:     "postgres",
			DSN:      getReplicationDSN(),
			Tables:   []string{"users", "orders"},
			SlotName: "iris_e2e_basic_slot",
		},
		Transform: nil, // No transform
		Sink: config.SinkConfig{
			Type:    "kafka",
			Brokers: brokers,
		},
	}

	// 2. Create pipeline
	p, err := pipeline.NewPipeline(cfg, logger.New("plain", "info"), observability.NewNoopMetrics())
	if err != nil {
		t.Fatalf("failed to create pipeline: %v", err)
	}
	defer p.Close()

	// 3. Start pipeline
	go func() {
		if err := p.Run(ctx); err != nil && err != context.Canceled {
			t.Logf("pipeline error: %v", err)
		}
	}()

	// Wait for replication to start
	time.Sleep(2 * time.Second)

	// 4. Insert test data
	db := connectToPostgres(t, dsn)
	defer db.Close()

	testName := fmt.Sprintf("e2e_test_%d", time.Now().Unix())
	_, err = db.ExecContext(ctx,
		"INSERT INTO users (name, email) VALUES ($1, $2)",
		testName, "e2e@test.com")
	if err != nil {
		t.Fatalf("failed to insert test data: %v", err)
	}

	// 5. Wait for event on the Kafka topic
	consumer := newKafkaConsumer(t, brokers, usersTopic)

	var eventData map[string]any
	if !pollKafkaUntil(t, consumer, 15*time.Second, func(ev map[string]any) bool {
		after, ok := ev["after"].(map[string]any)
		if !ok {
			return false
		}
		if after["name"] != testName {
			return false
		}
		eventData = ev
		return true
	}) {
		t.Fatal("timeout waiting for CDC event")
	}

	// 6. Verify event content
	if eventData["table"] != "users" {
		t.Errorf("expected table=users, got %v", eventData["table"])
	}
	if eventData["op"] != "create" {
		t.Errorf("expected op=create, got %v", eventData["op"])
	}
	after, ok := eventData["after"].(map[string]any)
	if !ok {
		t.Fatal("expected after to be a map")
	}
	if after["name"] != testName {
		t.Errorf("expected after.name=%s, got %v", testName, after["name"])
	}

	t.Log("E2E test passed: PostgreSQL -> Kafka CDC pipeline working")
}

// TestE2E_UpdateOperation tests UPDATE operation CDC
func TestE2E_UpdateOperation(t *testing.T) {
	skipIfNoE2E(t)

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	dsn := getPostgresDSN()
	brokers := getKafkaBrokers()
	usersTopic := "cdc.users"

	cfg := config.Config{
		Source: config.SourceConfig{
			Type:     "postgres",
			DSN:      getReplicationDSN(),
			Tables:   []string{"users"},
			SlotName: "iris_e2e_update_slot",
		},
		Sink: config.SinkConfig{
			Type:    "kafka",
			Brokers: brokers,
		},
	}

	p, err := pipeline.NewPipeline(cfg, logger.New("plain", "info"), observability.NewNoopMetrics())
	if err != nil {
		t.Fatalf("failed to create pipeline: %v", err)
	}
	defer p.Close()

	go func() {
		if err := p.Run(ctx); err != nil && err != context.Canceled {
			t.Logf("pipeline error: %v", err)
		}
	}()

	time.Sleep(2 * time.Second)

	db := connectToPostgres(t, dsn)
	defer db.Close()

	// First create a user
	testEmail := fmt.Sprintf("update_%d@test.com", time.Now().Unix())
	var userID int
	err = db.QueryRowContext(ctx,
		"INSERT INTO users (name, email) VALUES ($1, $2) RETURNING id",
		"Original Name", testEmail).Scan(&userID)
	if err != nil {
		t.Fatalf("failed to insert user: %v", err)
	}

	// Now update the user
	_, err = db.ExecContext(ctx,
		"UPDATE users SET name = $1 WHERE id = $2",
		"Updated Name", userID)
	if err != nil {
		t.Fatalf("failed to update user: %v", err)
	}

	consumer := newKafkaConsumer(t, brokers, usersTopic)

	// Wait for the update event matching this test's own email
	var eventData map[string]any
	if !pollKafkaUntil(t, consumer, 15*time.Second, func(ev map[string]any) bool {
		if ev["op"] != "update" {
			return false
		}
		after, ok := ev["after"].(map[string]any)
		if !ok || after["email"] != testEmail {
			return false
		}
		eventData = ev
		return true
	}) {
		t.Fatal("timeout waiting for update event")
	}

	// Verify update event has before and after
	if eventData["op"] != "update" {
		t.Errorf("expected op=update, got %v", eventData["op"])
	}
	before, ok := eventData["before"].(map[string]any)
	if !ok {
		t.Fatal("expected before to be a map")
	}
	after, ok := eventData["after"].(map[string]any)
	if !ok {
		t.Fatal("expected after to be a map")
	}
	if before["name"] != "Original Name" {
		t.Errorf("expected before.name='Original Name', got %v", before["name"])
	}
	if after["name"] != "Updated Name" {
		t.Errorf("expected after.name='Updated Name', got %v", after["name"])
	}

	t.Log("E2E update test passed")
}

// TestE2E_DeleteOperation tests DELETE operation CDC
func TestE2E_DeleteOperation(t *testing.T) {
	skipIfNoE2E(t)

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	dsn := getPostgresDSN()
	brokers := getKafkaBrokers()
	productsTopic := "cdc.products"

	cfg := config.Config{
		Source: config.SourceConfig{
			Type:     "postgres",
			DSN:      getReplicationDSN(),
			Tables:   []string{"products"},
			SlotName: "iris_e2e_delete_slot",
		},
		Sink: config.SinkConfig{
			Type:    "kafka",
			Brokers: brokers,
		},
	}

	p, err := pipeline.NewPipeline(cfg, logger.New("plain", "info"), observability.NewNoopMetrics())
	if err != nil {
		t.Fatalf("failed to create pipeline: %v", err)
	}
	defer p.Close()

	go func() {
		if err := p.Run(ctx); err != nil && err != context.Canceled {
			t.Logf("pipeline error: %v", err)
		}
	}()

	time.Sleep(2 * time.Second)

	db := connectToPostgres(t, dsn)
	defer db.Close()

	// Create a product to delete
	testSku := fmt.Sprintf("DELETE_TEST_%d", time.Now().Unix())
	var productID int
	err = db.QueryRowContext(ctx,
		"INSERT INTO products (name, price, inventory) VALUES ($1, $2, $3) RETURNING id",
		testSku, 9.99, 1).Scan(&productID)
	if err != nil {
		t.Fatalf("failed to insert product: %v", err)
	}

	// Delete the product
	_, err = db.ExecContext(ctx, "DELETE FROM products WHERE id = $1", productID)
	if err != nil {
		t.Fatalf("failed to delete product: %v", err)
	}

	consumer := newKafkaConsumer(t, brokers, productsTopic)

	// Wait for the delete event matching this test's own product
	var eventData map[string]any
	if !pollKafkaUntil(t, consumer, 15*time.Second, func(ev map[string]any) bool {
		if ev["op"] != "delete" {
			return false
		}
		before, ok := ev["before"].(map[string]any)
		if !ok || before["name"] != testSku {
			return false
		}
		eventData = ev
		return true
	}) {
		t.Fatal("timeout waiting for delete event")
	}

	// Verify delete event
	if eventData["op"] != "delete" {
		t.Errorf("expected op=delete, got %v", eventData["op"])
	}
	if eventData["table"] != "products" {
		t.Errorf("expected table=products, got %v", eventData["table"])
	}
	// Delete should have before but no after
	if _, ok := eventData["before"]; !ok {
		t.Error("expected before in delete event")
	}
	if _, ok := eventData["after"]; ok {
		t.Error("delete event should not have after")
	}

	t.Log("E2E delete test passed")
}

// TestE2E_MultipleTables tests CDC on multiple tables simultaneously
func TestE2E_MultipleTables(t *testing.T) {
	skipIfNoE2E(t)

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	dsn := getPostgresDSN()
	brokers := getKafkaBrokers()
	usersTopic := "cdc.users"
	productsTopic := "cdc.products"

	cfg := config.Config{
		Source: config.SourceConfig{
			Type:     "postgres",
			DSN:      getReplicationDSN(),
			Tables:   []string{"users", "products"},
			SlotName: "iris_e2e_multi_slot",
		},
		Sink: config.SinkConfig{
			Type:    "kafka",
			Brokers: brokers,
		},
	}

	p, err := pipeline.NewPipeline(cfg, logger.New("plain", "info"), observability.NewNoopMetrics())
	if err != nil {
		t.Fatalf("failed to create pipeline: %v", err)
	}
	defer p.Close()

	go func() {
		if err := p.Run(ctx); err != nil && err != context.Canceled {
			t.Logf("pipeline error: %v", err)
		}
	}()

	time.Sleep(2 * time.Second)

	db := connectToPostgres(t, dsn)
	defer db.Close()

	// Insert into both tables, tagging each row with a shared test ID so we
	// can distinguish these records from other tests sharing the same topics.
	testID := time.Now().UnixNano()
	userName := fmt.Sprintf("Multi User %d", testID)
	productName := fmt.Sprintf("Multi Product %d", testID)

	_, err = db.ExecContext(ctx,
		"INSERT INTO users (name, email) VALUES ($1, $2)",
		userName, fmt.Sprintf("multi%d@test.com", testID))
	if err != nil {
		t.Fatalf("failed to insert user: %v", err)
	}

	_, err = db.ExecContext(ctx,
		"INSERT INTO products (name, price, inventory) VALUES ($1, $2, $3)",
		productName, 49.99, 25)
	if err != nil {
		t.Fatalf("failed to insert product: %v", err)
	}

	consumer := newKafkaConsumer(t, brokers, usersTopic, productsTopic)

	// Wait for events from both tables carrying this test's marker
	tablesSeen := make(map[string]bool)
	if !pollKafkaUntil(t, consumer, 15*time.Second, func(ev map[string]any) bool {
		table, ok := ev["table"].(string)
		if !ok {
			return false
		}
		after, ok := ev["after"].(map[string]any)
		if !ok {
			return false
		}
		name, _ := after["name"].(string)
		switch table {
		case "users":
			if name == userName {
				tablesSeen["users"] = true
			}
		case "products":
			if name == productName {
				tablesSeen["products"] = true
			}
		}
		return tablesSeen["users"] && tablesSeen["products"]
	}) {
		t.Fatalf("timeout waiting for events from both tables, saw: %v", tablesSeen)
	}

	t.Logf("E2E multi-table test passed: saw events from %v", tablesSeen)
}
