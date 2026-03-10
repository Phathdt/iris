package e2e

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"os"
	"testing"
	"time"

	"iris/internal/pipeline"
	"iris/pkg/config"
	"iris/pkg/logger"

	_ "github.com/jackc/pgx/v5/stdlib"
	"github.com/redis/go-redis/v9"
)

// getPostgresDSN returns the PostgreSQL DSN from environment or default
func getPostgresDSN() string {
	if dsn := os.Getenv("TEST_POSTGRES_DSN"); dsn != "" {
		return dsn
	}
	return "postgres://iris:iris@localhost:54321/testdb"
}

// getRedisAddr returns the Redis address from environment or default
func getRedisAddr() string {
	if addr := os.Getenv("TEST_REDIS_ADDR"); addr != "" {
		return addr
	}
	return "localhost:63791"
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

// connectToRedis creates a test Redis client
func connectToRedis(t *testing.T, addr string) *redis.Client {
	t.Helper()
	client := redis.NewClient(&redis.Options{
		Addr: addr,
	})
	if err := client.Ping(context.Background()).Err(); err != nil {
		t.Fatalf("failed to ping redis: %v", err)
	}
	return client
}

// cleanupRedis clears the test key (works for both list and stream)
func cleanupRedis(t *testing.T, client *redis.Client, key string) {
	t.Helper()
	client.Del(context.Background(), key)
}

// skipIfNoE2E skips the test if E2E_TEST is not set
func skipIfNoE2E(t *testing.T) {
	if os.Getenv("E2E_TEST") == "" {
		t.Skip("set E2E_TEST=1 to run E2E tests")
	}
}

// TestE2E_PostgresToRedis_Basic tests the basic CDC pipeline without transform
func TestE2E_PostgresToRedis_Basic(t *testing.T) {
	skipIfNoE2E(t)

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	dsn := getPostgresDSN()
	redisAddr := getRedisAddr()
	testKey := "e2e:cdc:events:basic"

	// 1. Create pipeline config
	cfg := config.Config{
		Source: config.SourceConfig{
			Type:     "postgres",
			DSN:      dsn,
			Tables:   []string{"users", "orders"},
			SlotName: "iris_e2e_basic_slot",
		},
		Transform: nil, // No transform
		Sink: config.SinkConfig{
			Type: "redis",
			Addr: redisAddr,
			Key:  testKey,
		},
	}

	// 2. Create pipeline
	p, err := pipeline.NewPipeline(cfg, logger.New("plain", "info"))
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

	// 5. Wait for event in Redis
	redisClient := connectToRedis(t, redisAddr)
	defer cleanupRedis(t, redisClient, testKey)

	var eventData map[string]any
	if !eventuallyHelper(t, func() bool {
		result, err := redisClient.LRange(ctx, testKey, 0, -1).Result()
		if err != nil || len(result) == 0 {
			return false
		}
		if err := json.Unmarshal([]byte(result[0]), &eventData); err != nil {
			return false
		}
		return true
	}, 15*time.Second, 500*time.Millisecond, "timeout waiting for CDC event") {
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

	t.Log("E2E test passed: PostgreSQL -> Redis CDC pipeline working")
}

// TestE2E_UpdateOperation tests UPDATE operation CDC
func TestE2E_UpdateOperation(t *testing.T) {
	skipIfNoE2E(t)

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	dsn := getPostgresDSN()
	redisAddr := getRedisAddr()
	testKey := "e2e:cdc:events:update"

	cfg := config.Config{
		Source: config.SourceConfig{
			Type:     "postgres",
			DSN:      dsn,
			Tables:   []string{"users"},
			SlotName: "iris_e2e_update_slot",
		},
		Sink: config.SinkConfig{
			Type: "redis",
			Addr: redisAddr,
			Key:  testKey,
		},
	}

	p, err := pipeline.NewPipeline(cfg, logger.New("plain", "info"))
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

	redisClient := connectToRedis(t, redisAddr)
	defer cleanupRedis(t, redisClient, testKey)

	// Wait for update event
	var eventData map[string]any
	if !eventuallyHelper(t, func() bool {
		result, err := redisClient.LRange(ctx, testKey, 0, -1).Result()
		if err != nil || len(result) == 0 {
			return false
		}
		// Find the update event (may need to skip create event)
		for _, r := range result {
			var ev map[string]any
			if err := json.Unmarshal([]byte(r), &ev); err != nil {
				continue
			}
			if ev["op"] == "update" {
				eventData = ev
				return true
			}
		}
		return false
	}, 15*time.Second, 500*time.Millisecond, "timeout waiting for update event") {
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
	redisAddr := getRedisAddr()
	testKey := "e2e:cdc:events:delete"

	cfg := config.Config{
		Source: config.SourceConfig{
			Type:     "postgres",
			DSN:      dsn,
			Tables:   []string{"products"},
			SlotName: "iris_e2e_delete_slot",
		},
		Sink: config.SinkConfig{
			Type: "redis",
			Addr: redisAddr,
			Key:  testKey,
		},
	}

	p, err := pipeline.NewPipeline(cfg, logger.New("plain", "info"))
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

	redisClient := connectToRedis(t, redisAddr)
	defer cleanupRedis(t, redisClient, testKey)

	// Wait for delete event
	var eventData map[string]any
	if !eventuallyHelper(t, func() bool {
		result, err := redisClient.LRange(ctx, testKey, 0, -1).Result()
		if err != nil || len(result) == 0 {
			return false
		}
		for _, r := range result {
			var ev map[string]any
			if err := json.Unmarshal([]byte(r), &ev); err != nil {
				continue
			}
			if ev["op"] == "delete" {
				eventData = ev
				return true
			}
		}
		return false
	}, 15*time.Second, 500*time.Millisecond, "timeout waiting for delete event") {
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
	redisAddr := getRedisAddr()
	testKey := "e2e:cdc:events:multi"

	cfg := config.Config{
		Source: config.SourceConfig{
			Type:     "postgres",
			DSN:      dsn,
			Tables:   []string{"users", "products"},
			SlotName: "iris_e2e_multi_slot",
		},
		Sink: config.SinkConfig{
			Type: "redis",
			Addr: redisAddr,
			Key:  testKey,
		},
	}

	p, err := pipeline.NewPipeline(cfg, logger.New("plain", "info"))
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

	// Insert into both tables
	testID := time.Now().Unix()
	_, err = db.ExecContext(ctx,
		"INSERT INTO users (name, email) VALUES ($1, $2)",
		fmt.Sprintf("Multi User %d", testID),
		fmt.Sprintf("multi%d@test.com", testID))
	if err != nil {
		t.Fatalf("failed to insert user: %v", err)
	}

	_, err = db.ExecContext(ctx,
		"INSERT INTO products (name, price, inventory) VALUES ($1, $2, $3)",
		fmt.Sprintf("Multi Product %d", testID),
		49.99, 25)
	if err != nil {
		t.Fatalf("failed to insert product: %v", err)
	}

	redisClient := connectToRedis(t, redisAddr)
	defer cleanupRedis(t, redisClient, testKey)

	// Wait for both events
	tablesSeen := make(map[string]bool)
	if !eventuallyHelper(t, func() bool {
		result, err := redisClient.LRange(ctx, testKey, 0, -1).Result()
		if err != nil || len(result) == 0 {
			return false
		}
		for _, r := range result {
			var ev map[string]any
			if err := json.Unmarshal([]byte(r), &ev); err != nil {
				continue
			}
			if table, ok := ev["table"].(string); ok {
				tablesSeen[table] = true
			}
		}
		return tablesSeen["users"] && tablesSeen["products"]
	}, 15*time.Second, 500*time.Millisecond, "timeout waiting for events from both tables") {
		t.Fatal("timeout waiting for events from both tables")
	}

	t.Logf("E2E multi-table test passed: saw events from %v", tablesSeen)
}

// eventuallyHelper is a helper for Eventually assertions
func eventuallyHelper(t *testing.T, condition func() bool, timeout, tick time.Duration, msg string) bool {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()

	ticker := time.NewTicker(tick)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			t.Logf("timeout: %s", msg)
			return false
		case <-ticker.C:
			if condition() {
				return true
			}
		}
	}
}

// TestE2E_PostgresToRedisStream_Basic tests CDC pipeline with Redis Stream sink
func TestE2E_PostgresToRedisStream_Basic(t *testing.T) {
	skipIfNoE2E(t)

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	dsn := getPostgresDSN()
	redisAddr := getRedisAddr()
	testStreamKey := "e2e:cdc:stream:basic"

	// 1. Create pipeline config with redis_stream sink
	cfg := config.Config{
		Source: config.SourceConfig{
			Type:     "postgres",
			DSN:      dsn,
			Tables:   []string{"users", "orders"},
			SlotName: "iris_e2e_stream_slot",
		},
		Transform: nil, // No transform
		Sink: config.SinkConfig{
			Type: "redis_stream",
			Addr: redisAddr,
		},
		Mapping: config.MappingConfig{
			TableStreamMap: map[string]string{
				"users":  testStreamKey,
				"orders": testStreamKey,
			},
		},
	}

	// 2. Create pipeline
	p, err := pipeline.NewPipeline(cfg, logger.New("plain", "info"))
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

	testName := fmt.Sprintf("e2e_stream_test_%d", time.Now().Unix())
	_, err = db.ExecContext(ctx,
		"INSERT INTO users (name, email) VALUES ($1, $2)",
		testName, "e2e-stream@test.com")
	if err != nil {
		t.Fatalf("failed to insert test data: %v", err)
	}

	// 5. Wait for event in Redis Stream
	redisClient := connectToRedis(t, redisAddr)
	defer cleanupRedis(t, redisClient, testStreamKey)

	var eventData map[string]any
	if !eventuallyHelper(t, func() bool {
		result, err := redisClient.XRange(ctx, testStreamKey, "-", "+").Result()
		if err != nil || len(result) == 0 {
			return false
		}
		// Extract data field from stream entry
		entry := result[0]
		dataField, ok := entry.Values["data"]
		if !ok {
			return false
		}
		if err := json.Unmarshal([]byte(dataField.(string)), &eventData); err != nil {
			return false
		}
		return true
	}, 15*time.Second, 500*time.Millisecond, "timeout waiting for CDC stream event") {
		t.Fatal("timeout waiting for CDC stream event")
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

	t.Log("E2E Redis Stream test passed: PostgreSQL -> Redis Stream CDC pipeline working")
}

// TestE2E_RedisStream_MaxLen tests stream trimming with MaxLen
func TestE2E_RedisStream_MaxLen(t *testing.T) {
	skipIfNoE2E(t)

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	dsn := getPostgresDSN()
	redisAddr := getRedisAddr()
	testStreamKey := "e2e:cdc:stream:maxlen"
	maxLen := 3

	cfg := config.Config{
		Source: config.SourceConfig{
			Type:     "postgres",
			DSN:      dsn,
			Tables:   []string{"users"},
			SlotName: "iris_e2e_stream_maxlen_slot",
		},
		Sink: config.SinkConfig{
			Type:   "redis_stream",
			Addr:   redisAddr,
			MaxLen: maxLen,
		},
		Mapping: config.MappingConfig{
			TableStreamMap: map[string]string{
				"users": testStreamKey,
			},
		},
	}

	p, err := pipeline.NewPipeline(cfg, logger.New("plain", "info"))
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

	// Insert multiple records to trigger trimming
	redisClient := connectToRedis(t, redisAddr)
	defer cleanupRedis(t, redisClient, testStreamKey)

	for i := 0; i < 5; i++ {
		testEmail := fmt.Sprintf("maxlen_%d_%d@test.com", time.Now().Unix(), i)
		_, err = db.ExecContext(ctx,
			"INSERT INTO users (name, email) VALUES ($1, $2)",
			fmt.Sprintf("User %d", i), testEmail)
		if err != nil {
			t.Fatalf("failed to insert user %d: %v", i, err)
		}
		time.Sleep(100 * time.Millisecond)
	}

	// Wait for all events and verify trimming
	if !eventuallyHelper(t, func() bool {
		result, err := redisClient.XRange(ctx, testStreamKey, "-", "+").Result()
		if err != nil {
			return false
		}
		// Stream should be trimmed to maxLen
		return len(result) <= maxLen
	}, 15*time.Second, 500*time.Millisecond, "timeout waiting for stream trim") {
		t.Fatalf("stream not trimmed to maxLen=%d", maxLen)
	}

	result, _ := redisClient.XRange(ctx, testStreamKey, "-", "+").Result()
	t.Logf("E2E MaxLen test passed: stream trimmed to %d entries (maxLen=%d)", len(result), maxLen)
}
