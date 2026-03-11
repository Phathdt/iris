//go:build integration

package pipeline

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	"iris/pkg/config"
	"iris/pkg/logger"

	_ "github.com/jackc/pgx/v5/stdlib"
	"github.com/redis/go-redis/v9"
	"github.com/testcontainers/testcontainers-go"
	pgcontainer "github.com/testcontainers/testcontainers-go/modules/postgres"
	rediscontainer "github.com/testcontainers/testcontainers-go/modules/redis"
)

// PostgresTestContainer wraps PostgreSQL container for pipeline tests
type PostgresTestContainer struct {
	container *pgcontainer.PostgresContainer
	db        *sql.DB
	dsn       string
}

// startPostgresForPipelineTest starts a PostgreSQL container configured for CDC
func startPostgresForPipelineTest(t *testing.T, ctx context.Context) *PostgresTestContainer {
	// Create temporary init script for setup
	tmpDir, err := os.MkdirTemp("", "postgres-init")
	if err != nil {
		t.Fatalf("failed to create temp directory: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	initScriptPath := filepath.Join(tmpDir, "init.sql")
	initScript := `
-- Create test tables
CREATE TABLE users (
	id SERIAL PRIMARY KEY,
	name TEXT NOT NULL,
	email TEXT NOT NULL
);

-- Set REPLICA IDENTITY FULL for UPDATE/DELETE to include before data
ALTER TABLE users REPLICA IDENTITY FULL;

-- Create publication for logical replication AFTER tables are defined
CREATE PUBLICATION pglogrepl_publication FOR ALL TABLES;
`
	if err := os.WriteFile(initScriptPath, []byte(initScript), 0o644); err != nil {
		t.Fatalf("failed to create init script: %v", err)
	}

	// Start PostgreSQL with wal_level=logical
	container, err := pgcontainer.RunContainer(ctx,
		pgcontainer.WithDatabase("testdb"),
		pgcontainer.WithUsername("testuser"),
		pgcontainer.WithPassword("testpass"),
		pgcontainer.WithInitScripts(initScriptPath),
		pgcontainer.BasicWaitStrategies(),
		testcontainers.WithCmd(
			"postgres",
			"-c", "wal_level=logical",
			"-c", "max_replication_slots=4",
			"-c", "max_wal_senders=4",
		),
	)
	if err != nil {
		t.Fatalf("failed to start PostgreSQL container: %v", err)
	}

	t.Cleanup(func() {
		if err := container.Terminate(ctx); err != nil {
			t.Logf("failed to terminate PostgreSQL container: %v", err)
		}
	})

	// Get host and port
	host, err := container.Host(ctx)
	if err != nil {
		t.Fatalf("failed to get container host: %v", err)
	}
	port, err := container.MappedPort(ctx, "5432")
	if err != nil {
		t.Fatalf("failed to get container port: %v", err)
	}

	// Create DSN
	dsn := fmt.Sprintf("postgres://testuser:testpass@%s:%s/testdb?sslmode=disable", host, port.Port())

	// Connect to database
	db, err := sql.Open("pgx", dsn)
	if err != nil {
		t.Fatalf("failed to open database connection: %v", err)
	}

	if err := db.PingContext(ctx); err != nil {
		t.Fatalf("failed to ping PostgreSQL: %v", err)
	}

	return &PostgresTestContainer{
		container: container,
		db:        db,
		dsn:       dsn,
	}
}

// ReplicationDSN returns the DSN with replication parameter
func (ptc *PostgresTestContainer) ReplicationDSN() string {
	return ptc.dsn + "&replication=database"
}

// ExecSQL executes a SQL query
func (ptc *PostgresTestContainer) ExecSQL(t *testing.T, query string, args ...interface{}) {
	ctx := context.Background()
	if _, err := ptc.db.ExecContext(ctx, query, args...); err != nil {
		t.Fatalf("failed to execute SQL: %v", err)
	}
}

// Close closes the database connection
func (ptc *PostgresTestContainer) Close(t *testing.T) {
	if err := ptc.db.Close(); err != nil {
		t.Logf("failed to close database: %v", err)
	}
}

// RedisTestContainer wraps Redis container for pipeline tests
type RedisTestContainer struct {
	container *rediscontainer.RedisContainer
	client    *redis.Client
	addr      string
}

// startRedisForPipelineTest starts a Redis container
func startRedisForPipelineTest(t *testing.T, ctx context.Context) *RedisTestContainer {
	container, err := rediscontainer.RunContainer(ctx)
	if err != nil {
		t.Fatalf("failed to start Redis container: %v", err)
	}

	t.Cleanup(func() {
		if err := container.Terminate(ctx); err != nil {
			t.Logf("failed to terminate Redis container: %v", err)
		}
	})

	// Get host and port
	host, err := container.Host(ctx)
	if err != nil {
		t.Fatalf("failed to get Redis container host: %v", err)
	}
	port, err := container.MappedPort(ctx, "6379")
	if err != nil {
		t.Fatalf("failed to get Redis container port: %v", err)
	}

	addr := fmt.Sprintf("%s:%s", host, port.Port())

	// Create Redis client
	client := redis.NewClient(&redis.Options{
		Addr: addr,
	})

	// Test connection
	if err := client.Ping(ctx).Err(); err != nil {
		t.Fatalf("failed to ping Redis: %v", err)
	}

	return &RedisTestContainer{
		container: container,
		client:    client,
		addr:      addr,
	}
}

// Addr returns the Redis address
func (rtc *RedisTestContainer) Addr() string {
	return rtc.addr
}

// Client returns the Redis client
func (rtc *RedisTestContainer) Client() *redis.Client {
	return rtc.client
}

// Close closes the Redis connection (cleanup is handled by t.Cleanup)
func (rtc *RedisTestContainer) Close(t *testing.T) {
	if err := rtc.client.Close(); err != nil {
		t.Logf("failed to close Redis client: %v", err)
	}
}

// TestIntegration_Pipeline_PostgresToRedisList tests full CDC pipeline with Redis List sink
func TestIntegration_Pipeline_PostgresToRedisList(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	// Start containers
	pg := startPostgresForPipelineTest(t, ctx)
	defer pg.Close(t)

	redis := startRedisForPipelineTest(t, ctx)
	defer redis.Close(t)

	// Create pipeline config
	cfg := config.Config{
		Source: config.SourceConfig{
			Type:     "postgres",
			DSN:      pg.ReplicationDSN(),
			Tables:   []string{"users"},
			SlotName: "pipeline_test_list_slot",
		},
		Sink: config.SinkConfig{
			Type: "redis",
			Addr: redis.Addr(),
			Key:  "pipeline:test:list",
		},
		Transform: &config.TransformConfig{
			Enabled: false,
		},
	}

	// Create logger
	log := logger.New("plain", "info")

	// Create and start pipeline
	pipe, err := NewPipeline(cfg, log)
	if err != nil {
		t.Fatalf("failed to create pipeline: %v", err)
	}
	defer pipe.Close()

	// Run pipeline in a goroutine
	pipelineDone := make(chan error, 1)
	go func() {
		pipelineDone <- pipe.Run(ctx)
	}()

	// Give pipeline time to start replication
	time.Sleep(2 * time.Second)

	// Insert test data via SQL
	pg.ExecSQL(t, "INSERT INTO users (name, email) VALUES ($1, $2)", "Pipeline", "pipeline@test.com")

	// Poll Redis for events with timeout
	timeout := time.After(15 * time.Second)
	eventFound := false

	for {
		select {
		case <-timeout:
			if !eventFound {
				t.Fatal("timeout waiting for event in Redis List")
			}
			cancel() // Stop pipeline
			return

		case err := <-pipelineDone:
			if !eventFound && err != context.Canceled {
				t.Fatalf("pipeline error before event received: %v", err)
			}
			return

		default:
			// Check if event is in Redis
			length, err := redis.Client().LLen(ctx, cfg.Sink.Key).Result()
			if err != nil {
				t.Logf("error checking Redis list length: %v", err)
			} else if length > 0 {
				t.Logf("found %d events in Redis list", length)

				// Get the event
				data, err := redis.Client().LIndex(ctx, cfg.Sink.Key, 0).Result()
				if err != nil {
					t.Logf("error getting event from Redis: %v", err)
				} else {
					t.Logf("event data: %s", data)
					eventFound = true
					cancel() // Stop pipeline
					return
				}
			}

			time.Sleep(500 * time.Millisecond)
		}
	}
}

// TestIntegration_Pipeline_PostgresToRedisStream tests full CDC pipeline with Redis Stream sink
func TestIntegration_Pipeline_PostgresToRedisStream(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	// Start containers
	pg := startPostgresForPipelineTest(t, ctx)
	defer pg.Close(t)

	redis := startRedisForPipelineTest(t, ctx)
	defer redis.Close(t)

	// Create pipeline config
	cfg := config.Config{
		Source: config.SourceConfig{
			Type:     "postgres",
			DSN:      pg.ReplicationDSN(),
			Tables:   []string{"users"},
			SlotName: "pipeline_test_stream_slot",
		},
		Sink: config.SinkConfig{
			Type: "redis_stream",
			Addr: redis.Addr(),
		},
		Mapping: config.MappingConfig{
			TableStreamMap: map[string]string{
				"users": "users_stream",
			},
		},
		Transform: &config.TransformConfig{
			Enabled: false,
		},
	}

	// Create logger
	log := logger.New("plain", "info")

	// Create and start pipeline
	pipe, err := NewPipeline(cfg, log)
	if err != nil {
		t.Fatalf("failed to create pipeline: %v", err)
	}
	defer pipe.Close()

	// Run pipeline in a goroutine
	pipelineDone := make(chan error, 1)
	go func() {
		pipelineDone <- pipe.Run(ctx)
	}()

	// Give pipeline time to start replication
	time.Sleep(2 * time.Second)

	// Insert test data via SQL
	pg.ExecSQL(t, "INSERT INTO users (name, email) VALUES ($1, $2)", "Stream", "stream@test.com")

	// Poll Redis Stream for events with timeout
	timeout := time.After(15 * time.Second)
	eventFound := false
	streamKey := "users_stream"

	for {
		select {
		case <-timeout:
			if !eventFound {
				t.Fatal("timeout waiting for event in Redis Stream")
			}
			cancel() // Stop pipeline
			return

		case err := <-pipelineDone:
			if !eventFound && err != context.Canceled {
				t.Fatalf("pipeline error before event received: %v", err)
			}
			return

		default:
			// Check if event is in Redis Stream
			entries, err := redis.Client().XRange(ctx, streamKey, "-", "+").Result()
			if err != nil {
				// Stream may not exist yet
			} else if len(entries) > 0 {
				t.Logf("found %d events in Redis stream", len(entries))
				eventFound = true
				cancel()
				return
			}

			time.Sleep(500 * time.Millisecond)
		}
	}
}
