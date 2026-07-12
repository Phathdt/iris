//go:build integration

package pipeline

import (
	"bufio"
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	"iris/pkg/config"
	"iris/pkg/logger"
	"iris/pkg/observability"

	_ "github.com/jackc/pgx/v5/stdlib"
	"github.com/testcontainers/testcontainers-go"
	pgcontainer "github.com/testcontainers/testcontainers-go/modules/postgres"
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

// countJSONLines returns the number of JSON-line records currently
// persisted at path. Missing file is treated as zero lines (the file
// sink creates the file lazily on first write).
func countJSONLines(t *testing.T, path string) int {
	f, err := os.Open(path)
	if err != nil {
		if os.IsNotExist(err) {
			return 0
		}
		t.Fatalf("failed to open output file: %v", err)
	}
	defer f.Close()

	count := 0
	scanner := bufio.NewScanner(f)
	for scanner.Scan() {
		line := scanner.Bytes()
		if len(line) == 0 {
			continue
		}
		var raw map[string]interface{}
		if err := json.Unmarshal(line, &raw); err != nil {
			t.Fatalf("failed to unmarshal event line: %v", err)
		}
		count++
	}
	if err := scanner.Err(); err != nil {
		t.Fatalf("failed to scan output file: %v", err)
	}
	return count
}

// TestIntegration_Pipeline_PostgresToFile tests full CDC pipeline with the file sink:
// Postgres logical replication -> decode -> sink.Write -> JSON lines on disk.
func TestIntegration_Pipeline_PostgresToFile(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	// Start Postgres container
	pg := startPostgresForPipelineTest(t, ctx)
	defer pg.Close(t)

	// Output file for the file sink
	tmpDir, err := os.MkdirTemp("", "pipeline-file-sink")
	if err != nil {
		t.Fatalf("failed to create temp directory: %v", err)
	}
	defer os.RemoveAll(tmpDir)
	outputPath := filepath.Join(tmpDir, "events.jsonl")

	// Create pipeline config
	cfg := config.Config{
		Source: config.SourceConfig{
			Type:     "postgres",
			DSN:      pg.ReplicationDSN(),
			Tables:   []string{"users"},
			SlotName: "pipeline_test_file_slot",
		},
		Sink: config.SinkConfig{
			Type: "file",
			Path: outputPath,
		},
		Transform: &config.TransformConfig{
			Enabled: false,
		},
	}

	// Create logger
	log := logger.New("plain", "info")

	// Create and start pipeline
	pipe, err := NewPipeline(cfg, log, observability.NewNoopMetrics())
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

	// Poll the output file for events with timeout
	timeout := time.After(15 * time.Second)
	eventFound := false

	for {
		select {
		case <-timeout:
			if !eventFound {
				t.Fatal("timeout waiting for event in output file")
			}
			cancel() // Stop pipeline
			return

		case err := <-pipelineDone:
			if !eventFound && err != context.Canceled {
				t.Fatalf("pipeline error before event received: %v", err)
			}
			return

		default:
			if countJSONLines(t, outputPath) > 0 {
				t.Log("found event in output file")
				eventFound = true
				cancel() // Stop pipeline
				return
			}

			time.Sleep(500 * time.Millisecond)
		}
	}
}
