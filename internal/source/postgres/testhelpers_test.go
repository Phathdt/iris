//go:build integration

package postgres

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"path/filepath"
	"testing"

	_ "github.com/jackc/pgx/v5/stdlib"
	"github.com/testcontainers/testcontainers-go"
	pgcontainer "github.com/testcontainers/testcontainers-go/modules/postgres"
)

// PostgresContainer wraps the testcontainer and provides convenient helpers
type PostgresContainer struct {
	container *pgcontainer.PostgresContainer
	db        *sql.DB
	dsn       string
	host      string
	port      string
}

// SetupPostgresContainer starts a PostgreSQL container and returns a PostgresContainer helper
// It configures wal_level=logical for CDC support
// It uses t.Cleanup() to ensure the container is stopped after the test completes
func SetupPostgresContainer(t *testing.T) *PostgresContainer {
	ctx := context.Background()

	// Start PostgreSQL container (16-alpine with logical replication settings)
	// Create an init script to set up tables and publication
	tmpDir := t.TempDir()
	initScriptPath := filepath.Join(tmpDir, "init.sql")
	// Create tables BEFORE publication so they're included
	initScript := `
-- Create test tables
CREATE TABLE users (
	id SERIAL PRIMARY KEY,
	name TEXT NOT NULL,
	email TEXT NOT NULL
);

CREATE TABLE orders (
	id SERIAL PRIMARY KEY,
	user_id INT NOT NULL,
	amount DECIMAL(10,2) NOT NULL
);

-- Set REPLICA IDENTITY FULL for UPDATE/DELETE to include before data
ALTER TABLE users REPLICA IDENTITY FULL;
ALTER TABLE orders REPLICA IDENTITY FULL;

-- Create publication for logical replication AFTER tables are defined
CREATE PUBLICATION pglogrepl_publication FOR ALL TABLES;
`
	if err := os.WriteFile(initScriptPath, []byte(initScript), 0644); err != nil {
		t.Fatalf("failed to create init script: %v", err)
	}

	// Note: Postgres requires wal_level=logical at startup time
	// We use custom command args to set this
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

	// Register cleanup
	t.Cleanup(func() {
		if err := container.Terminate(ctx); err != nil {
			t.Logf("failed to terminate PostgreSQL container: %v", err)
		}
	})

	// Get the host:port address
	host, err := container.Host(ctx)
	if err != nil {
		t.Fatalf("failed to get container host: %v", err)
	}
	port, err := container.MappedPort(ctx, "5432")
	if err != nil {
		t.Fatalf("failed to get container port: %v", err)
	}

	// Create DSN for regular connections
	dsn := fmt.Sprintf("postgres://testuser:testpass@%s:%s/testdb?sslmode=disable", host, port.Port())

	// Connect to database
	db, err := sql.Open("pgx", dsn)
	if err != nil {
		t.Fatalf("failed to open database connection: %v", err)
	}

	// Test connection
	if err := db.PingContext(ctx); err != nil {
		t.Fatalf("failed to ping PostgreSQL container: %v", err)
	}

	return &PostgresContainer{
		container: container,
		db:        db,
		dsn:       dsn,
		host:      host,
		port:      port.Port(),
	}
}

// DSN returns the connection string for regular (non-replication) connections
func (pc *PostgresContainer) DSN() string {
	return pc.dsn
}

// ReplicationDSN returns the connection string with replication parameter for CDC
func (pc *PostgresContainer) ReplicationDSN() string {
	return pc.dsn + "&replication=database"
}

// ExecSQL executes a SQL query with arguments for test data manipulation
func (pc *PostgresContainer) ExecSQL(t *testing.T, query string, args ...interface{}) {
	ctx := context.Background()
	result, err := pc.db.ExecContext(ctx, query, args...)
	if err != nil {
		t.Fatalf("failed to execute SQL: %v", err)
	}

	// Log affected rows for debugging
	rows, err := result.RowsAffected()
	if err == nil {
		t.Logf("SQL affected %d rows", rows)
	}
}

// QueryRow executes a query that returns a single row
func (pc *PostgresContainer) QueryRow(t *testing.T, query string, args ...interface{}) *sql.Row {
	ctx := context.Background()
	return pc.db.QueryRowContext(ctx, query, args...)
}

// DB returns the underlying sql.DB for direct access
func (pc *PostgresContainer) DB() *sql.DB {
	return pc.db
}

// Close closes the database connection (cleanup is handled by t.Cleanup)
func (pc *PostgresContainer) Close(t *testing.T) {
	if err := pc.db.Close(); err != nil {
		t.Logf("failed to close database connection: %v", err)
	}
}
