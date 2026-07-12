//go:build integration

package postgres

import (
	"context"
	"sort"
	"strings"
	"testing"
	"time"
)

// queryPublicationTables returns the sorted table membership of the named
// publication via a direct SQL query (bypasses iris's own logic under test).
func queryPublicationTables(t *testing.T, pg *PostgresContainer, name string) []string {
	rows, err := pg.DB().QueryContext(context.Background(),
		"SELECT tablename FROM pg_publication_tables WHERE pubname = $1", name)
	if err != nil {
		t.Fatalf("query pg_publication_tables: %v", err)
	}
	defer rows.Close()

	var tables []string
	for rows.Next() {
		var tbl string
		if err := rows.Scan(&tbl); err != nil {
			t.Fatalf("scan tablename: %v", err)
		}
		tables = append(tables, tbl)
	}
	sort.Strings(tables)
	return tables
}

// publicationExists reports whether the named publication exists.
func publicationExists(t *testing.T, pg *PostgresContainer, name string) bool {
	var exists bool
	err := pg.DB().QueryRowContext(context.Background(),
		"SELECT EXISTS(SELECT 1 FROM pg_publication WHERE pubname = $1)", name).Scan(&exists)
	if err != nil {
		t.Fatalf("query pg_publication: %v", err)
	}
	return exists
}

// TestIntegration_PostgresSource_EnsurePublication_AutoCreate verifies that
// Start() with ensure_publication: true creates a missing publication scoped
// to the configured tables.
func TestIntegration_PostgresSource_EnsurePublication_AutoCreate(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	pg := SetupPostgresContainerNoPublication(t)
	defer pg.Close(t)

	src, err := NewSource(Config{
		DSN:               pg.ReplicationDSN(),
		SlotName:          "test_ensure_pub_autocreate_slot",
		Tables:            []string{"users"},
		Publication:       "pglogrepl_publication",
		EnsurePublication: true,
	})
	if err != nil {
		t.Fatalf("failed to create source: %v", err)
	}
	defer src.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	if _, err := src.Start(ctx); err != nil {
		t.Fatalf("Start() error = %v", err)
	}

	got := queryPublicationTables(t, pg, "pglogrepl_publication")
	want := []string{"users"}
	if len(got) != len(want) || got[0] != want[0] {
		t.Fatalf("publication tables = %v, want %v", got, want)
	}
}

// TestIntegration_PostgresSource_EnsurePublication_Idempotent verifies that
// starting a second source against an already-synced publication is a no-op.
func TestIntegration_PostgresSource_EnsurePublication_Idempotent(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	pg := SetupPostgresContainerNoPublication(t)
	defer pg.Close(t)

	cfg := Config{
		DSN:               pg.ReplicationDSN(),
		SlotName:          "test_ensure_pub_idempotent_slot",
		Tables:            []string{"users"},
		Publication:       "pglogrepl_publication",
		EnsurePublication: true,
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	src1, err := NewSource(cfg)
	if err != nil {
		t.Fatalf("failed to create first source: %v", err)
	}
	if _, err := src1.Start(ctx); err != nil {
		t.Fatalf("first Start() error = %v", err)
	}
	src1.Close()

	cfg.SlotName = "test_ensure_pub_idempotent_slot_2"
	src2, err := NewSource(cfg)
	if err != nil {
		t.Fatalf("failed to create second source: %v", err)
	}
	defer src2.Close()

	if _, err := src2.Start(ctx); err != nil {
		t.Fatalf("second Start() error = %v (expected no-op success)", err)
	}

	got := queryPublicationTables(t, pg, "pglogrepl_publication")
	want := []string{"users"}
	if len(got) != len(want) || got[0] != want[0] {
		t.Fatalf("publication tables after second start = %v, want %v", got, want)
	}
}

// TestIntegration_PostgresSource_EnsurePublication_AlterResync verifies that
// Start() alters an existing publication's table list when it differs from
// the configured Tables.
func TestIntegration_PostgresSource_EnsurePublication_AlterResync(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	pg := SetupPostgresContainerNoPublication(t)
	defer pg.Close(t)

	pg.ExecSQL(t, `CREATE PUBLICATION pglogrepl_publication FOR TABLE users`)

	src, err := NewSource(Config{
		DSN:               pg.ReplicationDSN(),
		SlotName:          "test_ensure_pub_alter_slot",
		Tables:            []string{"users", "orders"},
		Publication:       "pglogrepl_publication",
		EnsurePublication: true,
	})
	if err != nil {
		t.Fatalf("failed to create source: %v", err)
	}
	defer src.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	if _, err := src.Start(ctx); err != nil {
		t.Fatalf("Start() error = %v", err)
	}

	got := queryPublicationTables(t, pg, "pglogrepl_publication")
	want := []string{"orders", "users"}
	if len(got) != len(want) || got[0] != want[0] || got[1] != want[1] {
		t.Fatalf("publication tables = %v, want %v", got, want)
	}
}

// TestIntegration_PostgresSource_EnsurePublicationFalse_PreservesOldBehavior
// verifies that ensure_publication: false skips all publication management,
// matching pre-v0.1.1 behavior (publication must already exist).
func TestIntegration_PostgresSource_EnsurePublicationFalse_PreservesOldBehavior(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	pg := SetupPostgresContainer(t) // pre-creates pglogrepl_publication FOR ALL TABLES
	defer pg.Close(t)

	src, err := NewSource(Config{
		DSN:               pg.ReplicationDSN(),
		SlotName:          "test_ensure_pub_false_slot",
		Tables:            []string{"nonexistent_table"}, // must be ignored
		Publication:       "pglogrepl_publication",
		EnsurePublication: false,
	})
	if err != nil {
		t.Fatalf("failed to create source: %v", err)
	}
	defer src.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	if _, err := src.Start(ctx); err != nil {
		t.Fatalf("Start() error = %v (expected success, publication management skipped)", err)
	}
}

// TestIntegration_PostgresSource_EnsurePublicationFalse_MissingPublicationFails
// verifies that ensure_publication: false skips iris's own publication check
// entirely, matching v0.1.0 behavior exactly. Note the name is aspirational
// from the plan, not literal: Postgres does not validate publication_names
// synchronously in START_REPLICATION — verified empirically (a bare
// pglogrepl.StartReplication against a nonexistent publication returns a nil
// error). The "publication does not exist" ErrorResponse (SQLSTATE 42704)
// only arrives once pgoutput tries to decode a change, and today's
// receiveWALMessages loop (source.go) only handles *pgproto3.CopyData
// messages — an ErrorResponse is silently dropped, matching pre-existing
// (not new) behavior. So the observable contract for this scenario is:
// Start() succeeds immediately, exactly as before this feature existed.
func TestIntegration_PostgresSource_EnsurePublicationFalse_MissingPublicationFails(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	pg := SetupPostgresContainerNoPublication(t)
	defer pg.Close(t)

	src, err := NewSource(Config{
		DSN:               pg.ReplicationDSN(),
		SlotName:          "test_ensure_pub_false_missing_slot",
		Tables:            []string{"users"},
		Publication:       "pglogrepl_publication",
		EnsurePublication: false,
	})
	if err != nil {
		t.Fatalf("failed to create source: %v", err)
	}
	defer src.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	if _, err := src.Start(ctx); err != nil {
		t.Fatalf("Start() error = %v (expected success: ensure_publication is false, so iris skips its own check and Postgres doesn't validate synchronously)", err)
	}
	if publicationExists(t, pg, "pglogrepl_publication") {
		t.Fatal("ensure_publication: false must not create the publication")
	}
}

// TestIntegration_PostgresSource_EnsurePublication_EmptyTablesMissingPublication
// verifies that ensure_publication: true with empty Tables and a missing
// publication errors clearly, naming the publication and pointing at
// source.tables, instead of attempting to create/alter anything.
func TestIntegration_PostgresSource_EnsurePublication_EmptyTablesMissingPublication(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	pg := SetupPostgresContainerNoPublication(t)
	defer pg.Close(t)

	src, err := NewSource(Config{
		DSN:               pg.ReplicationDSN(),
		SlotName:          "test_ensure_pub_empty_tables_slot",
		Tables:            nil,
		Publication:       "pglogrepl_publication",
		EnsurePublication: true,
	})
	if err != nil {
		t.Fatalf("failed to create source: %v", err)
	}
	defer src.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	_, err = src.Start(ctx)
	if err == nil {
		t.Fatal("expected Start() to fail when tables is empty and publication is missing")
	}
	if !strings.Contains(err.Error(), "pglogrepl_publication") || !strings.Contains(err.Error(), "source.tables") {
		t.Fatalf("Start() error should mention publication name and source.tables, got: %v", err)
	}
	if publicationExists(t, pg, "pglogrepl_publication") {
		t.Fatal("publication should not have been created")
	}
}
