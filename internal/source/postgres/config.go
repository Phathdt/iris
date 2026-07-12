package postgres

import "iris/pkg/cdc"

// Config holds PostgreSQL source configuration
type Config struct {
	// DSN is the PostgreSQL connection string
	DSN string

	// Tables is the list of tables to replicate (empty means all tables)
	Tables []string

	// SlotName is the replication slot name
	SlotName string

	// StartOffset is the WAL position to start from (optional)
	// If nil, starts from current WAL position
	StartOffset *cdc.Offset

	// Publication is the PostgreSQL publication name used for logical
	// replication
	Publication string

	// EnsurePublication controls whether Start() auto-creates/syncs the
	// publication from Tables. When false, the publication must already
	// exist (matches pre-v0.1.1 behavior).
	EnsurePublication bool
}
