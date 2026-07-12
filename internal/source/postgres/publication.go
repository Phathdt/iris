package postgres

import (
	"context"
	"fmt"
	"sort"
	"strings"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
)

// ensurePublication makes the named publication match the desired table
// list, using the given connection (expected to already be open in
// replication mode). It does not create or alter an all-tables publication;
// callers must pass a non-empty tables slice to get auto-create/alter
// behavior. An empty tables slice only verifies the publication exists.
func ensurePublication(ctx context.Context, conn *pgconn.PgConn, name string, tables []string) error {
	current, exists, err := currentPublicationTables(ctx, conn, name)
	if err != nil {
		return fmt.Errorf("check publication %q: %w", name, err)
	}

	if !exists {
		if len(tables) == 0 {
			return fmt.Errorf("publication %q does not exist and source.tables is empty; create it manually or set source.tables", name)
		}
		sql := fmt.Sprintf("CREATE PUBLICATION %s FOR TABLE %s", pgx.Identifier{name}.Sanitize(), quoteTableList(tables))
		if _, err := conn.Exec(ctx, sql).ReadAll(); err != nil {
			return fmt.Errorf("create publication %q: %w", name, err)
		}
		return nil
	}

	if len(tables) == 0 || tablesMatch(tables, current) {
		return nil
	}

	sql := fmt.Sprintf("ALTER PUBLICATION %s SET TABLE %s", pgx.Identifier{name}.Sanitize(), quoteTableList(tables))
	if _, err := conn.Exec(ctx, sql).ReadAll(); err != nil {
		return fmt.Errorf("alter publication %q: %w", name, err)
	}
	return nil
}

// currentPublicationTables returns the current table membership of the
// named publication, and whether it exists at all (distinguishing
// "exists with zero tables" from "does not exist").
func currentPublicationTables(ctx context.Context, conn *pgconn.PgConn, name string) (tables []string, exists bool, err error) {
	existsSQL := fmt.Sprintf("SELECT 1 FROM pg_publication WHERE pubname = '%s'", escapeLiteral(name))
	existsResult, err := conn.Exec(ctx, existsSQL).ReadAll()
	if err != nil {
		return nil, false, err
	}
	if len(existsResult) == 0 || len(existsResult[0].Rows) == 0 {
		return nil, false, nil
	}

	tablesSQL := fmt.Sprintf("SELECT tablename FROM pg_publication_tables WHERE pubname = '%s'", escapeLiteral(name))
	tablesResult, err := conn.Exec(ctx, tablesSQL).ReadAll()
	if err != nil {
		return nil, true, err
	}
	if len(tablesResult) == 0 {
		return []string{}, true, nil
	}
	for _, row := range tablesResult[0].Rows {
		tables = append(tables, string(row[0]))
	}
	return tables, true, nil
}

// tablesMatch reports whether two table lists contain the same set of
// tables, ignoring order and duplicates.
func tablesMatch(a, b []string) bool {
	return setOf(a).equal(setOf(b))
}

type stringSet map[string]struct{}

func setOf(items []string) stringSet {
	s := make(stringSet, len(items))
	for _, item := range items {
		s[item] = struct{}{}
	}
	return s
}

func (s stringSet) equal(other stringSet) bool {
	if len(s) != len(other) {
		return false
	}
	for item := range s {
		if _, ok := other[item]; !ok {
			return false
		}
	}
	return true
}

// quoteTableList renders a comma-separated, identifier-quoted table list
// for use in CREATE/ALTER PUBLICATION statements. Table names are sorted so
// generated SQL is deterministic (useful for tests and logs).
func quoteTableList(tables []string) string {
	sorted := append([]string(nil), tables...)
	sort.Strings(sorted)
	quoted := make([]string, len(sorted))
	for i, t := range sorted {
		quoted[i] = pgx.Identifier(strings.SplitN(t, ".", 2)).Sanitize()
	}
	return strings.Join(quoted, ", ")
}

// escapeLiteral escapes a value for safe interpolation into a single-quoted
// SQL string literal (pgconn.Exec uses the simple query protocol, which has
// no placeholder support).
func escapeLiteral(s string) string {
	return strings.ReplaceAll(s, "'", "''")
}
