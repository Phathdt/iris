package postgres

import "testing"

func TestTablesMatch(t *testing.T) {
	tests := []struct {
		name string
		a    []string
		b    []string
		want bool
	}{
		{
			name: "identical order",
			a:    []string{"users", "orders"},
			b:    []string{"users", "orders"},
			want: true,
		},
		{
			name: "different order",
			a:    []string{"users", "orders"},
			b:    []string{"orders", "users"},
			want: true,
		},
		{
			name: "duplicates ignored",
			a:    []string{"users", "users", "orders"},
			b:    []string{"orders", "users"},
			want: true,
		},
		{
			name: "empty vs empty",
			a:    []string{},
			b:    []string{},
			want: true,
		},
		{
			name: "subset is not a match",
			a:    []string{"users"},
			b:    []string{"users", "orders"},
			want: false,
		},
		{
			name: "superset is not a match",
			a:    []string{"users", "orders"},
			b:    []string{"users"},
			want: false,
		},
		{
			name: "disjoint",
			a:    []string{"users"},
			b:    []string{"orders"},
			want: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := tablesMatch(tt.a, tt.b); got != tt.want {
				t.Errorf("tablesMatch(%v, %v) = %v, want %v", tt.a, tt.b, got, tt.want)
			}
		})
	}
}

func TestQuoteTableList(t *testing.T) {
	tests := []struct {
		name   string
		tables []string
		want   string
	}{
		{
			name:   "single table",
			tables: []string{"outbox_events"},
			want:   `"outbox_events"`,
		},
		{
			name:   "multiple tables sorted",
			tables: []string{"orders", "users"},
			want:   `"orders", "users"`,
		},
		{
			name:   "mixed case is quoted and preserved",
			tables: []string{"OutboxEvents"},
			want:   `"OutboxEvents"`,
		},
		{
			name:   "reserved word is quoted",
			tables: []string{"select"},
			want:   `"select"`,
		},
		{
			name:   "double quote in identifier is escaped",
			tables: []string{`weird"table`},
			want:   `"weird""table"`,
		},
		{
			name:   "schema-qualified table name",
			tables: []string{"public.outbox_events"},
			want:   `"public"."outbox_events"`,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := quoteTableList(tt.tables); got != tt.want {
				t.Errorf("quoteTableList(%v) = %q, want %q", tt.tables, got, tt.want)
			}
		})
	}
}

func TestEscapeLiteral(t *testing.T) {
	tests := []struct {
		name string
		in   string
		want string
	}{
		{name: "no special chars", in: "pglogrepl_publication", want: "pglogrepl_publication"},
		{name: "single quote escaped", in: "o'brien_pub", want: "o''brien_pub"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := escapeLiteral(tt.in); got != tt.want {
				t.Errorf("escapeLiteral(%q) = %q, want %q", tt.in, got, tt.want)
			}
		})
	}
}
