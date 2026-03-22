package nats

import (
	"testing"
)

func TestGetSubject_ExplicitMapping(t *testing.T) {
	cfg := Config{
		TableSubjectMap: map[string]string{
			"users":  "myapp.users",
			"orders": "myapp.orders",
		},
	}

	tests := []struct {
		table string
		want  string
	}{
		{"users", "myapp.users"},
		{"orders", "myapp.orders"},
		{"unknown", "cdc.unknown"},
	}

	for _, tt := range tests {
		got := cfg.GetSubject(tt.table)
		if got != tt.want {
			t.Errorf("GetSubject(%q) = %q, want %q", tt.table, got, tt.want)
		}
	}
}

func TestGetSubject_DefaultMapping(t *testing.T) {
	cfg := Config{}

	got := cfg.GetSubject("users")
	if got != "cdc.users" {
		t.Errorf("GetSubject(users) = %q, want %q", got, "cdc.users")
	}
}

func TestNewNATSSink_EmptyURL(t *testing.T) {
	_, err := NewNATSSink(Config{URL: ""})
	if err == nil {
		t.Fatal("expected error for empty URL")
	}
}
