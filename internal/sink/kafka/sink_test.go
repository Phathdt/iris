package kafka

import (
	"testing"
)

func TestGetTopic_ExplicitMapping(t *testing.T) {
	cfg := Config{
		TableTopicMap: map[string]string{
			"users":  "prod.users.events",
			"orders": "prod.orders.events",
		},
	}

	if got := cfg.GetTopic("users"); got != "prod.users.events" {
		t.Errorf("GetTopic(users) = %q, want %q", got, "prod.users.events")
	}
	if got := cfg.GetTopic("orders"); got != "prod.orders.events" {
		t.Errorf("GetTopic(orders) = %q, want %q", got, "prod.orders.events")
	}
}

func TestGetTopic_DefaultMapping(t *testing.T) {
	cfg := Config{}

	if got := cfg.GetTopic("users"); got != "cdc.users" {
		t.Errorf("GetTopic(users) = %q, want %q", got, "cdc.users")
	}
}

func TestGetTopic_FallbackForUnmappedTable(t *testing.T) {
	cfg := Config{
		TableTopicMap: map[string]string{
			"users": "prod.users",
		},
	}

	if got := cfg.GetTopic("orders"); got != "cdc.orders" {
		t.Errorf("GetTopic(orders) = %q, want %q", got, "cdc.orders")
	}
}

func TestNewKafkaSink_NoBrokers(t *testing.T) {
	_, err := NewKafkaSink(Config{})
	if err == nil {
		t.Fatal("expected error for empty brokers")
	}
}
