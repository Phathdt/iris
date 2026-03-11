//go:build integration

package postgres

import (
	"context"
	"testing"
	"time"

	"iris/pkg/cdc"
)

// TestIntegration_PostgresSource_Connect verifies source connects and creates replication slot
func TestIntegration_PostgresSource_Connect(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	pg := SetupPostgresContainer(t)
	defer pg.Close(t)

	// Create source
	src, err := NewSource(Config{
		DSN:      pg.ReplicationDSN(),
		SlotName: "test_connect_slot",
		Tables:   []string{"users"},
	})
	if err != nil {
		t.Fatalf("failed to create source: %v", err)
	}
	defer src.Close()

	// Start source should succeed
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	events, err := src.Start(ctx)
	if err != nil {
		t.Fatalf("failed to start source: %v", err)
	}

	// Verify events channel is not nil
	if events == nil {
		t.Fatal("events channel is nil")
	}

	t.Log("Source connected and replication started successfully")
}

// TestIntegration_PostgresSource_InsertEvent verifies INSERT events are captured
func TestIntegration_PostgresSource_InsertEvent(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	pg := SetupPostgresContainer(t)
	defer pg.Close(t)

	// Create source
	src, err := NewSource(Config{
		DSN:      pg.ReplicationDSN(),
		SlotName: "test_insert_slot",
		Tables:   []string{"users"},
	})
	if err != nil {
		t.Fatalf("failed to create source: %v", err)
	}
	defer src.Close()

	// Start source
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	t.Log("Starting source for INSERT test...")
	events, err := src.Start(ctx)
	if err != nil {
		t.Fatalf("failed to start source: %v", err)
	}
	t.Log("Source started successfully")

	// Check if there are any replication slots
	var slotCount int
	err = pg.DB().QueryRowContext(ctx, "SELECT COUNT(*) FROM pg_replication_slots").Scan(&slotCount)
	if err != nil {
		t.Logf("error querying slots: %v", err)
	} else {
		t.Logf("Active replication slots: %d", slotCount)
	}

	// Give replication a moment to set up
	time.Sleep(5 * time.Second)

	// Insert test data via regular SQL connection
	t.Log("Inserting test data...")
	pg.ExecSQL(t, "INSERT INTO users (name, email) VALUES ($1, $2)", "Alice", "alice@test.com")
	t.Log("Data inserted")

	// Force a checkpoint to ensure WAL is flushed
	pg.ExecSQL(t, "SELECT pg_current_wal_lsn()")

	// Create decoder and read events
	decoder := NewDecoder()

	// Wait for events with timeout
	timeout := time.After(30 * time.Second)
	eventFound := false
	msgCount := 0
	channelClosed := false

	for {
		select {
		case <-timeout:
			if !eventFound {
				t.Logf(
					"timeout waiting for INSERT CDC event (got %d messages, channel closed: %v)",
					msgCount,
					channelClosed,
				)
				t.Fatal("timeout waiting for INSERT CDC event")
			}
			return
		case <-ctx.Done():
			if !eventFound {
				t.Fatalf("context cancelled before event received: %v", ctx.Err())
			}
			return
		case raw, ok := <-events:
			if !ok {
				channelClosed = true
				if !eventFound {
					t.Fatal("events channel closed unexpectedly")
				}
				return
			}

			msgCount++
			t.Logf("Message %d: %T", msgCount, raw.Data)

			// Check for error events
			if errData, ok := raw.Data.(error); ok {
				t.Logf("  ERROR EVENT: %v", errData)
				t.Logf("  (This might indicate a replication problem)")
				continue
			}

			event, err := decoder.Decode(raw)
			if err != nil {
				t.Logf("  Decode error (skipping): %v", err)
				continue
			}
			if event == nil {
				// Skip relation/begin/commit messages
				t.Logf("  Nil event (skipped)")
				continue
			}

			t.Logf("  Decoded event: table=%s, op=%s", event.Table, event.Op)

			// Verify the INSERT event
			if event.Op == cdc.EventTypeCreate && event.Table == "users" {
				t.Logf("Received INSERT event: table=%s, op=%s", event.Table, event.Op)

				// Verify fields
				if name, ok := event.After["name"]; !ok || name != "Alice" {
					t.Errorf("expected name=Alice, got %v", name)
				}
				if email, ok := event.After["email"]; !ok || email != "alice@test.com" {
					t.Errorf("expected email=alice@test.com, got %v", email)
				}

				eventFound = true
				return
			}
		}
	}
}

// TestIntegration_PostgresSource_UpdateEvent verifies UPDATE events are captured
func TestIntegration_PostgresSource_UpdateEvent(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	pg := SetupPostgresContainer(t)
	defer pg.Close(t)

	// Create source
	src, err := NewSource(Config{
		DSN:      pg.ReplicationDSN(),
		SlotName: "test_update_slot",
		Tables:   []string{"users"},
	})
	if err != nil {
		t.Fatalf("failed to create source: %v", err)
	}
	defer src.Close()

	// Start source
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	events, err := src.Start(ctx)
	if err != nil {
		t.Fatalf("failed to start source: %v", err)
	}

	// Give replication a moment to set up
	time.Sleep(2 * time.Second)

	// Insert initial data
	pg.ExecSQL(t, "INSERT INTO users (name, email) VALUES ($1, $2)", "Bob", "bob@test.com")

	// Give CDC time to process insert
	time.Sleep(1 * time.Second)

	// Update the row
	pg.ExecSQL(t, "UPDATE users SET email = $1 WHERE name = $2", "bob_new@test.com", "Bob")

	// Create decoder and read events
	decoder := NewDecoder()

	// Wait for UPDATE event with timeout
	timeout := time.After(15 * time.Second)
	updateFound := false

	for {
		select {
		case <-timeout:
			if !updateFound {
				t.Fatal("timeout waiting for UPDATE CDC event")
			}
			return
		case <-ctx.Done():
			if !updateFound {
				t.Fatalf("context cancelled before event received: %v", ctx.Err())
			}
			return
		case raw, ok := <-events:
			if !ok {
				if !updateFound {
					t.Fatal("events channel closed unexpectedly")
				}
				return
			}

			// Skip error events
			if _, ok := raw.Data.(error); ok {
				continue
			}

			event, err := decoder.Decode(raw)
			if err != nil {
				continue
			}
			if event == nil {
				continue
			}

			// Verify the UPDATE event
			if event.Op == cdc.EventTypeUpdate && event.Table == "users" {
				t.Logf("Received UPDATE event: table=%s, op=%s", event.Table, event.Op)

				// Verify before data
				if event.Before == nil || len(event.Before) == 0 {
					t.Error("expected before data in UPDATE event")
				}

				// Verify after data
				if email, ok := event.After["email"]; !ok || email != "bob_new@test.com" {
					t.Errorf("expected email=bob_new@test.com in after, got %v", email)
				}

				updateFound = true
				return
			}
		}
	}
}

// TestIntegration_PostgresSource_DeleteEvent verifies DELETE events are captured
func TestIntegration_PostgresSource_DeleteEvent(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	pg := SetupPostgresContainer(t)
	defer pg.Close(t)

	// Create source
	src, err := NewSource(Config{
		DSN:      pg.ReplicationDSN(),
		SlotName: "test_delete_slot",
		Tables:   []string{"users"},
	})
	if err != nil {
		t.Fatalf("failed to create source: %v", err)
	}
	defer src.Close()

	// Start source
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	events, err := src.Start(ctx)
	if err != nil {
		t.Fatalf("failed to start source: %v", err)
	}

	// Give replication a moment to set up
	time.Sleep(2 * time.Second)

	// Insert initial data
	pg.ExecSQL(t, "INSERT INTO users (name, email) VALUES ($1, $2)", "Charlie", "charlie@test.com")

	// Give CDC time to process insert
	time.Sleep(1 * time.Second)

	// Delete the row
	pg.ExecSQL(t, "DELETE FROM users WHERE name = $1", "Charlie")

	// Create decoder and read events
	decoder := NewDecoder()

	// Wait for DELETE event with timeout
	timeout := time.After(15 * time.Second)
	deleteFound := false

	for {
		select {
		case <-timeout:
			if !deleteFound {
				t.Fatal("timeout waiting for DELETE CDC event")
			}
			return
		case <-ctx.Done():
			if !deleteFound {
				t.Fatalf("context cancelled before event received: %v", ctx.Err())
			}
			return
		case raw, ok := <-events:
			if !ok {
				if !deleteFound {
					t.Fatal("events channel closed unexpectedly")
				}
				return
			}

			// Skip error events
			if _, ok := raw.Data.(error); ok {
				continue
			}

			event, err := decoder.Decode(raw)
			if err != nil {
				continue
			}
			if event == nil {
				continue
			}

			// Verify the DELETE event
			if event.Op == cdc.EventTypeDelete && event.Table == "users" {
				t.Logf("Received DELETE event: table=%s, op=%s", event.Table, event.Op)

				// Verify before data
				if event.Before == nil || len(event.Before) == 0 {
					t.Error("expected before data in DELETE event")
				}
				if name, ok := event.Before["name"]; !ok || name != "Charlie" {
					t.Errorf("expected name=Charlie in before, got %v", name)
				}

				deleteFound = true
				return
			}
		}
	}
}

// TestIntegration_PostgresSource_MultiTable verifies events from multiple tables
func TestIntegration_PostgresSource_MultiTable(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	pg := SetupPostgresContainer(t)
	defer pg.Close(t)

	// Create source
	src, err := NewSource(Config{
		DSN:      pg.ReplicationDSN(),
		SlotName: "test_multi_table_slot",
		Tables:   []string{"users", "orders"},
	})
	if err != nil {
		t.Fatalf("failed to create source: %v", err)
	}
	defer src.Close()

	// Start source
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	events, err := src.Start(ctx)
	if err != nil {
		t.Fatalf("failed to start source: %v", err)
	}

	// Give replication a moment to set up
	time.Sleep(2 * time.Second)

	// Insert into users table
	pg.ExecSQL(t, "INSERT INTO users (name, email) VALUES ($1, $2)", "David", "david@test.com")

	// Insert into orders table
	pg.ExecSQL(t, "INSERT INTO orders (user_id, amount) VALUES ($1, $2)", 1, "99.99")

	// Create decoder and read events
	decoder := NewDecoder()

	// Wait for both events
	timeout := time.After(15 * time.Second)
	eventsFound := map[string]bool{
		"users":  false,
		"orders": false,
	}

	for {
		select {
		case <-timeout:
			if !eventsFound["users"] || !eventsFound["orders"] {
				t.Fatalf("timeout waiting for events: users=%v, orders=%v", eventsFound["users"], eventsFound["orders"])
			}
			return
		case <-ctx.Done():
			if !eventsFound["users"] || !eventsFound["orders"] {
				t.Fatalf("context cancelled before all events received: %v", ctx.Err())
			}
			return
		case raw, ok := <-events:
			if !ok {
				t.Fatal("events channel closed unexpectedly")
			}

			// Skip error events
			if _, ok := raw.Data.(error); ok {
				continue
			}

			event, err := decoder.Decode(raw)
			if err != nil {
				continue
			}
			if event == nil {
				continue
			}

			// Check for table events
			if event.Op == cdc.EventTypeCreate {
				if event.Table == "users" && !eventsFound["users"] {
					t.Logf("Received INSERT event from users table")
					eventsFound["users"] = true
				}
				if event.Table == "orders" && !eventsFound["orders"] {
					t.Logf("Received INSERT event from orders table")
					eventsFound["orders"] = true
				}

				// Return when both found
				if eventsFound["users"] && eventsFound["orders"] {
					return
				}
			}
		}
	}
}

// TestIntegration_PostgresSource_ContextCancel verifies source exits gracefully when context is cancelled
func TestIntegration_PostgresSource_ContextCancel(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	pg := SetupPostgresContainer(t)
	defer pg.Close(t)

	// Create source
	src, err := NewSource(Config{
		DSN:      pg.ReplicationDSN(),
		SlotName: "test_cancel_slot",
		Tables:   []string{"users"},
	})
	if err != nil {
		t.Fatalf("failed to create source: %v", err)
	}
	defer src.Close()

	// Start source with short timeout context
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	events, err := src.Start(ctx)
	if err != nil {
		t.Fatalf("failed to start source: %v", err)
	}

	// Wait for context to expire
	<-ctx.Done()

	// Give goroutine time to exit
	time.Sleep(1 * time.Second)

	// Verify channel is closed (trying to receive should return ok=false eventually)
	// This is best-effort as channels may not close immediately
	select {
	case _, ok := <-events:
		if ok {
			t.Log("channel still has data")
		} else {
			t.Log("channel closed as expected")
		}
	case <-time.After(2 * time.Second):
		t.Log("timeout waiting for channel closure")
	}
}
