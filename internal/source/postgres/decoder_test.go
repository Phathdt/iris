package postgres

import (
	"testing"
	"time"

	"iris/pkg/cdc"

	"github.com/jackc/pglogrepl"
)

func TestPostgresDecoder_New(t *testing.T) {
	dec := NewDecoder()
	if dec == nil {
		t.Fatal("NewDecoder() returned nil")
	}
	if dec.relations == nil {
		t.Error("relations map should be initialized")
	}
	if len(dec.relations) != 0 {
		t.Error("relations map should be empty initially")
	}
}

func TestPostgresDecoder_Decode_UnknownType(t *testing.T) {
	dec := NewDecoder()

	raw := cdc.RawEvent{
		Data: "unknown type",
	}

	_, err := dec.Decode(raw)
	if err == nil {
		t.Fatal("expected error for unknown message type")
	}
	if err.Error() != "unexpected message type: string" {
		t.Errorf("unexpected error message: %v", err)
	}
}

func TestPostgresDecoder_RelationMessage(t *testing.T) {
	dec := NewDecoder()

	relation := &pglogrepl.RelationMessage{
		RelationID:   12345,
		Namespace:    "public",
		RelationName: "users",
		Columns: []*pglogrepl.RelationMessageColumn{
			{Name: "id", DataType: 23},   // int4 OID
			{Name: "name", DataType: 25}, // text OID
		},
	}

	raw := cdc.RawEvent{
		Data: relation,
	}

	event, err := dec.Decode(raw)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if event != nil {
		t.Error("RelationMessage should return nil event (cached only)")
	}

	// Verify relation was cached
	if _, exists := dec.relations[12345]; !exists {
		t.Error("relation should be cached")
	}
}

func TestPostgresDecoder_BeginMessage(t *testing.T) {
	dec := NewDecoder()

	begin := &pglogrepl.BeginMessage{
		FinalLSN:   pglogrepl.LSN(100),
		CommitTime: time.Now(),
	}

	raw := cdc.RawEvent{
		Data: begin,
	}

	event, err := dec.Decode(raw)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if event != nil {
		t.Error("BeginMessage should return nil event")
	}
}

func TestPostgresDecoder_CommitMessage(t *testing.T) {
	dec := NewDecoder()

	commitTime := time.Date(2026, 3, 5, 12, 0, 0, 0, time.UTC)
	commit := &pglogrepl.CommitMessage{
		CommitTime: commitTime,
		CommitLSN:  pglogrepl.LSN(200),
	}

	raw := cdc.RawEvent{
		Data: commit,
	}

	event, err := dec.Decode(raw)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if event != nil {
		t.Error("CommitMessage should return nil event")
	}

	// Verify commit time was stored
	if !dec.commitTime.Equal(commitTime) {
		t.Errorf("commitTime not stored: got %v, want %v", dec.commitTime, commitTime)
	}
}

func TestPostgresDecoder_InsertMessage(t *testing.T) {
	dec := NewDecoder()

	// First cache the relation
	relation := &pglogrepl.RelationMessage{
		RelationID:   100,
		Namespace:    "public",
		RelationName: "users",
		Columns: []*pglogrepl.RelationMessageColumn{
			{Name: "id", DataType: 23},   // int4 OID
			{Name: "name", DataType: 25}, // text OID
		},
	}
	dec.relations[100] = relation

	// Set commit time
	dec.commitTime = time.Date(2026, 3, 5, 12, 0, 0, 0, time.UTC)

	// Create insert message
	insert := &pglogrepl.InsertMessage{
		RelationID: 100,
		Tuple: &pglogrepl.TupleData{
			Columns: []*pglogrepl.TupleDataColumn{
				{DataType: 't', Data: []byte("1")},
				{DataType: 't', Data: []byte("alice")},
			},
		},
	}

	raw := cdc.RawEvent{
		Data: insert,
	}

	event, err := dec.Decode(raw)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if event == nil {
		t.Fatal("expected event for InsertMessage")
	}

	// Verify event fields
	if event.Source != "postgres" {
		t.Errorf("unexpected source: %v", event.Source)
	}
	if event.Table != "users" {
		t.Errorf("unexpected table: %v", event.Table)
	}
	if event.Op != cdc.EventTypeCreate {
		t.Errorf("unexpected op: %v", event.Op)
	}
	if event.After["id"] != int64(1) {
		t.Errorf("unexpected after.id: %v", event.After["id"])
	}
	if event.After["name"] != "alice" {
		t.Errorf("unexpected after.name: %v", event.After["name"])
	}
	if event.Before != nil {
		t.Error("Before should be nil for insert")
	}
}

func TestPostgresDecoder_UpdateMessage(t *testing.T) {
	dec := NewDecoder()

	// Cache relation
	relation := &pglogrepl.RelationMessage{
		RelationID:   200,
		Namespace:    "public",
		RelationName: "users",
		Columns: []*pglogrepl.RelationMessageColumn{
			{Name: "id", DataType: 23},   // int4 OID
			{Name: "name", DataType: 25}, // text OID
		},
	}
	dec.relations[200] = relation
	dec.commitTime = time.Date(2026, 3, 5, 12, 0, 0, 0, time.UTC)

	update := &pglogrepl.UpdateMessage{
		RelationID: 200,
		OldTuple: &pglogrepl.TupleData{
			Columns: []*pglogrepl.TupleDataColumn{
				{DataType: 't', Data: []byte("1")},
				{DataType: 't', Data: []byte("old-name")},
			},
		},
		NewTuple: &pglogrepl.TupleData{
			Columns: []*pglogrepl.TupleDataColumn{
				{DataType: 't', Data: []byte("1")},
				{DataType: 't', Data: []byte("new-name")},
			},
		},
	}

	raw := cdc.RawEvent{
		Data: update,
	}

	event, err := dec.Decode(raw)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if event == nil {
		t.Fatal("expected event for UpdateMessage")
	}

	if event.Op != cdc.EventTypeUpdate {
		t.Errorf("unexpected op: %v", event.Op)
	}
	if event.Before["name"] != "old-name" {
		t.Errorf("unexpected before.name: %v", event.Before["name"])
	}
	if event.After["name"] != "new-name" {
		t.Errorf("unexpected after.name: %v", event.After["name"])
	}
}

func TestPostgresDecoder_DeleteMessage(t *testing.T) {
	dec := NewDecoder()

	// Cache relation
	relation := &pglogrepl.RelationMessage{
		RelationID:   300,
		Namespace:    "public",
		RelationName: "orders",
		Columns: []*pglogrepl.RelationMessageColumn{
			{Name: "id", DataType: 23}, // int4 OID
		},
	}
	dec.relations[300] = relation
	dec.commitTime = time.Date(2026, 3, 5, 12, 0, 0, 0, time.UTC)

	delete := &pglogrepl.DeleteMessage{
		RelationID: 300,
		OldTuple: &pglogrepl.TupleData{
			Columns: []*pglogrepl.TupleDataColumn{
				{DataType: 't', Data: []byte("999")},
			},
		},
	}

	raw := cdc.RawEvent{
		Data: delete,
	}

	event, err := dec.Decode(raw)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if event == nil {
		t.Fatal("expected event for DeleteMessage")
	}

	if event.Op != cdc.EventTypeDelete {
		t.Errorf("unexpected op: %v", event.Op)
	}
	if event.Before["id"] != int64(999) {
		t.Errorf("unexpected before.id: %v", event.Before["id"])
	}
	if event.After != nil {
		t.Error("After should be nil for delete")
	}
}

func TestPostgresDecoder_UnknownRelationID(t *testing.T) {
	dec := NewDecoder()
	dec.commitTime = time.Now()

	insert := &pglogrepl.InsertMessage{
		RelationID: 99999, // Not cached
		Tuple:      &pglogrepl.TupleData{},
	}

	raw := cdc.RawEvent{
		Data: insert,
	}

	_, err := dec.Decode(raw)
	if err == nil {
		t.Fatal("expected error for unknown relation ID")
	}
	if err.Error() != "unknown relation ID: 99999" {
		t.Errorf("unexpected error: %v", err)
	}
}

func TestPostgresDecoder_TruncateMessage(t *testing.T) {
	dec := NewDecoder()

	truncate := &pglogrepl.TruncateMessage{}

	raw := cdc.RawEvent{
		Data: truncate,
	}

	event, err := dec.Decode(raw)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if event != nil {
		t.Error("TruncateMessage should return nil event")
	}
}

func TestGetTableName(t *testing.T) {
	tests := []struct {
		name     string
		relation *pglogrepl.RelationMessage
		want     string
	}{
		{
			name: "public schema",
			relation: &pglogrepl.RelationMessage{
				Namespace:    "public",
				RelationName: "users",
			},
			want: "users",
		},
		{
			name: "empty schema",
			relation: &pglogrepl.RelationMessage{
				Namespace:    "",
				RelationName: "orders",
			},
			want: "orders",
		},
		{
			name: "custom schema",
			relation: &pglogrepl.RelationMessage{
				Namespace:    "app",
				RelationName: "products",
			},
			want: "app.products",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := getTableName(tt.relation)
			if got != tt.want {
				t.Errorf("getTableName() = %v, want %v", got, tt.want)
			}
		})
	}
}
