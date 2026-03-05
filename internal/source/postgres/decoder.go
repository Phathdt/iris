package postgres

import (
	"fmt"
	"time"

	"iris/pkg/cdc"

	"github.com/jackc/pglogrepl"
)

// PostgresDecoder converts PostgreSQL WAL messages to unified CDC events
type PostgresDecoder struct {
	relations  map[uint32]*pglogrepl.RelationMessage
	commitTime time.Time // Track commit time from CommitMessage
}

// NewDecoder creates a new PostgreSQL decoder
func NewDecoder() *PostgresDecoder {
	return &PostgresDecoder{
		relations: make(map[uint32]*pglogrepl.RelationMessage),
	}
}

// Decode converts a raw event to a unified CDC event
func (d *PostgresDecoder) Decode(raw cdc.RawEvent) (*cdc.Event, error) {
	msg, ok := raw.Data.(pglogrepl.Message)
	if !ok {
		return nil, fmt.Errorf("unexpected message type: %T", raw.Data)
	}

	// Handle different message types based on pgoutput protocol
	switch m := msg.(type) {
	case *pglogrepl.RelationMessage:
		// Cache relation schema info
		d.relations[m.RelationID] = m
		return nil, nil // Relation messages are cached, not emitted as events

	case *pglogrepl.BeginMessage:
		// Transaction start - can be used for batching
		return nil, nil

	case *pglogrepl.CommitMessage:
		// Store commit time for subsequent operation messages
		d.commitTime = m.CommitTime
		return nil, nil

	case *pglogrepl.InsertMessage:
		return d.parseInsert(m)

	case *pglogrepl.UpdateMessage:
		return d.parseUpdate(m)

	case *pglogrepl.DeleteMessage:
		return d.parseDelete(m)

	case *pglogrepl.TruncateMessage:
		// TRUNCATE is not typically needed for CDC
		return nil, nil

	default:
		return nil, fmt.Errorf("unknown message type: %T", msg)
	}
}

// parseInsert handles INSERT operations
func (d *PostgresDecoder) parseInsert(msg *pglogrepl.InsertMessage) (*cdc.Event, error) {
	relation, ok := d.relations[msg.RelationID]
	if !ok {
		return nil, fmt.Errorf("unknown relation ID: %d", msg.RelationID)
	}

	// Parse the new row data
	after, err := parseTuple(msg.Tuple, relation)
	if err != nil {
		return nil, fmt.Errorf("failed to parse tuple: %w", err)
	}

	return &cdc.Event{
		Source: "postgres",
		Table:  getTableName(relation),
		Op:     cdc.EventTypeCreate,
		TS:     d.commitTime,
		After:  after,
		Raw:    msg,
	}, nil
}

// parseUpdate handles UPDATE operations
func (d *PostgresDecoder) parseUpdate(msg *pglogrepl.UpdateMessage) (*cdc.Event, error) {
	relation, ok := d.relations[msg.RelationID]
	if !ok {
		return nil, fmt.Errorf("unknown relation ID: %d", msg.RelationID)
	}

	var before map[string]any
	var err error

	// Parse old row data if available
	if msg.OldTuple != nil {
		before, err = parseTuple(msg.OldTuple, relation)
		if err != nil {
			return nil, fmt.Errorf("failed to parse old tuple: %w", err)
		}
	}

	// Parse new row data
	after, err := parseTuple(msg.NewTuple, relation)
	if err != nil {
		return nil, fmt.Errorf("failed to parse new tuple: %w", err)
	}

	return &cdc.Event{
		Source: "postgres",
		Table:  getTableName(relation),
		Op:     cdc.EventTypeUpdate,
		TS:     d.commitTime,
		Before: before,
		After:  after,
		Raw:    msg,
	}, nil
}

// parseDelete handles DELETE operations
func (d *PostgresDecoder) parseDelete(msg *pglogrepl.DeleteMessage) (*cdc.Event, error) {
	relation, ok := d.relations[msg.RelationID]
	if !ok {
		return nil, fmt.Errorf("unknown relation ID: %d", msg.RelationID)
	}

	// Parse old row data
	before, err := parseTuple(msg.OldTuple, relation)
	if err != nil {
		return nil, fmt.Errorf("failed to parse old tuple: %w", err)
	}

	return &cdc.Event{
		Source: "postgres",
		Table:  getTableName(relation),
		Op:     cdc.EventTypeDelete,
		TS:     d.commitTime,
		Before: before,
		Raw:    msg,
	}, nil
}

// getTableName extracts the full table name from relation
func getTableName(rel *pglogrepl.RelationMessage) string {
	if rel.Namespace != "" && rel.Namespace != "public" {
		return fmt.Sprintf("%s.%s", rel.Namespace, rel.RelationName)
	}
	return rel.RelationName
}
