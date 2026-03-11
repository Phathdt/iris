package postgres

import (
	"fmt"
	"strconv"
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

	var event *cdc.Event
	var err error

	// Handle different message types based on pgoutput protocol
	switch m := msg.(type) {
	case *pglogrepl.RelationMessage:
		// Cache relation schema info
		d.relations[m.RelationID] = m
		return nil, nil

	case *pglogrepl.BeginMessage:
		return nil, nil

	case *pglogrepl.CommitMessage:
		d.commitTime = m.CommitTime
		return nil, nil

	case *pglogrepl.InsertMessage:
		event, err = d.parseInsert(m)

	case *pglogrepl.UpdateMessage:
		event, err = d.parseUpdate(m)

	case *pglogrepl.DeleteMessage:
		event, err = d.parseDelete(m)

	case *pglogrepl.TruncateMessage:
		return nil, nil

	default:
		return nil, fmt.Errorf("unknown message type: %T", msg)
	}

	if err != nil || event == nil {
		return event, err
	}

	// Attach source offset to event
	event.Offset = extractOffset(raw.Meta)
	if event.Offset != nil {
		event.Offset.CommitTime = d.commitTime
	}

	return event, nil
}

// extractOffset reads the LSN from RawEvent.Meta if present.
func extractOffset(meta map[string]string) *cdc.Offset {
	if meta == nil {
		return nil
	}
	lsnStr, ok := meta["lsn"]
	if !ok || lsnStr == "" {
		return nil
	}
	lsn, err := strconv.ParseUint(lsnStr, 10, 64)
	if err != nil {
		return nil
	}
	return &cdc.Offset{LSN: lsn}
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
