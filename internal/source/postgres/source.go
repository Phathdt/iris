package postgres

import (
	"context"
	"fmt"
	"sync/atomic"

	"iris/pkg/cdc"

	"github.com/jackc/pglogrepl"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgproto3"
)

// PostgresSource emits CDC events from PostgreSQL logical replication
type PostgresSource struct {
	config   Config
	conn     *pgconn.PgConn
	replConn *pgconn.PgConn
	events   chan cdc.RawEvent
	slotName string

	// lastAckedLSN tracks the last LSN confirmed to PG via Ack().
	// Used by keepalive handler to avoid advancing the slot past unprocessed events.
	lastAckedLSN atomic.Uint64
}

// NewSource creates a new PostgreSQL source
func NewSource(cfg Config) (*PostgresSource, error) {
	if cfg.SlotName == "" {
		return nil, fmt.Errorf("slot name is required")
	}
	if cfg.DSN == "" {
		return nil, fmt.Errorf("DSN is required")
	}

	return &PostgresSource{
		config:   cfg,
		events:   make(chan cdc.RawEvent, 1024),
		slotName: cfg.SlotName,
	}, nil
}

// Start begins logical replication and returns the raw event channel
func (s *PostgresSource) Start(ctx context.Context) (<-chan cdc.RawEvent, error) {
	var err error

	// Connect to PostgreSQL for replication
	s.replConn, err = pgconn.Connect(ctx, s.config.DSN)
	if err != nil {
		return nil, fmt.Errorf("failed to connect to PostgreSQL: %w", err)
	}

	// Create replication slot if it doesn't exist
	err = s.ensureReplicationSlot(ctx)
	if err != nil {
		s.replConn.Close(ctx)
		return nil, fmt.Errorf("failed to create replication slot: %w", err)
	}

	// Determine start LSN.
	// Use explicit StartOffset if provided, otherwise pass LSN(0) so PG resumes
	// from the slot's confirmed_flush_lsn automatically.
	var startLSN pglogrepl.LSN
	if s.config.StartOffset != nil && !s.config.StartOffset.IsZero() {
		startLSN = pglogrepl.LSN(s.config.StartOffset.LSN)
	}
	// When startLSN is 0, PG uses confirmed_flush_lsn from the replication slot.
	// This is the correct behavior for resuming after restart.

	// Start logical replication with pgoutput plugin
	err = pglogrepl.StartReplication(
		ctx,
		s.replConn,
		s.slotName,
		startLSN,
		pglogrepl.StartReplicationOptions{
			PluginArgs: []string{
				"proto_version '1'",
				"publication_names 'pglogrepl_publication'",
			},
		},
	)
	if err != nil {
		s.replConn.Close(ctx)
		return nil, fmt.Errorf("failed to start replication: %w", err)
	}

	// Start goroutine to receive WAL messages
	go s.receiveWALMessages(ctx)

	return s.events, nil
}

// ensureReplicationSlot creates the replication slot if it doesn't exist
func (s *PostgresSource) ensureReplicationSlot(ctx context.Context) error {
	// Try to create the slot (will fail if it already exists)
	_, err := pglogrepl.CreateReplicationSlot(
		ctx,
		s.replConn,
		s.slotName,
		"pgoutput",
		pglogrepl.CreateReplicationSlotOptions{
			Temporary: false,
		},
	)
	if err != nil {
		// Check if it's a "already exists" error
		if pgconnErr, ok := err.(*pgconn.PgError); ok {
			if pgconnErr.Code == "42710" { // duplicate_object
				return nil // Slot already exists
			}
		}
		return err
	}
	return nil
}

// receiveWALMessages reads WAL messages and sends them to the events channel
func (s *PostgresSource) receiveWALMessages(ctx context.Context) {
	defer close(s.events)

	for {
		select {
		case <-ctx.Done():
			return
		default:
			// Receive a message from the replication connection
			msg, err := s.replConn.ReceiveMessage(ctx)
			if err != nil {
				select {
				case <-ctx.Done():
					return
				default:
					// Send error as raw event
					s.events <- cdc.RawEvent{
						Data: err,
						Meta: map[string]string{"error": "receive_failed"},
					}
					return
				}
			}

			// Handle CopyData messages which contain WAL data
			switch m := msg.(type) {
			case *pgproto3.CopyData:
				if len(m.Data) == 0 {
					continue
				}

				// Check message type (first byte indicates type)
				switch m.Data[0] {
				case pglogrepl.XLogDataByteID:
					// Parse XLogData envelope (24-byte header: WALStart + ServerWALEnd + ServerTime)
					xld, err := pglogrepl.ParseXLogData(m.Data[1:])
					if err != nil {
						continue
					}

					// Parse the logical replication message from WAL data
					logicalMsg, err := pglogrepl.Parse(xld.WALData)
					if err != nil {
						continue
					}

					// Send as raw event with LSN for offset tracking
					s.events <- cdc.RawEvent{
						Data: logicalMsg,
						Meta: map[string]string{
							"type": fmt.Sprintf("%T", logicalMsg),
							"lsn":  fmt.Sprintf("%d", uint64(xld.WALStart)),
						},
					}

				case pglogrepl.PrimaryKeepaliveMessageByteID:
					pkm, err := pglogrepl.ParsePrimaryKeepaliveMessage(m.Data[1:])
					if err != nil {
						continue
					}
					// Reply to keepalive using the last acked LSN, not ServerWALEnd.
					// Using ServerWALEnd would advance the slot past events we haven't processed yet.
					if pkm.ReplyRequested {
						replyLSN := pglogrepl.LSN(s.lastAckedLSN.Load())
						pglogrepl.SendStandbyStatusUpdate(
							ctx,
							s.replConn,
							pglogrepl.StandbyStatusUpdate{
								WALWritePosition: replyLSN,
							},
						)
					}
				}
			}
		}
	}
}

// Ack acknowledges the processed offset by sending standby status update.
// This advances the replication slot's confirmed_flush_lsn in PostgreSQL,
// which is persisted server-side and used to resume on reconnect.
func (s *PostgresSource) Ack(offset cdc.Offset) error {
	if s.replConn == nil {
		return fmt.Errorf("connection not initialized")
	}

	// Track last acked LSN for keepalive replies
	s.lastAckedLSN.Store(offset.LSN)

	// Send standby status update to advance confirmed_flush_lsn
	return pglogrepl.SendStandbyStatusUpdate(
		context.Background(),
		s.replConn,
		pglogrepl.StandbyStatusUpdate{
			WALWritePosition: pglogrepl.LSN(offset.LSN),
			WALFlushPosition: pglogrepl.LSN(offset.LSN),
			WALApplyPosition: pglogrepl.LSN(offset.LSN),
		},
	)
}

// Close stops replication and closes connections
func (s *PostgresSource) Close() error {
	var errs []error

	if s.replConn != nil {
		if err := s.replConn.Close(context.Background()); err != nil {
			errs = append(errs, fmt.Errorf("failed to close repl conn: %w", err))
		}
	}

	if s.conn != nil {
		if err := s.conn.Close(context.Background()); err != nil {
			errs = append(errs, fmt.Errorf("failed to close conn: %w", err))
		}
	}

	if len(errs) > 0 {
		return fmt.Errorf("errors during close: %v", errs)
	}

	return nil
}
