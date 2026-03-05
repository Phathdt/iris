package postgres

import (
	"context"
	"fmt"

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

	// Determine start LSN
	var startLSN pglogrepl.LSN
	if s.config.StartOffset != nil && !s.config.StartOffset.IsZero() {
		startLSN = pglogrepl.LSN(s.config.StartOffset.LSN)
	} else {
		// Get current WAL position
		sysID, err := pglogrepl.IdentifySystem(ctx, s.replConn)
		if err != nil {
			s.replConn.Close(ctx)
			return nil, fmt.Errorf("failed to identify system: %w", err)
		}
		startLSN = sysID.XLogPos
	}

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
				case 'w': // XLogData (WAL data)
					walData := m.Data[1:] // Skip the 'w' prefix
					if len(walData) > 0 {
						// Parse the logical replication message
						logicalMsg, err := pglogrepl.Parse(walData)
						if err != nil {
							// Skip unparseable messages
							continue
						}

						// Send as raw event
						s.events <- cdc.RawEvent{
							Data: logicalMsg,
							Meta: map[string]string{
								"type": fmt.Sprintf("%T", logicalMsg),
							},
						}
					}
				}
			}
		}
	}
}

// Ack acknowledges the processed offset by sending standby status update
func (s *PostgresSource) Ack(offset cdc.Offset) error {
	if s.replConn == nil {
		return fmt.Errorf("connection not initialized")
	}

	// Send standby status update to advance restart_lsn
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
