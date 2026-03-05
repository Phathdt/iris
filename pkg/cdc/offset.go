package cdc

import (
	"fmt"
	"time"
)

// Offset tracks position in the source stream
// Uses interface{} for Raw to avoid importing pgx in the base cdc package
type Offset struct {
	// LSN is the PostgreSQL WAL position (stored as uint64 to avoid pgx import)
	LSN uint64

	// CommitTime from WAL
	CommitTime time.Time

	// Raw holds source-specific offset data
	Raw any
}

// String returns human-readable offset
func (o *Offset) String() string {
	if o == nil {
		return ""
	}
	return fmt.Sprintf("%d/%d", o.LSN>>32, o.LSN&0xFFFFFFFF)
}

// IsZero returns true if offset is uninitialized
func (o *Offset) IsZero() bool {
	return o == nil || o.LSN == 0
}
