package cdc

import (
	"testing"
	"time"
)

func TestOffset_IsZero(t *testing.T) {
	tests := []struct {
		name   string
		offset *Offset
		want   bool
	}{
		{
			name:   "nil offset",
			offset: nil,
			want:   true,
		},
		{
			name:   "zero LSN",
			offset: &Offset{LSN: 0},
			want:   true,
		},
		{
			name:   "non-zero LSN",
			offset: &Offset{LSN: 100},
			want:   false,
		},
		{
			name:   "LSN with commit time",
			offset: &Offset{LSN: 100, CommitTime: time.Now()},
			want:   false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := tt.offset.IsZero(); got != tt.want {
				t.Errorf("Offset.IsZero() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestOffset_String(t *testing.T) {
	tests := []struct {
		name   string
		offset *Offset
		want   string
	}{
		{
			name:   "nil offset",
			offset: nil,
			want:   "",
		},
		{
			name:   "zero offset",
			offset: &Offset{LSN: 0},
			want:   "0/0",
		},
		{
			name:   "small LSN",
			offset: &Offset{LSN: 1},
			want:   "0/1",
		},
		{
			name:   "LSN with high bits",
			offset: &Offset{LSN: 0x1234567890ABCDEF},
			want:   "305419896/2427178479", // 0x12345678 / 0x90ABCDEF
		},
		{
			name:   "typical WAL position",
			offset: &Offset{LSN: 0x0000000100000000},
			want:   "1/0",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := tt.offset.String(); got != tt.want {
				t.Errorf("Offset.String() = %q, want %q", got, tt.want)
			}
		})
	}
}

func TestOffset_WithRaw(t *testing.T) {
	rawData := map[string]string{"key": "value"}
	offset := &Offset{
		LSN: 100,
		Raw: rawData,
	}

	if offset.IsZero() {
		t.Error("Offset with LSN=100 should not be zero")
	}

	// Verify Raw is accessible (can't directly compare maps, check type and presence)
	if offset.Raw == nil {
		t.Error("Raw data should not be nil")
	}
	// Type assertion to verify the map content
	if m, ok := offset.Raw.(map[string]string); ok {
		if m["key"] != "value" {
			t.Errorf("Raw data value mismatch: got %v", m["key"])
		}
	} else {
		t.Error("Raw data is not map[string]string")
	}
}

func TestOffset_WithCommitTime(t *testing.T) {
	now := time.Now()
	offset := &Offset{
		LSN:        100,
		CommitTime: now,
	}

	if !offset.CommitTime.Equal(now) {
		t.Errorf("CommitTime mismatch: got %v, want %v", offset.CommitTime, now)
	}

	// String should still work with commit time
	str := offset.String()
	if str == "" {
		t.Error("String() should return non-empty string for non-zero offset")
	}
}
