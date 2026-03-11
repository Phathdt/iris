package file

import (
	"os"
	"path/filepath"
	"testing"
	"time"

	"iris/pkg/cdc"
)

func TestLoad_NonexistentFile(t *testing.T) {
	store := NewStore(filepath.Join(t.TempDir(), "missing.json"))

	offset, err := store.Load()
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !offset.IsZero() {
		t.Errorf("expected zero offset, got LSN=%d", offset.LSN)
	}
}

func TestMarkFlushLoad_RoundTrip(t *testing.T) {
	path := filepath.Join(t.TempDir(), "offset.json")
	store := NewStore(path)

	commitTime := time.Date(2026, 3, 11, 6, 0, 0, 0, time.UTC)
	store.Mark(cdc.Offset{LSN: 12345, CommitTime: commitTime})

	if err := store.Flush(); err != nil {
		t.Fatalf("flush error: %v", err)
	}

	// Load with a new store instance
	store2 := NewStore(path)
	offset, err := store2.Load()
	if err != nil {
		t.Fatalf("load error: %v", err)
	}
	if offset.LSN != 12345 {
		t.Errorf("expected LSN=12345, got %d", offset.LSN)
	}
	if !offset.CommitTime.Equal(commitTime) {
		t.Errorf("expected CommitTime=%v, got %v", commitTime, offset.CommitTime)
	}
}

func TestMark_WithoutFlush_NotPersisted(t *testing.T) {
	path := filepath.Join(t.TempDir(), "offset.json")
	store := NewStore(path)

	store.Mark(cdc.Offset{LSN: 999})
	// No flush

	store2 := NewStore(path)
	offset, err := store2.Load()
	if err != nil {
		t.Fatalf("load error: %v", err)
	}
	if !offset.IsZero() {
		t.Errorf("expected zero offset without flush, got LSN=%d", offset.LSN)
	}
}

func TestMultipleMarks_OnlyLatestPersisted(t *testing.T) {
	path := filepath.Join(t.TempDir(), "offset.json")
	store := NewStore(path)

	store.Mark(cdc.Offset{LSN: 100})
	store.Mark(cdc.Offset{LSN: 200})
	store.Mark(cdc.Offset{LSN: 300})

	if err := store.Flush(); err != nil {
		t.Fatalf("flush error: %v", err)
	}

	store2 := NewStore(path)
	offset, err := store2.Load()
	if err != nil {
		t.Fatalf("load error: %v", err)
	}
	if offset.LSN != 300 {
		t.Errorf("expected LSN=300, got %d", offset.LSN)
	}
}

func TestFlush_WhenNotDirty_NoOp(t *testing.T) {
	path := filepath.Join(t.TempDir(), "offset.json")
	store := NewStore(path)

	// Flush without any Mark should be a no-op
	if err := store.Flush(); err != nil {
		t.Fatalf("flush error: %v", err)
	}

	// File should not exist
	if _, err := os.Stat(path); !os.IsNotExist(err) {
		t.Error("file should not exist after flushing with no marks")
	}
}

func TestLoad_CorruptedFile(t *testing.T) {
	path := filepath.Join(t.TempDir(), "offset.json")
	if err := os.WriteFile(path, []byte("{invalid json"), 0o644); err != nil {
		t.Fatal(err)
	}

	store := NewStore(path)
	_, err := store.Load()
	if err == nil {
		t.Fatal("expected error for corrupted file")
	}
}

func TestAtomicWrite_TmpFileRemoved(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "offset.json")
	store := NewStore(path)

	store.Mark(cdc.Offset{LSN: 42})
	if err := store.Flush(); err != nil {
		t.Fatalf("flush error: %v", err)
	}

	// Tmp file should not exist after successful rename
	if _, err := os.Stat(path + ".tmp"); !os.IsNotExist(err) {
		t.Error("tmp file should be removed after atomic write")
	}

	// Main file should exist
	if _, err := os.Stat(path); err != nil {
		t.Errorf("offset file should exist: %v", err)
	}
}

func TestClose_FlushesOffset(t *testing.T) {
	path := filepath.Join(t.TempDir(), "offset.json")
	store := NewStore(path)

	store.Mark(cdc.Offset{LSN: 555})
	if err := store.Close(); err != nil {
		t.Fatalf("close error: %v", err)
	}

	store2 := NewStore(path)
	offset, err := store2.Load()
	if err != nil {
		t.Fatalf("load error: %v", err)
	}
	if offset.LSN != 555 {
		t.Errorf("expected LSN=555, got %d", offset.LSN)
	}
}

func TestLoad_ZeroCommitTime(t *testing.T) {
	path := filepath.Join(t.TempDir(), "offset.json")
	store := NewStore(path)

	// Mark with zero commit time
	store.Mark(cdc.Offset{LSN: 42})
	if err := store.Flush(); err != nil {
		t.Fatalf("flush error: %v", err)
	}

	store2 := NewStore(path)
	offset, err := store2.Load()
	if err != nil {
		t.Fatalf("load error: %v", err)
	}
	if offset.LSN != 42 {
		t.Errorf("expected LSN=42, got %d", offset.LSN)
	}
	if !offset.CommitTime.IsZero() {
		t.Errorf("expected zero commit time, got %v", offset.CommitTime)
	}
}
