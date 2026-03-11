package file

import (
	"encoding/json"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"sync"
	"time"

	"iris/pkg/cdc"
)

// offsetData is the JSON-serializable offset format.
type offsetData struct {
	LSN        uint64 `json:"lsn"`
	CommitTime string `json:"commit_time,omitempty"`
}

// FileStore persists offsets to a local JSON file with atomic writes.
type FileStore struct {
	path   string
	mu     sync.Mutex
	latest cdc.Offset
	dirty  bool
}

// NewStore creates a file-based offset store.
func NewStore(path string) *FileStore {
	return &FileStore{path: path}
}

// Load reads the last persisted offset from disk.
// Returns a zero-value Offset if the file does not exist.
func (s *FileStore) Load() (cdc.Offset, error) {
	data, err := os.ReadFile(s.path)
	if err != nil {
		if errors.Is(err, fs.ErrNotExist) {
			return cdc.Offset{}, nil
		}
		return cdc.Offset{}, fmt.Errorf("read offset file: %w", err)
	}

	var od offsetData
	if err := json.Unmarshal(data, &od); err != nil {
		return cdc.Offset{}, fmt.Errorf("parse offset file: %w", err)
	}

	offset := cdc.Offset{LSN: od.LSN}
	if od.CommitTime != "" {
		t, err := time.Parse(time.RFC3339, od.CommitTime)
		if err == nil {
			offset.CommitTime = t
		}
	}
	return offset, nil
}

// Mark updates the in-memory offset (no disk write).
func (s *FileStore) Mark(offset cdc.Offset) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.latest = offset
	s.dirty = true
}

// Flush writes the current offset to disk atomically (tmp file + rename).
func (s *FileStore) Flush() error {
	s.mu.Lock()
	if !s.dirty {
		s.mu.Unlock()
		return nil
	}
	offset := s.latest
	s.dirty = false
	s.mu.Unlock()

	od := offsetData{
		LSN: offset.LSN,
	}
	if !offset.CommitTime.IsZero() {
		od.CommitTime = offset.CommitTime.Format(time.RFC3339)
	}

	data, err := json.MarshalIndent(od, "", "  ")
	if err != nil {
		return fmt.Errorf("marshal offset: %w", err)
	}
	data = append(data, '\n')

	// Atomic write: write to tmp file, then rename
	tmp := s.path + ".tmp"
	if err := os.WriteFile(tmp, data, 0o644); err != nil {
		return fmt.Errorf("write offset tmp: %w", err)
	}
	if err := os.Rename(tmp, s.path); err != nil {
		return fmt.Errorf("rename offset file: %w", err)
	}
	return nil
}

// Close flushes any pending offset and releases resources.
func (s *FileStore) Close() error {
	return s.Flush()
}
