package file

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sync"

	"iris/pkg/cdc"
)

// Ensure FileSink implements cdc.Sink interface
var _ cdc.Sink = (*FileSink)(nil)

// Config holds file sink configuration
type Config struct {
	// Path is the output file path
	Path string
	// MaxSize is the maximum file size before rotation (in bytes)
	// Default: 0 (no rotation)
	MaxSize int64
	// MaxFiles is the number of rotated files to keep
	// Default: 0 (no rotation)
	MaxFiles int
}

// FileSink writes events as JSON lines to a file
type FileSink struct {
	config      Config
	file        *os.File
	currentSize int64
	mu          sync.Mutex
}

// NewFileSink creates a new file sink
func NewFileSink(cfg Config) (*FileSink, error) {
	if cfg.Path == "" {
		return nil, fmt.Errorf("path is required")
	}

	// Create parent directory if it doesn't exist
	dir := filepath.Dir(cfg.Path)
	if err := os.MkdirAll(dir, 0755); err != nil {
		return nil, fmt.Errorf("create directory: %w", err)
	}

	// Open file in append mode, create if not exists
	f, err := os.OpenFile(cfg.Path, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0600)
	if err != nil {
		return nil, fmt.Errorf("open file: %w", err)
	}

	// Get current file size
	stat, err := f.Stat()
	if err != nil {
		f.Close()
		return nil, fmt.Errorf("stat file: %w", err)
	}

	return &FileSink{
		config:      cfg,
		file:        f,
		currentSize: stat.Size(),
	}, nil
}

// Write writes the event as JSON line to the file
func (s *FileSink) Write(ctx context.Context, event *cdc.Event) error {
	if event == nil {
		return fmt.Errorf("event is nil")
	}

	data, err := json.Marshal(event)
	if err != nil {
		return fmt.Errorf("marshal event: %w", err)
	}

	// Add newline
	line := append(data, '\n')
	lineLen := int64(len(line))

	s.mu.Lock()
	defer s.mu.Unlock()

	// Check if rotation is needed
	if s.config.MaxSize > 0 && s.currentSize+lineLen > s.config.MaxSize {
		if err := s.rotateLocked(); err != nil {
			return fmt.Errorf("rotate: %w", err)
		}
	}

	// Write the line
	n, err := s.file.Write(line)
	if err != nil {
		return fmt.Errorf("write: %w", err)
	}

	s.currentSize += int64(n)
	return nil
}

// rotateLocked rotates the log file (must hold mu)
func (s *FileSink) rotateLocked() error {
	// Sync before closing to ensure data is on disk
	if err := s.file.Sync(); err != nil {
		return fmt.Errorf("sync: %w", err)
	}

	// Close current file
	if err := s.file.Close(); err != nil {
		return fmt.Errorf("close file: %w", err)
	}

	// Rotate existing files
	for i := s.config.MaxFiles - 1; i > 0; i-- {
		oldPath := rotatedPath(s.config.Path, i)
		newPath := rotatedPath(s.config.Path, i+1)

		// Check if old file exists
		if _, err := os.Stat(oldPath); err == nil {
			// Remove newPath if exists
			os.Remove(newPath)
			// Rename old to new
			if err := os.Rename(oldPath, newPath); err != nil {
				return fmt.Errorf("rename %s to %s: %w", oldPath, newPath, err)
			}
		}
	}

	// Rename current file to .1
	if s.config.MaxFiles > 0 {
		rotated1Path := rotatedPath(s.config.Path, 1)
		if err := os.Rename(s.config.Path, rotated1Path); err != nil {
			return fmt.Errorf("rename to %s: %w", rotated1Path, err)
		}
	}

	// Create new file
	f, err := os.Create(s.config.Path)
	if err != nil {
		// Mark file as closed to prevent writes
		s.file = nil
		s.currentSize = 0
		return fmt.Errorf("create file: %w", err)
	}

	s.file = f
	s.currentSize = 0

	return nil
}

// rotatedPath returns the path for a rotated file
func rotatedPath(path string, num int) string {
	dir := filepath.Dir(path)
	ext := filepath.Ext(path)
	base := filepath.Base(path)
	name := base[:len(base)-len(ext)]
	return filepath.Join(dir, fmt.Sprintf("%s.%d%s", name, num, ext))
}

// Close flushes and closes the file
func (s *FileSink) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.file != nil {
		s.file.Sync()
		err := s.file.Close()
		s.file = nil
		return err
	}
	return nil
}
