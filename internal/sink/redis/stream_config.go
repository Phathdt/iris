package redis

// StreamConfig holds Redis Stream sink configuration
type StreamConfig struct {
	// Addr is the Redis server address (e.g., "localhost:6379")
	Addr string `yaml:"addr"`

	// Password is the Redis password (optional)
	Password string `yaml:"password,omitempty"`

	// DB is the Redis database number (default 0)
	DB int `yaml:"db,omitempty"`

	// TableStreamMap maps table names to stream keys
	// If not set, defaults to "cdc:{table}"
	TableStreamMap map[string]string `yaml:"table_stream_map,omitempty"`

	// MaxLen trims the stream to maximum length (0 = no trimming)
	MaxLen int `yaml:"max_len,omitempty"`

	// ApproximateTrim uses ~MAXLEN for better performance (default: false)
	// When true, Redis will trim approximately to MaxLen for better performance
	ApproximateTrim bool `yaml:"approximate_trim,omitempty"`
}

// GetStreamKey returns the stream key for a given table name.
// If TableStreamMap has an explicit mapping, it uses that.
// Otherwise, it defaults to "cdc:{table}".
func (c *StreamConfig) GetStreamKey(table string) string {
	if c.TableStreamMap != nil {
		if key, ok := c.TableStreamMap[table]; ok {
			return key
		}
	}
	// Default: cdc:{table}
	return "cdc:" + table
}
