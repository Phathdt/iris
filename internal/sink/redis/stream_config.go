package redis

// StreamConfig holds Redis Stream sink configuration
type StreamConfig struct {
	// Addr is the Redis server address (e.g., "localhost:6379")
	Addr string `yaml:"addr"`

	// Password is the Redis password (optional)
	Password string `yaml:"password,omitempty"`

	// DB is the Redis database number (default 0)
	DB int `yaml:"db,omitempty"`

	// StreamKey is the Redis stream key for CDC events
	StreamKey string `yaml:"stream_key"`

	// MaxLen trims the stream to maximum length (0 = no trimming)
	MaxLen int `yaml:"max_len,omitempty"`

	// ApproximateTrim uses ~MAXLEN for better performance (default: false)
	// When true, Redis will trim approximately to MaxLen for better performance
	ApproximateTrim bool `yaml:"approximate_trim,omitempty"`
}
