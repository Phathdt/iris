package redis

// Config holds Redis sink configuration
type Config struct {
	// Addr is the Redis server address (e.g., "localhost:6379")
	Addr string `yaml:"addr"`

	// Password is the Redis password (optional)
	Password string `yaml:"password,omitempty"`

	// DB is the Redis database number (default 0)
	DB int `yaml:"db,omitempty"`

	// Key is the Redis list key for CDC events
	Key string `yaml:"key"`

	// MaxLen trims the list to maximum length (0 = no trimming)
	MaxLen int `yaml:"max_len,omitempty"`
}
