package nats

// Config holds NATS JetStream sink configuration
type Config struct {
	// URL is the NATS server URL (e.g., "nats://localhost:4222")
	URL string

	// TableSubjectMap maps table names to NATS subject names.
	// If not set, defaults to "cdc.{table}"
	TableSubjectMap map[string]string
}

// GetSubject returns the NATS subject for a given table name.
// Uses explicit mapping if available, otherwise defaults to "cdc.{table}".
func (c *Config) GetSubject(table string) string {
	if c.TableSubjectMap != nil {
		if subject, ok := c.TableSubjectMap[table]; ok {
			return subject
		}
	}
	return "cdc." + table
}
