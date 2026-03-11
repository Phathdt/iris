package kafka

// Config holds Kafka sink configuration
type Config struct {
	// Brokers is the list of Kafka broker addresses (e.g., ["localhost:9092"])
	Brokers []string

	// TableTopicMap maps table names to Kafka topic names.
	// If not set, defaults to "cdc.{table}"
	TableTopicMap map[string]string
}

// GetTopic returns the Kafka topic for a given table name.
// Uses explicit mapping if available, otherwise defaults to "cdc.{table}".
func (c *Config) GetTopic(table string) string {
	if c.TableTopicMap != nil {
		if topic, ok := c.TableTopicMap[table]; ok {
			return topic
		}
	}
	return "cdc." + table
}
