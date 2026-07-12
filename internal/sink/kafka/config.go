package kafka

// Config holds Kafka sink configuration
type Config struct {
	// Brokers is the list of Kafka broker addresses (e.g., ["localhost:9092"])
	Brokers []string

	// TableTopicMap maps table names to Kafka topic names.
	// If not set, defaults to "cdc.{table}"
	TableTopicMap map[string]string

	// Outbox enables outbox column shaping: derive the Kafka message key,
	// topic, and body from columns in event.After. When nil, the sink keeps
	// its default behavior (key=table, topic=GetTopic, body=full envelope).
	Outbox *OutboxConfig
}

// OutboxConfig maps outbox table columns to Kafka record fields.
// Models the Debezium Outbox Event Router SMT (table.field.event.key,
// route.by.field, table.field.event.payload). Each field is independent:
// if a configured column is missing/null on an event, that field alone
// falls back to the sink's default behavior.
type OutboxConfig struct {
	// KeyField is the column whose value becomes the Kafka message key.
	// Keeps per-aggregate ordering on a partition (e.g., "aggregate_id").
	KeyField string

	// RouteField is the column whose value becomes the topic (identity routing).
	RouteField string

	// PayloadField is the column whose value becomes the message body.
	PayloadField string
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
