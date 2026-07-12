package kafka

import (
	"encoding/json"
	"fmt"

	"iris/pkg/cdc"
)

// shape derives the Kafka record key, topic, and value from an outbox event.
//
// It reads the columns named in cfg from event.After. Each field is resolved
// independently: a missing or null column falls back to the sink's default
// behavior for that field only:
//   - key    → event.Table
//   - topic  → fallbackTopic(event.Table)
//   - value  → full CDC envelope (json.Marshal(event))
//
// An error is returned only when the chosen payload value cannot be marshaled.
func shape(event *cdc.Event, cfg *OutboxConfig, fallbackTopic func(string) string) (key []byte, topic string, value []byte, err error) {
	// Key: value of cfg.KeyField, else table name.
	if v, ok := columnValue(event.After, cfg.KeyField); ok {
		key = []byte(fmt.Sprint(v))
	} else {
		key = []byte(event.Table)
	}

	// Topic: value of cfg.RouteField (identity routing), else default topic.
	if v, ok := columnValue(event.After, cfg.RouteField); ok {
		topic = fmt.Sprint(v)
	} else {
		topic = fallbackTopic(event.Table)
	}

	// Value: marshaled value of cfg.PayloadField, else full envelope.
	if v, ok := columnValue(event.After, cfg.PayloadField); ok {
		value, err = json.Marshal(v)
	} else {
		value, err = json.Marshal(event)
	}
	if err != nil {
		return nil, "", nil, fmt.Errorf("encode outbox payload: %w", err)
	}

	return key, topic, value, nil
}

// columnValue returns the value of column from row and whether it is usable.
// An empty column name, nil row, absent key, nil value, or a value that
// stringifies to empty all report absent, so the caller falls back to default
// behavior. Treating empty-but-present as absent avoids emitting an invalid
// empty Kafka topic/key from bad outbox data.
func columnValue(row map[string]any, column string) (any, bool) {
	if column == "" || row == nil {
		return nil, false
	}
	v, ok := row[column]
	if !ok || v == nil || fmt.Sprint(v) == "" {
		return nil, false
	}
	return v, true
}
