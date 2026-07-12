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
		key = []byte(toString(v))
	} else {
		key = []byte(event.Table)
	}

	// Topic: value of cfg.RouteField (identity routing), else default topic.
	if v, ok := columnValue(event.After, cfg.RouteField); ok {
		topic = toString(v)
	} else {
		topic = fallbackTopic(event.Table)
	}

	// Value: the payload column, else the full envelope. A payload that is
	// already serialized (string, []byte, json.RawMessage) is passed through
	// as-is; json.Marshal would double-encode a string or base64-encode bytes.
	if v, ok := columnValue(event.After, cfg.PayloadField); ok {
		switch val := v.(type) {
		case string:
			value = []byte(val)
		case []byte:
			value = val
		case json.RawMessage:
			value = val
		default:
			value, err = json.Marshal(val)
		}
	} else {
		value, err = json.Marshal(event)
	}
	if err != nil {
		return nil, "", nil, fmt.Errorf("encode outbox payload: %w", err)
	}

	return key, topic, value, nil
}

// columnValue returns the value of column from row and whether it is usable.
// An empty column name, nil row, absent key, nil value, or an empty string
// value all report absent, so the caller falls back to default behavior.
// Treating empty-but-present as absent avoids emitting an invalid empty Kafka
// topic/key from bad outbox data.
func columnValue(row map[string]any, column string) (any, bool) {
	if column == "" || row == nil {
		return nil, false
	}
	v, ok := row[column]
	if !ok || v == nil {
		return nil, false
	}
	if s, ok := v.(string); ok && s == "" {
		return nil, false
	}
	return v, true
}

// toString converts a column value to a string, avoiding reflection-based
// fmt.Sprint for the common string case (keys/topics are almost always strings).
func toString(v any) string {
	if s, ok := v.(string); ok {
		return s
	}
	return fmt.Sprint(v)
}
