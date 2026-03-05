package postgres

import (
	"encoding/json"
	"fmt"
	"strconv"
	"time"

	"github.com/jackc/pglogrepl"
)

// parseTuple converts PostgreSQL tuple data to a map[string]any
func parseTuple(tuple *pglogrepl.TupleData, relation *pglogrepl.RelationMessage) (map[string]any, error) {
	result := make(map[string]any, len(tuple.Columns))

	for i, col := range tuple.Columns {
		if i >= len(relation.Columns) {
			return nil, fmt.Errorf("column index %d out of range for relation %s", i, relation.RelationName)
		}

		colDef := relation.Columns[i]
		colName := colDef.Name

		// Convert based on column data type
		value, err := convertValue(col.Data, colDef.DataType)
		if err != nil {
			return nil, fmt.Errorf("failed to convert column %s: %w", colName, err)
		}
		result[colName] = value
	}

	return result, nil
}

// convertValue converts PostgreSQL byte data to Go types based on column type OID
func convertValue(data []byte, typeOID uint32) (any, error) {
	// Handle NULL values
	if data == nil {
		return nil, nil
	}

	dataStr := string(data)

	// Convert based on PostgreSQL type OID
	// See https://github.com/postgres/postgres/blob/master/src/include/catalog/pg_type.dat
	switch typeOID {
	// Integer types
	case 21: // int2
		return strconv.ParseInt(dataStr, 10, 16)
	case 23: // int4
		return strconv.ParseInt(dataStr, 10, 32)
	case 20: // int8
		return strconv.ParseInt(dataStr, 10, 64)

	// Float types
	case 700: // float4
		return strconv.ParseFloat(dataStr, 32)
	case 701: // float8
		return strconv.ParseFloat(dataStr, 64)

	// Numeric/decimal - return as string to preserve precision
	case 1700: // numeric
		return dataStr, nil

	// String types
	case 25: // text
		return dataStr, nil
	case 1043: // varchar
		return dataStr, nil
	case 18: // char
		return dataStr, nil
	case 1042: // bpchar
		return dataStr, nil

	// Boolean
	case 16: // bool
		return dataStr == "t" || dataStr == "true", nil

	// Date/Time types
	case 1082: // date
		return time.Parse("2006-01-02", dataStr)
	case 1083: // time
		return time.Parse("15:04:05", dataStr)
	case 1266: // timetz
		return time.Parse("15:04:05Z07", dataStr)
	case 1114: // timestamp
		return time.Parse("2006-01-02 15:04:05", dataStr)
	case 1184: // timestamptz
		return time.Parse("2006-01-02 15:04:05.999999Z07", dataStr)

	// JSON types
	case 114: // json
		return parseJSON(data, false)
	case 3802: // jsonb
		return parseJSON(data, true)

	// UUID
	case 2950: // uuid
		return dataStr, nil

	// Binary data
	case 17: // bytea
		return data, nil

	// Arrays - return as string, could be enhanced to parse array types
	case 1005, 1007, 1016: // int2[], int4[], int8[]
		return dataStr, nil
	case 1009, 1015: // text[], varchar[]
		return dataStr, nil

	// Default: return as string
	default:
		return dataStr, nil
	}
}

// parseJSON parses JSON/JSONB data into a Go value
func parseJSON(data []byte, isJSONB bool) (any, error) {
	if len(data) == 0 {
		return nil, nil
	}

	// For JSONB, the first byte is a version number (usually 1)
	// We need to strip it before parsing
	if isJSONB && data[0] == 1 {
		data = data[1:]
	}

	var result any
	err := json.Unmarshal(data, &result)
	if err != nil {
		return string(data), nil // Return as string if parsing fails
	}
	return result, nil
}
