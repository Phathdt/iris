package postgres

import (
	"math"
	"testing"
	"time"

	"github.com/jackc/pglogrepl"
)

// TestParseTuple tests the parseTuple function
func TestParseTuple(t *testing.T) {
	tests := []struct {
		name     string
		tuple    *pglogrepl.TupleData
		relation *pglogrepl.RelationMessage
		wantErr  bool
	}{
		{
			name: "valid tuple with multiple columns",
			tuple: &pglogrepl.TupleData{
				Columns: []*pglogrepl.TupleDataColumn{
					{DataType: 't', Data: []byte("1")},
					{DataType: 't', Data: []byte("alice")},
					{DataType: 't', Data: []byte("alice@example.com")},
				},
			},
			relation: &pglogrepl.RelationMessage{
				RelationID:   1,
				Namespace:    "public",
				RelationName: "users",
				Columns: []*pglogrepl.RelationMessageColumn{
					{Name: "id", DataType: 23},    // int4
					{Name: "name", DataType: 25},  // text
					{Name: "email", DataType: 1043}, // varchar
				},
			},
			wantErr: false,
		},
		{
			name: "tuple with NULL value",
			tuple: &pglogrepl.TupleData{
				Columns: []*pglogrepl.TupleDataColumn{
					{DataType: 'n', Data: nil}, // NULL indicator
					{DataType: 't', Data: []byte("bob")},
				},
			},
			relation: &pglogrepl.RelationMessage{
				RelationID:   2,
				Namespace:    "public",
				RelationName: "users",
				Columns: []*pglogrepl.RelationMessageColumn{
					{Name: "id", DataType: 23},
					{Name: "name", DataType: 25},
				},
			},
			wantErr: false,
		},
		{
			name: "tuple column out of range",
			tuple: &pglogrepl.TupleData{
				Columns: []*pglogrepl.TupleDataColumn{
					{DataType: 't', Data: []byte("1")},
					{DataType: 't', Data: []byte("test")},
					{DataType: 't', Data: []byte("extra")},
				},
			},
			relation: &pglogrepl.RelationMessage{
				RelationID:   3,
				Namespace:    "public",
				RelationName: "test",
				Columns: []*pglogrepl.RelationMessageColumn{
					{Name: "id", DataType: 23},
				},
			},
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := parseTuple(tt.tuple, tt.relation)
			if (err != nil) != tt.wantErr {
				t.Errorf("parseTuple() error = %v, wantErr %v", err, tt.wantErr)
			}
			if err == nil {
				if result == nil {
					t.Error("expected non-nil result")
				}
			}
		})
	}
}

// TestConvertValue_IntegerTypes tests integer type conversions
func TestConvertValue_IntegerTypes(t *testing.T) {
	tests := []struct {
		name    string
		data    []byte
		typeOID uint32
		want    any
		wantErr bool
	}{
		// int2 (OID 21)
		{
			name:    "int2 positive",
			data:    []byte("32767"),
			typeOID: 21,
			want:    int64(32767),
			wantErr: false,
		},
		{
			name:    "int2 negative",
			data:    []byte("-32768"),
			typeOID: 21,
			want:    int64(-32768),
			wantErr: false,
		},
		{
			name:    "int2 zero",
			data:    []byte("0"),
			typeOID: 21,
			want:    int64(0),
			wantErr: false,
		},
		{
			name:    "int2 invalid",
			data:    []byte("not-a-number"),
			typeOID: 21,
			want:    nil,
			wantErr: true,
		},
		// int4 (OID 23)
		{
			name:    "int4 positive",
			data:    []byte("2147483647"),
			typeOID: 23,
			want:    int64(2147483647),
			wantErr: false,
		},
		{
			name:    "int4 negative",
			data:    []byte("-2147483648"),
			typeOID: 23,
			want:    int64(-2147483648),
			wantErr: false,
		},
		{
			name:    "int4 invalid",
			data:    []byte("overflow123"),
			typeOID: 23,
			want:    nil,
			wantErr: true,
		},
		// int8 (OID 20)
		{
			name:    "int8 positive",
			data:    []byte("9223372036854775807"),
			typeOID: 20,
			want:    int64(9223372036854775807),
			wantErr: false,
		},
		{
			name:    "int8 negative",
			data:    []byte("-9223372036854775808"),
			typeOID: 20,
			want:    int64(-9223372036854775808),
			wantErr: false,
		},
		{
			name:    "int8 invalid",
			data:    []byte("not-a-number"),
			typeOID: 20,
			want:    nil,
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := convertValue(tt.data, tt.typeOID)
			if (err != nil) != tt.wantErr {
				t.Errorf("convertValue() error = %v, wantErr %v", err, tt.wantErr)
			}
			if !tt.wantErr && got != tt.want {
				t.Errorf("convertValue() = %v, want %v", got, tt.want)
			}
		})
	}
}

// TestConvertValue_FloatTypes tests float type conversions
func TestConvertValue_FloatTypes(t *testing.T) {
	tests := []struct {
		name    string
		data    []byte
		typeOID uint32
		want    any
		wantErr bool
	}{
		// float4 (OID 700)
		{
			name:    "float4 positive",
			data:    []byte("3.14159"),
			typeOID: 700,
			want:    float32(3.14159),
			wantErr: false,
		},
		{
			name:    "float4 negative",
			data:    []byte("-2.71828"),
			typeOID: 700,
			want:    float32(-2.71828),
			wantErr: false,
		},
		{
			name:    "float4 scientific notation",
			data:    []byte("1.23e10"),
			typeOID: 700,
			want:    float32(1.23e10),
			wantErr: false,
		},
		{
			name:    "float4 invalid",
			data:    []byte("not-a-float"),
			typeOID: 700,
			want:    nil,
			wantErr: true,
		},
		// float8 (OID 701)
		{
			name:    "float8 positive",
			data:    []byte("3.14159265358979"),
			typeOID: 701,
			want:    float64(3.14159265358979),
			wantErr: false,
		},
		{
			name:    "float8 negative",
			data:    []byte("-2.71828182845904"),
			typeOID: 701,
			want:    float64(-2.71828182845904),
			wantErr: false,
		},
		{
			name:    "float8 infinity",
			data:    []byte("Infinity"),
			typeOID: 701,
			want:    math.Inf(1),
			wantErr: false,
		},
		{
			name:    "float8 invalid",
			data:    []byte("not-a-number"),
			typeOID: 701,
			want:    nil,
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := convertValue(tt.data, tt.typeOID)
			if (err != nil) != tt.wantErr {
				t.Errorf("convertValue() error = %v, wantErr %v", err, tt.wantErr)
			}
			if !tt.wantErr {
				switch want := tt.want.(type) {
				case float32:
					if gotFloat, ok := got.(float64); ok {
						if math.Abs(float64(want)-gotFloat) > 0.0001 {
							t.Errorf("convertValue() = %v, want %v", gotFloat, want)
						}
					}
				case float64:
					if gotFloat, ok := got.(float64); ok {
						if gotFloat != want {
							t.Errorf("convertValue() = %v, want %v", gotFloat, want)
						}
					}
				}
			}
		})
	}
}

// TestConvertValue_Numeric tests numeric type
func TestConvertValue_Numeric(t *testing.T) {
	tests := []struct {
		name    string
		data    []byte
		typeOID uint32
		want    string
		wantErr bool
	}{
		{
			name:    "numeric integer",
			data:    []byte("123456789"),
			typeOID: 1700,
			want:    "123456789",
			wantErr: false,
		},
		{
			name:    "numeric decimal",
			data:    []byte("123.456789"),
			typeOID: 1700,
			want:    "123.456789",
			wantErr: false,
		},
		{
			name:    "numeric high precision",
			data:    []byte("123456789.1234567890123456789"),
			typeOID: 1700,
			want:    "123456789.1234567890123456789",
			wantErr: false,
		},
		{
			name:    "numeric negative",
			data:    []byte("-999.99"),
			typeOID: 1700,
			want:    "-999.99",
			wantErr: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := convertValue(tt.data, tt.typeOID)
			if (err != nil) != tt.wantErr {
				t.Errorf("convertValue() error = %v, wantErr %v", err, tt.wantErr)
			}
			if got != tt.want {
				t.Errorf("convertValue() = %v, want %v", got, tt.want)
			}
		})
	}
}

// TestConvertValue_TextTypes tests text/varchar/char type conversions
func TestConvertValue_TextTypes(t *testing.T) {
	tests := []struct {
		name    string
		data    []byte
		typeOID uint32
		want    string
		wantErr bool
	}{
		// text (OID 25)
		{
			name:    "text simple",
			data:    []byte("hello world"),
			typeOID: 25,
			want:    "hello world",
			wantErr: false,
		},
		{
			name:    "text unicode",
			data:    []byte("こんにちは"),
			typeOID: 25,
			want:    "こんにちは",
			wantErr: false,
		},
		{
			name:    "text emoji",
			data:    []byte("test 🚀"),
			typeOID: 25,
			want:    "test 🚀",
			wantErr: false,
		},
		// varchar (OID 1043)
		{
			name:    "varchar simple",
			data:    []byte("varchar value"),
			typeOID: 1043,
			want:    "varchar value",
			wantErr: false,
		},
		// char (OID 18)
		{
			name:    "char single",
			data:    []byte("c"),
			typeOID: 18,
			want:    "c",
			wantErr: false,
		},
		// bpchar (OID 1042)
		{
			name:    "bpchar padded",
			data:    []byte("padded  "),
			typeOID: 1042,
			want:    "padded  ",
			wantErr: false,
		},
		// empty string
		{
			name:    "text empty",
			data:    []byte(""),
			typeOID: 25,
			want:    "",
			wantErr: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := convertValue(tt.data, tt.typeOID)
			if (err != nil) != tt.wantErr {
				t.Errorf("convertValue() error = %v, wantErr %v", err, tt.wantErr)
			}
			if got != tt.want {
				t.Errorf("convertValue() = %v, want %v", got, tt.want)
			}
		})
	}
}

// TestConvertValue_Boolean tests boolean type conversion
func TestConvertValue_Boolean(t *testing.T) {
	tests := []struct {
		name    string
		data    []byte
		typeOID uint32
		want    bool
		wantErr bool
	}{
		{
			name:    "bool true",
			data:    []byte("t"),
			typeOID: 16,
			want:    true,
			wantErr: false,
		},
		{
			name:    "bool true full",
			data:    []byte("true"),
			typeOID: 16,
			want:    true,
			wantErr: false,
		},
		{
			name:    "bool false",
			data:    []byte("f"),
			typeOID: 16,
			want:    false,
			wantErr: false,
		},
		{
			name:    "bool false full",
			data:    []byte("false"),
			typeOID: 16,
			want:    false,
			wantErr: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := convertValue(tt.data, tt.typeOID)
			if (err != nil) != tt.wantErr {
				t.Errorf("convertValue() error = %v, wantErr %v", err, tt.wantErr)
			}
			if got != tt.want {
				t.Errorf("convertValue() = %v, want %v", got, tt.want)
			}
		})
	}
}

// TestConvertValue_DateTime tests date/time type conversions
func TestConvertValue_DateTime(t *testing.T) {
	tests := []struct {
		name    string
		data    []byte
		typeOID uint32
		wantErr bool
	}{
		// date (OID 1082)
		{
			name:    "date valid",
			data:    []byte("2026-03-05"),
			typeOID: 1082,
			wantErr: false,
		},
		{
			name:    "date invalid format",
			data:    []byte("03-05-2026"),
			typeOID: 1082,
			wantErr: true,
		},
		{
			name:    "date invalid string",
			data:    []byte("not-a-date"),
			typeOID: 1082,
			wantErr: true,
		},
		// time (OID 1083)
		{
			name:    "time valid",
			data:    []byte("14:30:00"),
			typeOID: 1083,
			wantErr: false,
		},
		{
			name:    "time with seconds",
			data:    []byte("09:15:30"),
			typeOID: 1083,
			wantErr: false,
		},
		{
			name:    "time invalid",
			data:    []byte("25:00:00"),
			typeOID: 1083,
			wantErr: true,
		},
		// timetz (OID 1266)
		{
			name:    "timetz with timezone",
			data:    []byte("14:30:00+05"),
			typeOID: 1266,
			wantErr: false,
		},
		{
			name:    "timetz negative offset",
			data:    []byte("09:00:00-08"),
			typeOID: 1266,
			wantErr: false,
		},
		// timestamp (OID 1114)
		{
			name:    "timestamp valid",
			data:    []byte("2026-03-05 14:30:00"),
			typeOID: 1114,
			wantErr: false,
		},
		{
			name:    "timestamp with microseconds",
			data:    []byte("2026-03-05 14:30:00.123456"),
			typeOID: 1114,
			wantErr: false,
		},
		{
			name:    "timestamp invalid",
			data:    []byte("not-a-timestamp"),
			typeOID: 1114,
			wantErr: true,
		},
		// timestamptz (OID 1184)
		{
			name:    "timestamptz UTC",
			data:    []byte("2026-03-05 14:30:00Z"),
			typeOID: 1184,
			wantErr: false,
		},
		{
			name:    "timestamptz with offset",
			data:    []byte("2026-03-05 14:30:00+00"),
			typeOID: 1184,
			wantErr: false,
		},
		{
			name:    "timestamptz with microseconds and timezone",
			data:    []byte("2026-03-05 14:30:00.123456+05"),
			typeOID: 1184,
			wantErr: false,
		},
		{
			name:    "timestamptz with timezone negative",
			data:    []byte("2026-03-05 14:30:00-08"),
			typeOID: 1184,
			wantErr: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := convertValue(tt.data, tt.typeOID)
			if (err != nil) != tt.wantErr {
				t.Errorf("convertValue() error = %v, wantErr %v", err, tt.wantErr)
			}
			if !tt.wantErr {
				if _, ok := got.(time.Time); !ok {
					t.Errorf("convertValue() expected time.Time, got %T", got)
				}
			}
		})
	}
}

// TestConvertValue_JSON tests JSON/JSONB type conversions
func TestConvertValue_JSON(t *testing.T) {
	tests := []struct {
		name    string
		data    []byte
		typeOID uint32
		isJSONB bool
		wantErr bool
	}{
		// json (OID 114)
		{
			name:    "json object",
			data:    []byte(`{"key":"value"}`),
			typeOID: 114,
			isJSONB: false,
			wantErr: false,
		},
		{
			name:    "json array",
			data:    []byte(`[1,2,3]`),
			typeOID: 114,
			isJSONB: false,
			wantErr: false,
		},
		{
			name:    "json string",
			data:    []byte(`"hello"`),
			typeOID: 114,
			isJSONB: false,
			wantErr: false,
		},
		{
			name:    "json number",
			data:    []byte(`42`),
			typeOID: 114,
			isJSONB: false,
			wantErr: false,
		},
		{
			name:    "json boolean",
			data:    []byte(`true`),
			typeOID: 114,
			isJSONB: false,
			wantErr: false,
		},
		{
			name:    "json null",
			data:    []byte(`null`),
			typeOID: 114,
			isJSONB: false,
			wantErr: false,
		},
		{
			name:    "json invalid",
			data:    []byte(`{invalid json}`),
			typeOID: 114,
			isJSONB: false,
			wantErr: false, // Returns as string on parse failure
		},
		// jsonb (OID 3802)
		{
			name:    "jsonb with version byte",
			data:    append([]byte{1}, []byte(`{"version":1}`)...),
			typeOID: 3802,
			isJSONB: true,
			wantErr: false,
		},
		{
			name:    "jsonb object",
			data:    append([]byte{1}, []byte(`{"key":"jsonb-value"}`)...),
			typeOID: 3802,
			isJSONB: true,
			wantErr: false,
		},
		{
			name:    "jsonb array",
			data:    append([]byte{1}, []byte(`["a","b","c"]`)...),
			typeOID: 3802,
			isJSONB: true,
			wantErr: false,
		},
		{
			name:    "jsonb nested",
			data:    append([]byte{1}, []byte(`{"nested":{"key":"value"}}`)...),
			typeOID: 3802,
			isJSONB: true,
			wantErr: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := convertValue(tt.data, tt.typeOID)
			if (err != nil) != tt.wantErr {
				t.Errorf("convertValue() error = %v, wantErr %v", err, tt.wantErr)
			}
			if !tt.wantErr && got == nil && string(tt.data) != "null" {
				t.Error("convertValue() expected non-nil result")
			}
		})
	}
}

// TestConvertValue_UUID tests UUID type conversion
func TestConvertValue_UUID(t *testing.T) {
	tests := []struct {
		name    string
		data    []byte
		typeOID uint32
		want    string
		wantErr bool
	}{
		{
			name:    "uuid standard format",
			data:    []byte("a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11"),
			typeOID: 2950,
			want:    "a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11",
			wantErr: false,
		},
		{
			name:    "uuid uppercase",
			data:    []byte("A0EEBC99-9C0B-4EF8-BB6D-6BB9BD380A11"),
			typeOID: 2950,
			want:    "A0EEBC99-9C0B-4EF8-BB6D-6BB9BD380A11",
			wantErr: false,
		},
		{
			name:    "uuid no dashes",
			data:    []byte("a0eebc999c0b4ef8bb6d6bb9bd380a11"),
			typeOID: 2950,
			want:    "a0eebc999c0b4ef8bb6d6bb9bd380a11",
			wantErr: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := convertValue(tt.data, tt.typeOID)
			if (err != nil) != tt.wantErr {
				t.Errorf("convertValue() error = %v, wantErr %v", err, tt.wantErr)
			}
			if got != tt.want {
				t.Errorf("convertValue() = %v, want %v", got, tt.want)
			}
		})
	}
}

// TestConvertValue_Bytea tests bytea type conversion
func TestConvertValue_Bytea(t *testing.T) {
	tests := []struct {
		name    string
		data    []byte
		typeOID uint32
		wantLen int
		wantErr bool
	}{
		{
			name:    "bytea empty",
			data:    []byte{},
			typeOID: 17,
			wantLen: 0,
			wantErr: false,
		},
		{
			name:    "bytea binary",
			data:    []byte{0x00, 0x01, 0x02, 0x03},
			typeOID: 17,
			wantLen: 4,
			wantErr: false,
		},
		{
			name:    "bytea hex representation",
			data:    []byte("\\xdeadbeef"),
			typeOID: 17,
			wantLen: 10,
			wantErr: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := convertValue(tt.data, tt.typeOID)
			if (err != nil) != tt.wantErr {
				t.Errorf("convertValue() error = %v, wantErr %v", err, tt.wantErr)
			}
			if !tt.wantErr {
				gotBytes, ok := got.([]byte)
				if !ok {
					t.Errorf("convertValue() expected []byte, got %T", got)
				}
				if len(gotBytes) != tt.wantLen {
					t.Errorf("convertValue() length = %d, want %d", len(gotBytes), tt.wantLen)
				}
			}
		})
	}
}

// TestConvertValue_NULL tests NULL handling for all types
func TestConvertValue_NULL(t *testing.T) {
	types := []struct {
		name  string
		typeOID uint32
	}{
		{"int2", 21},
		{"int4", 23},
		{"int8", 20},
		{"float4", 700},
		{"float8", 701},
		{"numeric", 1700},
		{"text", 25},
		{"varchar", 1043},
		{"bool", 16},
		{"date", 1082},
		{"timestamp", 1114},
		{"json", 114},
		{"uuid", 2950},
		{"bytea", 17},
	}

	for _, tt := range types {
		t.Run(tt.name, func(t *testing.T) {
			got, err := convertValue(nil, tt.typeOID)
			if err != nil {
				t.Errorf("convertValue(nil) error = %v", err)
			}
			if got != nil {
				t.Errorf("convertValue(nil) = %v, want nil", got)
			}
		})
	}
}

// TestConvertValue_Arrays tests array type conversions
func TestConvertValue_Arrays(t *testing.T) {
	tests := []struct {
		name    string
		data    []byte
		typeOID uint32
		wantErr bool
	}{
		// int4[] (OID 1007)
		{
			name:    "int4 array",
			data:    []byte("{1,2,3}"),
			typeOID: 1007,
			wantErr: false,
		},
		// text[] (OID 1009)
		{
			name:    "text array",
			data:    []byte("{a,b,c}"),
			typeOID: 1009,
			wantErr: false,
		},
		// int8[] (OID 1016)
		{
			name:    "int8 array",
			data:    []byte("{100,200,300}"),
			typeOID: 1016,
			wantErr: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := convertValue(tt.data, tt.typeOID)
			if (err != nil) != tt.wantErr {
				t.Errorf("convertValue() error = %v, wantErr %v", err, tt.wantErr)
			}
			// Arrays are returned as strings
			if !tt.wantErr {
				if _, ok := got.(string); !ok {
					t.Errorf("convertValue() expected string for array, got %T", got)
				}
			}
		})
	}
}

// TestConvertValue_UnknownType tests unknown/unsupported types
func TestConvertValue_UnknownType(t *testing.T) {
	tests := []struct {
		name    string
		data    []byte
		typeOID uint32
		want    string
		wantErr bool
	}{
		{
			name:    "unknown type OID",
			data:    []byte("unknown data"),
			typeOID: 99999,
			want:    "unknown data",
			wantErr: false,
		},
		{
			name:    "reserved type",
			data:    []byte("reserved"),
			typeOID: 0,
			want:    "reserved",
			wantErr: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := convertValue(tt.data, tt.typeOID)
			if (err != nil) != tt.wantErr {
				t.Errorf("convertValue() error = %v, wantErr %v", err, tt.wantErr)
			}
			if got != tt.want {
				t.Errorf("convertValue() = %v, want %v", got, tt.want)
			}
		})
	}
}

// TestParseJSON tests the parseJSON helper function
func TestParseJSON(t *testing.T) {
	tests := []struct {
		name    string
		data    []byte
		isJSONB bool
		wantErr bool
	}{
		{
			name:    "empty data",
			data:    []byte{},
			isJSONB: false,
			wantErr: false,
		},
		{
			name:    "nil result",
			data:    []byte("null"),
			isJSONB: false,
			wantErr: false,
		},
		{
			name:    "valid object",
			data:    []byte(`{"name":"test","value":123}`),
			isJSONB: false,
			wantErr: false,
		},
		{
			name:    "valid array",
			data:    []byte(`[1,2,3,"four"]`),
			isJSONB: false,
			wantErr: false,
		},
		{
			name:    "invalid json returns as string",
			data:    []byte(`{not valid}`),
			isJSONB: false,
			wantErr: false,
		},
		{
			name:    "jsonb with version",
			data:    append([]byte{1}, []byte(`{"jsonb":true}`)...),
			isJSONB: true,
			wantErr: false,
		},
		{
			name:    "complex nested json",
			data:    []byte(`{"users":[{"id":1,"name":"alice"},{"id":2,"name":"bob"}]}`),
			isJSONB: false,
			wantErr: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := parseJSON(tt.data, tt.isJSONB)
			if (err != nil) != tt.wantErr {
				t.Errorf("parseJSON() error = %v, wantErr %v", err, tt.wantErr)
			}
			// Note: JSON null is valid and returns nil, empty data returns nil too
			if !tt.wantErr && got == nil && len(tt.data) > 0 {
				// Check if it's the null case - if input is "null", nil is expected
				if string(tt.data) != "null" {
					t.Error("parseJSON() expected non-nil result")
				}
			}
		})
	}
}

// TestConvertValue_EdgeCases tests edge cases and boundary conditions
func TestConvertValue_EdgeCases(t *testing.T) {
	tests := []struct {
		name    string
		data    []byte
		typeOID uint32
		wantErr bool
	}{
		// Integer overflow attempts
		{
			name:    "int2 overflow attempt",
			data:    []byte("99999999999"),
			typeOID: 21,
			wantErr: true,
		},
		{
			name:    "int4 overflow attempt",
			data:    []byte("99999999999"),
			typeOID: 23,
			wantErr: true,
		},
		// Float special values
		{
			name:    "float4 nan",
			data:    []byte("NaN"),
			typeOID: 700,
			wantErr: false,
		},
		// Empty values
		{
			name:    "empty int4",
			data:    []byte(""),
			typeOID: 23,
			wantErr: true,
		},
		{
			name:    "empty float8",
			data:    []byte(""),
			typeOID: 701,
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := convertValue(tt.data, tt.typeOID)
			if (err != nil) != tt.wantErr {
				t.Errorf("convertValue() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}
