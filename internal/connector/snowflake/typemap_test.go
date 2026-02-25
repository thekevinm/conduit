package snowflake

import "testing"

func TestMapSnowflakeType(t *testing.T) {
	tests := []struct {
		dataType string
		want     string
	}{
		// Integer types
		{"NUMBER", "integer"},
		{"INT", "integer"},
		{"INTEGER", "integer"},
		{"BIGINT", "integer"},
		{"SMALLINT", "integer"},
		{"TINYINT", "integer"},
		{"BYTEINT", "integer"},

		// Parameterized integer type
		{"NUMBER(38,0)", "integer"},

		// Decimal/float types
		{"FLOAT", "decimal"},
		{"FLOAT4", "decimal"},
		{"FLOAT8", "decimal"},
		{"DOUBLE", "decimal"},
		{"DOUBLE PRECISION", "decimal"},
		{"REAL", "decimal"},
		{"NUMERIC", "decimal"},
		{"DECIMAL", "decimal"},

		// Parameterized decimal type
		{"NUMERIC(10,2)", "decimal"},

		// String types
		{"VARCHAR", "string"},
		{"STRING", "string"},
		{"TEXT", "string"},
		{"CHAR", "string"},
		{"CHARACTER", "string"},

		// Parameterized string type
		{"VARCHAR(100)", "string"},

		// Boolean
		{"BOOLEAN", "boolean"},

		// Datetime types
		{"TIMESTAMP_NTZ", "datetime"},
		{"TIMESTAMP_LTZ", "datetime"},
		{"TIMESTAMP_TZ", "datetime"},
		{"TIMESTAMP", "datetime"},
		{"DATE", "datetime"},
		{"TIME", "datetime"},
		{"DATETIME", "datetime"},

		// Binary
		{"BINARY", "binary"},
		{"VARBINARY", "binary"},

		// Semi-structured (JSON-like)
		{"VARIANT", "json"},
		{"OBJECT", "json"},
		{"ARRAY", "json"},

		// Geography/Geometry map to string
		{"GEOGRAPHY", "string"},
		{"GEOMETRY", "string"},

		// Unknown types default to string
		{"SUPER", "string"},
		{"UNKNOWN_TYPE", "string"},
		{"", "string"},
	}

	for _, tt := range tests {
		t.Run(tt.dataType, func(t *testing.T) {
			got := mapSnowflakeType(tt.dataType)
			if got != tt.want {
				t.Errorf("mapSnowflakeType(%q) = %q, want %q", tt.dataType, got, tt.want)
			}
		})
	}
}

func TestMapSnowflakeType_CaseInsensitive(t *testing.T) {
	// Verify lowercase input is handled (function uppercases internally).
	tests := []struct {
		dataType string
		want     string
	}{
		{"number", "integer"},
		{"varchar", "string"},
		{"boolean", "boolean"},
		{"timestamp_ntz", "datetime"},
		{"variant", "json"},
		{"binary", "binary"},
		{"float", "decimal"},
	}

	for _, tt := range tests {
		t.Run(tt.dataType, func(t *testing.T) {
			got := mapSnowflakeType(tt.dataType)
			if got != tt.want {
				t.Errorf("mapSnowflakeType(%q) = %q, want %q", tt.dataType, got, tt.want)
			}
		})
	}
}

func TestMapSnowflakeType_WhitespaceHandling(t *testing.T) {
	// Verify leading/trailing whitespace is trimmed.
	got := mapSnowflakeType("  VARCHAR  ")
	if got != "string" {
		t.Errorf("mapSnowflakeType(\"  VARCHAR  \") = %q, want \"string\"", got)
	}

	got = mapSnowflakeType(" BOOLEAN ")
	if got != "boolean" {
		t.Errorf("mapSnowflakeType(\" BOOLEAN \") = %q, want \"boolean\"", got)
	}
}
