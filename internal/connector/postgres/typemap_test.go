package postgres

import "testing"

func TestMapPgType(t *testing.T) {
	tests := []struct {
		udtName  string
		dataType string
		want     string
	}{
		// String types
		{"text", "", "string"},
		{"varchar", "character varying", "string"},
		{"character varying", "", "string"},
		{"char", "character", "string"},
		{"bpchar", "", "string"},
		{"uuid", "", "string"},
		{"citext", "", "string"},
		{"name", "", "string"},
		{"xml", "", "string"},
		{"inet", "", "string"},
		{"cidr", "", "string"},
		{"macaddr", "", "string"},
		{"tsvector", "", "string"},

		// Integer types
		{"int2", "", "integer"},
		{"int4", "", "integer"},
		{"int8", "", "integer"},
		{"smallint", "", "integer"},
		{"integer", "", "integer"},
		{"bigint", "", "integer"},
		{"serial", "", "integer"},
		{"bigserial", "", "integer"},
		{"smallserial", "", "integer"},
		{"oid", "", "integer"},

		// Decimal types
		{"numeric", "", "decimal"},
		{"decimal", "", "decimal"},
		{"float4", "", "decimal"},
		{"float8", "", "decimal"},
		{"real", "", "decimal"},
		{"double precision", "", "decimal"},
		{"money", "", "decimal"},

		// Boolean
		{"bool", "", "boolean"},
		{"boolean", "", "boolean"},

		// Datetime types
		{"timestamp", "", "datetime"},
		{"timestamptz", "", "datetime"},
		{"timestamp without time zone", "", "datetime"},
		{"timestamp with time zone", "", "datetime"},
		{"date", "", "datetime"},
		{"time", "", "datetime"},
		{"timetz", "", "datetime"},
		{"interval", "", "datetime"},

		// Binary
		{"bytea", "", "binary"},

		// JSON
		{"json", "", "json"},
		{"jsonb", "", "json"},

		// Array types (PG stores arrays with _ prefix in udt_name)
		{"_text", "", "string[]"},
		{"_int4", "", "integer[]"},
		{"_int8", "", "integer[]"},
		{"_bool", "", "boolean[]"},
		{"_float8", "", "decimal[]"},
		{"_jsonb", "", "json[]"},
		{"_timestamptz", "", "datetime[]"},
		{"_bytea", "", "binary[]"},
		{"_uuid", "", "string[]"},

		// Array via data_type column
		{"int4", "ARRAY", "integer[]"},
		{"text", "ARRAY", "string[]"},

		// Unknown types default to string
		{"geometry", "", "string"},
		{"hstore", "", "string"},
		{"ltree", "", "string"},
	}

	for _, tt := range tests {
		t.Run(tt.udtName, func(t *testing.T) {
			got := MapPgType(tt.udtName, tt.dataType)
			if got != tt.want {
				t.Errorf("MapPgType(%q, %q) = %q, want %q", tt.udtName, tt.dataType, got, tt.want)
			}
		})
	}
}

func TestMapPgType_CaseInsensitive(t *testing.T) {
	// Verify uppercase input is handled.
	got := MapPgType("TEXT", "")
	if got != "string" {
		t.Errorf("MapPgType(\"TEXT\", \"\") = %q, want \"string\"", got)
	}

	got = MapPgType("INT4", "")
	if got != "integer" {
		t.Errorf("MapPgType(\"INT4\", \"\") = %q, want \"integer\"", got)
	}
}
