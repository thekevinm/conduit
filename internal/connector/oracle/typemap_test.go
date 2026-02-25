package oracle

import (
	"fmt"
	"testing"
)

func TestMapOracleType(t *testing.T) {
	// Helper to create int pointers for precision/scale.
	intPtr := func(v int) *int { return &v }

	tests := []struct {
		name      string
		dataType  string
		precision *int
		scale     *int
		want      string
	}{
		// NUMBER with scale=0 and precision<=10 -> integer
		{"NUMBER integer (precision 5, scale 0)", "NUMBER", intPtr(5), intPtr(0), "integer"},
		{"NUMBER integer (precision 10, scale 0)", "NUMBER", intPtr(10), intPtr(0), "integer"},
		{"NUMBER integer (precision 1, scale 0)", "NUMBER", intPtr(1), intPtr(0), "integer"},

		// NUMBER with scale!=0 or precision>10 -> decimal
		{"NUMBER decimal (precision 12, scale 0)", "NUMBER", intPtr(12), intPtr(0), "decimal"},
		{"NUMBER decimal (precision 10, scale 2)", "NUMBER", intPtr(10), intPtr(2), "decimal"},
		{"NUMBER decimal (nil precision, nil scale)", "NUMBER", nil, nil, "decimal"},
		{"NUMBER decimal (nil precision, scale 0)", "NUMBER", nil, intPtr(0), "decimal"},
		{"NUMBER decimal (precision 10, nil scale)", "NUMBER", intPtr(10), nil, "decimal"},
		{"NUMBER decimal (precision 38, scale 10)", "NUMBER", intPtr(38), intPtr(10), "decimal"},

		// Explicit integer types
		{"INTEGER", "INTEGER", nil, nil, "integer"},
		{"SMALLINT", "SMALLINT", nil, nil, "integer"},
		{"PLS_INTEGER", "PLS_INTEGER", nil, nil, "integer"},
		{"BINARY_INTEGER", "BINARY_INTEGER", nil, nil, "integer"},

		// Float / decimal types
		{"FLOAT", "FLOAT", nil, nil, "decimal"},
		{"BINARY_FLOAT", "BINARY_FLOAT", nil, nil, "decimal"},
		{"BINARY_DOUBLE", "BINARY_DOUBLE", nil, nil, "decimal"},

		// String types
		{"VARCHAR2", "VARCHAR2", nil, nil, "string"},
		{"NVARCHAR2", "NVARCHAR2", nil, nil, "string"},
		{"CHAR", "CHAR", nil, nil, "string"},
		{"NCHAR", "NCHAR", nil, nil, "string"},
		{"CLOB", "CLOB", nil, nil, "string"},
		{"NCLOB", "NCLOB", nil, nil, "string"},
		{"LONG", "LONG", nil, nil, "string"},
		{"XMLTYPE", "XMLTYPE", nil, nil, "string"},
		{"ROWID", "ROWID", nil, nil, "string"},
		{"UROWID", "UROWID", nil, nil, "string"},

		// Datetime types
		{"DATE", "DATE", nil, nil, "datetime"},
		{"TIMESTAMP", "TIMESTAMP", nil, nil, "datetime"},
		{"TIMESTAMP(6)", "TIMESTAMP(6)", nil, nil, "datetime"},
		{"TIMESTAMP WITH TIME ZONE", "TIMESTAMP WITH TIME ZONE", nil, nil, "datetime"},
		{"TIMESTAMP WITH LOCAL TIME ZONE", "TIMESTAMP WITH LOCAL TIME ZONE", nil, nil, "datetime"},
		{"INTERVAL YEAR TO MONTH", "INTERVAL YEAR TO MONTH", nil, nil, "datetime"},
		{"INTERVAL DAY TO SECOND", "INTERVAL DAY TO SECOND", nil, nil, "datetime"},

		// Binary types
		{"BLOB", "BLOB", nil, nil, "binary"},
		{"RAW", "RAW", nil, nil, "binary"},
		{"LONG RAW", "LONG RAW", nil, nil, "binary"},
		{"BFILE", "BFILE", nil, nil, "binary"},

		// Unknown types default to string
		{"SDO_GEOMETRY", "SDO_GEOMETRY", nil, nil, "string"},
		{"CUSTOM_TYPE", "CUSTOM_TYPE", nil, nil, "string"},
		{"ANYDATA", "ANYDATA", nil, nil, "string"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := mapOracleType(tt.dataType, tt.precision, tt.scale)
			if got != tt.want {
				t.Errorf("mapOracleType(%q, %v, %v) = %q, want %q",
					tt.dataType, ptrStr(tt.precision), ptrStr(tt.scale), got, tt.want)
			}
		})
	}
}

func TestMapOracleType_CaseInsensitive(t *testing.T) {
	// The function uppercases input, so lowercase should work too.
	got := mapOracleType("varchar2", nil, nil)
	if got != "string" {
		t.Errorf("mapOracleType(\"varchar2\", nil, nil) = %q, want \"string\"", got)
	}

	got = mapOracleType("number", intPtr(5), intPtr(0))
	if got != "integer" {
		t.Errorf("mapOracleType(\"number\", 5, 0) = %q, want \"integer\"", got)
	}

	got = mapOracleType("blob", nil, nil)
	if got != "binary" {
		t.Errorf("mapOracleType(\"blob\", nil, nil) = %q, want \"binary\"", got)
	}

	got = mapOracleType("date", nil, nil)
	if got != "datetime" {
		t.Errorf("mapOracleType(\"date\", nil, nil) = %q, want \"datetime\"", got)
	}
}

func TestMapOracleType_WhitespaceTrimmed(t *testing.T) {
	// Verify leading/trailing whitespace is handled.
	got := mapOracleType("  VARCHAR2  ", nil, nil)
	if got != "string" {
		t.Errorf("mapOracleType(\"  VARCHAR2  \", nil, nil) = %q, want \"string\"", got)
	}

	got = mapOracleType(" NUMBER ", intPtr(5), intPtr(0))
	if got != "integer" {
		t.Errorf("mapOracleType(\" NUMBER \", 5, 0) = %q, want \"integer\"", got)
	}
}

// intPtr creates an *int for test table convenience.
func intPtr(v int) *int { return &v }

// ptrStr formats an *int for error messages.
func ptrStr(p *int) string {
	if p == nil {
		return "nil"
	}
	return fmt.Sprintf("%d", *p)
}
