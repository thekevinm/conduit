package mssql

import "testing"

func TestMapMSSQLType(t *testing.T) {
	tests := []struct {
		dataType string
		want     string
	}{
		// Integer types
		{"int", "integer"},
		{"bigint", "integer"},
		{"smallint", "integer"},
		{"tinyint", "integer"},

		// String types
		{"varchar", "string"},
		{"nvarchar", "string"},
		{"char", "string"},
		{"nchar", "string"},
		{"text", "string"},
		{"ntext", "string"},
		{"uniqueidentifier", "string"},
		{"sysname", "string"},
		{"xml", "string"},

		// Decimal types
		{"decimal", "decimal"},
		{"numeric", "decimal"},
		{"float", "decimal"},
		{"real", "decimal"},
		{"money", "decimal"},
		{"smallmoney", "decimal"},

		// Boolean
		{"bit", "boolean"},

		// Datetime types
		{"datetime", "datetime"},
		{"datetime2", "datetime"},
		{"smalldatetime", "datetime"},
		{"date", "datetime"},
		{"time", "datetime"},
		{"datetimeoffset", "datetime"},

		// Binary types
		{"binary", "binary"},
		{"varbinary", "binary"},
		{"image", "binary"},

		// JSON (SQL Server 2016+)
		{"json", "json"},

		// Unknown types default to string
		{"geography", "string"},
		{"geometry", "string"},
		{"hierarchyid", "string"},
		{"sql_variant", "string"},
	}

	for _, tt := range tests {
		t.Run(tt.dataType, func(t *testing.T) {
			got := mapMSSQLType(tt.dataType)
			if got != tt.want {
				t.Errorf("mapMSSQLType(%q) = %q, want %q", tt.dataType, got, tt.want)
			}
		})
	}
}

func TestMapMSSQLType_CaseInsensitive(t *testing.T) {
	// Verify uppercase input is handled.
	got := mapMSSQLType("VARCHAR")
	if got != "string" {
		t.Errorf("mapMSSQLType(\"VARCHAR\") = %q, want \"string\"", got)
	}

	got = mapMSSQLType("INT")
	if got != "integer" {
		t.Errorf("mapMSSQLType(\"INT\") = %q, want \"integer\"", got)
	}

	got = mapMSSQLType("BIT")
	if got != "boolean" {
		t.Errorf("mapMSSQLType(\"BIT\") = %q, want \"boolean\"", got)
	}
}

func TestMapMSSQLType_WithWhitespace(t *testing.T) {
	// Verify leading/trailing whitespace is trimmed.
	got := mapMSSQLType("  varchar  ")
	if got != "string" {
		t.Errorf("mapMSSQLType(\"  varchar  \") = %q, want \"string\"", got)
	}

	got = mapMSSQLType(" int ")
	if got != "integer" {
		t.Errorf("mapMSSQLType(\" int \") = %q, want \"integer\"", got)
	}
}
