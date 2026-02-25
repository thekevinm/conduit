package mysql

import "testing"

func TestMapMySQLType(t *testing.T) {
	tests := []struct {
		columnType string
		dataType   string
		want       string
	}{
		// Boolean: tinyint(1) is conventionally boolean in MySQL
		{"tinyint(1)", "tinyint", "boolean"},

		// Integer types
		{"tinyint(4)", "tinyint", "integer"},
		{"smallint", "smallint", "integer"},
		{"mediumint", "mediumint", "integer"},
		{"int", "int", "integer"},
		{"int(11)", "int", "integer"},
		{"integer", "integer", "integer"},
		{"bigint", "bigint", "integer"},
		{"bigint(20)", "bigint", "integer"},
		{"serial", "serial", "integer"},
		{"bit(1)", "bit", "integer"},

		// Decimal types
		{"decimal(10,2)", "decimal", "decimal"},
		{"numeric(8,4)", "numeric", "decimal"},
		{"float", "float", "decimal"},
		{"double", "double", "decimal"},
		{"real", "real", "decimal"},

		// Boolean (explicit bool alias)
		{"boolean", "boolean", "boolean"},
		{"bool", "bool", "boolean"},

		// String types
		{"varchar(255)", "varchar", "string"},
		{"char(10)", "char", "string"},
		{"tinytext", "tinytext", "string"},
		{"text", "text", "string"},
		{"mediumtext", "mediumtext", "string"},
		{"longtext", "longtext", "string"},
		{"enum('a','b')", "enum", "string"},
		{"set('x','y')", "set", "string"},
		{"uuid", "uuid", "string"},

		// Datetime types
		{"datetime", "datetime", "datetime"},
		{"timestamp", "timestamp", "datetime"},
		{"date", "date", "datetime"},
		{"time", "time", "datetime"},
		{"year", "year", "datetime"},

		// Binary types
		{"blob", "blob", "binary"},
		{"binary(16)", "binary", "binary"},
		{"varbinary(255)", "varbinary", "binary"},
		{"tinyblob", "tinyblob", "binary"},
		{"mediumblob", "mediumblob", "binary"},
		{"longblob", "longblob", "binary"},

		// JSON
		{"json", "json", "json"},

		// Geometry types map to string
		{"geometry", "geometry", "string"},
		{"point", "point", "string"},
		{"linestring", "linestring", "string"},
		{"polygon", "polygon", "string"},
		{"multipoint", "multipoint", "string"},
		{"multilinestring", "multilinestring", "string"},
		{"multipolygon", "multipolygon", "string"},
		{"geometrycollection", "geometrycollection", "string"},

		// Unknown types default to string
		{"unknown_type", "unknown_type", "string"},
		{"custom", "custom", "string"},
	}

	for _, tt := range tests {
		t.Run(tt.columnType, func(t *testing.T) {
			got := mapMySQLType(tt.columnType, tt.dataType)
			if got != tt.want {
				t.Errorf("mapMySQLType(%q, %q) = %q, want %q", tt.columnType, tt.dataType, got, tt.want)
			}
		})
	}
}

func TestMapMySQLType_CaseInsensitive(t *testing.T) {
	// Verify uppercase input is handled via strings.ToLower normalization.
	got := mapMySQLType("VARCHAR(255)", "VARCHAR")
	if got != "string" {
		t.Errorf("mapMySQLType(\"VARCHAR(255)\", \"VARCHAR\") = %q, want \"string\"", got)
	}

	got = mapMySQLType("INT(11)", "INT")
	if got != "integer" {
		t.Errorf("mapMySQLType(\"INT(11)\", \"INT\") = %q, want \"integer\"", got)
	}

	got = mapMySQLType("JSON", "JSON")
	if got != "json" {
		t.Errorf("mapMySQLType(\"JSON\", \"JSON\") = %q, want \"json\"", got)
	}
}

func TestMapMySQLType_TinyintBooleanEdgeCases(t *testing.T) {
	// Only tinyint(1) is boolean; other tinyint widths are integer.
	tests := []struct {
		columnType string
		dataType   string
		want       string
	}{
		{"tinyint(1)", "tinyint", "boolean"},
		{"tinyint(2)", "tinyint", "integer"},
		{"tinyint(3)", "tinyint", "integer"},
		{"tinyint(4)", "tinyint", "integer"},
		{"TINYINT(1)", "TINYINT", "boolean"},
	}

	for _, tt := range tests {
		t.Run(tt.columnType, func(t *testing.T) {
			got := mapMySQLType(tt.columnType, tt.dataType)
			if got != tt.want {
				t.Errorf("mapMySQLType(%q, %q) = %q, want %q", tt.columnType, tt.dataType, got, tt.want)
			}
		})
	}
}
