package postgres

import (
	"context"
	"fmt"
	"strings"

	"github.com/conduitdb/conduit/internal/schema"
)

// ListTables returns a summary of all tables and views in the configured schemas.
// Uses pg_stat_user_tables for row count estimates (avoids COUNT(*) overhead).
func (c *PostgresConnector) ListTables(ctx context.Context) ([]schema.TableSummary, error) {
	schemaPlaceholders, schemaArgs := c.schemaPlaceholders(1)

	query := fmt.Sprintf(`
		SELECT
			n.nspname || '.' || cls.relname AS full_name,
			CASE cls.relkind
				WHEN 'r' THEN 'table'
				WHEN 'v' THEN 'view'
				WHEN 'm' THEN 'materialized_view'
				WHEN 'p' THEN 'table'
			END AS table_type,
			COALESCE(s.n_live_tup, 0) AS row_estimate
		FROM pg_catalog.pg_class cls
		JOIN pg_catalog.pg_namespace n ON n.oid = cls.relnamespace
		LEFT JOIN pg_stat_user_tables s
			ON s.schemaname = n.nspname AND s.relname = cls.relname
		WHERE n.nspname IN (%s)
			AND cls.relkind IN ('r', 'v', 'm', 'p')
		ORDER BY n.nspname, cls.relname
	`, schemaPlaceholders)

	rows, err := c.db.QueryContext(ctx, query, schemaArgs...)
	if err != nil {
		return nil, fmt.Errorf("postgres: list tables failed: %w", err)
	}
	defer rows.Close()

	var tables []schema.TableSummary
	for rows.Next() {
		var t schema.TableSummary
		if err := rows.Scan(&t.Name, &t.Type, &t.RowCount); err != nil {
			return nil, fmt.Errorf("postgres: scan table summary: %w", err)
		}
		tables = append(tables, t)
	}
	return tables, rows.Err()
}

// DescribeTable returns full detail for a single table or view,
// including columns, primary keys, foreign keys, and indexes.
func (c *PostgresConnector) DescribeTable(ctx context.Context, tableName string) (*schema.TableDetail, error) {
	schemaName, tblName := splitTableName(tableName)

	detail := &schema.TableDetail{
		Name:   tableName,
		Schema: schemaName,
	}

	// Fetch row count estimate.
	var rowCount int64
	err := c.db.QueryRowContext(ctx, `
		SELECT COALESCE(s.n_live_tup, 0)
		FROM pg_stat_user_tables s
		WHERE s.schemaname = $1 AND s.relname = $2
	`, schemaName, tblName).Scan(&rowCount)
	if err != nil {
		// Not fatal — views won't have stats.
		rowCount = 0
	}
	detail.RowCount = rowCount

	// Fetch table comment/description.
	var desc *string
	_ = c.db.QueryRowContext(ctx, `
		SELECT obj_description(c.oid)
		FROM pg_catalog.pg_class c
		JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace
		WHERE n.nspname = $1 AND c.relname = $2
	`, schemaName, tblName).Scan(&desc)
	if desc != nil {
		detail.Description = *desc
	}

	// Fetch columns.
	columns, err := c.describeColumns(ctx, schemaName, tblName)
	if err != nil {
		return nil, err
	}
	detail.Columns = columns

	// Fetch primary key columns.
	pkCols, err := c.describePrimaryKey(ctx, schemaName, tblName)
	if err != nil {
		return nil, err
	}
	detail.PrimaryKey = pkCols

	// Mark PK columns in the column list.
	pkSet := make(map[string]bool, len(pkCols))
	for _, pk := range pkCols {
		pkSet[pk] = true
	}
	for i := range detail.Columns {
		if pkSet[detail.Columns[i].Name] {
			detail.Columns[i].PK = true
		}
	}

	// Fetch foreign keys.
	fks, err := c.describeForeignKeys(ctx, schemaName, tblName)
	if err != nil {
		return nil, err
	}
	detail.ForeignKeys = fks

	// Annotate FK columns in the column list.
	for _, fk := range fks {
		for i := range detail.Columns {
			if detail.Columns[i].Name == fk.Column {
				detail.Columns[i].FK = fk.RefTable + "." + fk.RefColumn
			}
		}
	}

	// Fetch indexes.
	indexes, err := c.describeIndexes(ctx, schemaName, tblName)
	if err != nil {
		return nil, err
	}
	detail.Indexes = indexes

	return detail, nil
}

// describeColumns fetches column metadata from information_schema.
func (c *PostgresConnector) describeColumns(ctx context.Context, schemaName, tableName string) ([]schema.ColumnInfo, error) {
	query := `
		SELECT
			c.column_name,
			c.udt_name,
			c.data_type,
			c.is_nullable,
			COALESCE(c.column_default, '')
		FROM information_schema.columns c
		WHERE c.table_schema = $1
			AND c.table_name = $2
		ORDER BY c.ordinal_position
	`

	rows, err := c.db.QueryContext(ctx, query, schemaName, tableName)
	if err != nil {
		return nil, fmt.Errorf("postgres: describe columns failed: %w", err)
	}
	defer rows.Close()

	var columns []schema.ColumnInfo
	for rows.Next() {
		var (
			name       string
			udtName    string
			dataType   string
			isNullable string
			dflt       string
		)
		if err := rows.Scan(&name, &udtName, &dataType, &isNullable, &dflt); err != nil {
			return nil, fmt.Errorf("postgres: scan column: %w", err)
		}

		columns = append(columns, schema.ColumnInfo{
			Name:     name,
			Type:     MapPgType(udtName, dataType),
			Nullable: isNullable == "YES",
			Default:  dflt,
		})
	}
	return columns, rows.Err()
}

// describePrimaryKey returns the column names in the primary key.
func (c *PostgresConnector) describePrimaryKey(ctx context.Context, schemaName, tableName string) ([]string, error) {
	query := `
		SELECT kcu.column_name
		FROM information_schema.table_constraints tc
		JOIN information_schema.key_column_usage kcu
			ON tc.constraint_name = kcu.constraint_name
			AND tc.table_schema = kcu.table_schema
		WHERE tc.table_schema = $1
			AND tc.table_name = $2
			AND tc.constraint_type = 'PRIMARY KEY'
		ORDER BY kcu.ordinal_position
	`

	rows, err := c.db.QueryContext(ctx, query, schemaName, tableName)
	if err != nil {
		return nil, fmt.Errorf("postgres: describe primary key failed: %w", err)
	}
	defer rows.Close()

	var cols []string
	for rows.Next() {
		var col string
		if err := rows.Scan(&col); err != nil {
			return nil, fmt.Errorf("postgres: scan pk column: %w", err)
		}
		cols = append(cols, col)
	}
	return cols, rows.Err()
}

// describeForeignKeys returns all foreign key relationships for a table.
func (c *PostgresConnector) describeForeignKeys(ctx context.Context, schemaName, tableName string) ([]schema.FKInfo, error) {
	query := `
		SELECT
			kcu.column_name,
			ccu.table_schema || '.' || ccu.table_name AS ref_table,
			ccu.column_name AS ref_column
		FROM information_schema.table_constraints tc
		JOIN information_schema.key_column_usage kcu
			ON tc.constraint_name = kcu.constraint_name
			AND tc.table_schema = kcu.table_schema
		JOIN information_schema.constraint_column_usage ccu
			ON tc.constraint_name = ccu.constraint_name
			AND tc.table_schema = ccu.table_schema
		WHERE tc.table_schema = $1
			AND tc.table_name = $2
			AND tc.constraint_type = 'FOREIGN KEY'
		ORDER BY kcu.ordinal_position
	`

	rows, err := c.db.QueryContext(ctx, query, schemaName, tableName)
	if err != nil {
		return nil, fmt.Errorf("postgres: describe foreign keys failed: %w", err)
	}
	defer rows.Close()

	var fks []schema.FKInfo
	for rows.Next() {
		var fk schema.FKInfo
		if err := rows.Scan(&fk.Column, &fk.RefTable, &fk.RefColumn); err != nil {
			return nil, fmt.Errorf("postgres: scan fk: %w", err)
		}
		fks = append(fks, fk)
	}
	return fks, rows.Err()
}

// describeIndexes returns the index names for a table.
func (c *PostgresConnector) describeIndexes(ctx context.Context, schemaName, tableName string) ([]string, error) {
	query := `
		SELECT indexname
		FROM pg_indexes
		WHERE schemaname = $1 AND tablename = $2
		ORDER BY indexname
	`

	rows, err := c.db.QueryContext(ctx, query, schemaName, tableName)
	if err != nil {
		return nil, fmt.Errorf("postgres: describe indexes failed: %w", err)
	}
	defer rows.Close()

	var indexes []string
	for rows.Next() {
		var idx string
		if err := rows.Scan(&idx); err != nil {
			return nil, fmt.Errorf("postgres: scan index: %w", err)
		}
		indexes = append(indexes, idx)
	}
	return indexes, rows.Err()
}

// ListProcedures returns stored procedures and functions from pg_proc.
func (c *PostgresConnector) ListProcedures(ctx context.Context) ([]schema.ProcedureSummary, error) {
	schemaPlaceholders, schemaArgs := c.schemaPlaceholders(1)

	query := fmt.Sprintf(`
		SELECT
			n.nspname || '.' || p.proname AS full_name,
			CASE p.prokind
				WHEN 'f' THEN 'function'
				WHEN 'p' THEN 'procedure'
				WHEN 'a' THEN 'function'
				WHEN 'w' THEN 'function'
			END AS proc_type,
			p.pronargs AS param_count
		FROM pg_catalog.pg_proc p
		JOIN pg_catalog.pg_namespace n ON n.oid = p.pronamespace
		WHERE n.nspname IN (%s)
			AND p.prokind IN ('f', 'p')
		ORDER BY n.nspname, p.proname
	`, schemaPlaceholders)

	rows, err := c.db.QueryContext(ctx, query, schemaArgs...)
	if err != nil {
		return nil, fmt.Errorf("postgres: list procedures failed: %w", err)
	}
	defer rows.Close()

	var procs []schema.ProcedureSummary
	for rows.Next() {
		var p schema.ProcedureSummary
		if err := rows.Scan(&p.Name, &p.Type, &p.Params); err != nil {
			return nil, fmt.Errorf("postgres: scan procedure summary: %w", err)
		}
		procs = append(procs, p)
	}
	return procs, rows.Err()
}

// DescribeProcedure returns detailed information about a stored procedure/function.
func (c *PostgresConnector) DescribeProcedure(ctx context.Context, name string) (*schema.ProcedureDetail, error) {
	schemaName, procName := splitTableName(name)

	detail := &schema.ProcedureDetail{
		Name: name,
	}

	// Fetch procedure metadata from pg_proc.
	var prokind string
	var returnType string
	err := c.db.QueryRowContext(ctx, `
		SELECT
			CASE p.prokind
				WHEN 'f' THEN 'function'
				WHEN 'p' THEN 'procedure'
				WHEN 'a' THEN 'function'
				WHEN 'w' THEN 'function'
			END,
			COALESCE(t.typname, 'void')
		FROM pg_catalog.pg_proc p
		JOIN pg_catalog.pg_namespace n ON n.oid = p.pronamespace
		LEFT JOIN pg_catalog.pg_type t ON t.oid = p.prorettype
		WHERE n.nspname = $1 AND p.proname = $2
		LIMIT 1
	`, schemaName, procName).Scan(&prokind, &returnType)
	if err != nil {
		return nil, fmt.Errorf("postgres: describe procedure %q failed: %w", name, err)
	}
	detail.Type = prokind
	detail.Returns = MapPgType(returnType, "")

	// Fetch parameter information from information_schema.parameters (PG 11+).
	paramRows, err := c.db.QueryContext(ctx, `
		SELECT
			COALESCE(p.parameter_name, '$' || p.ordinal_position::text),
			p.udt_name,
			p.data_type,
			p.parameter_mode,
			COALESCE(p.parameter_default, '')
		FROM information_schema.parameters p
		WHERE p.specific_schema = $1
			AND p.specific_name LIKE $2 || '_%'
		ORDER BY p.ordinal_position
	`, schemaName, procName)
	if err != nil {
		// Not all PG versions populate this fully; treat as non-fatal.
		return detail, nil
	}
	defer paramRows.Close()

	for paramRows.Next() {
		var (
			pName    string
			udtName  string
			dataType string
			mode     string
			dflt     string
		)
		if err := paramRows.Scan(&pName, &udtName, &dataType, &mode, &dflt); err != nil {
			return nil, fmt.Errorf("postgres: scan procedure param: %w", err)
		}

		direction := "in"
		switch strings.ToUpper(mode) {
		case "OUT":
			direction = "out"
		case "INOUT":
			direction = "inout"
		}

		detail.Parameters = append(detail.Parameters, schema.ParamInfo{
			Name:      pName,
			Type:      MapPgType(udtName, dataType),
			Direction: direction,
			Default:   dflt,
		})
	}

	return detail, paramRows.Err()
}

// splitTableName splits a "schema.name" string. Defaults schema to "public".
func splitTableName(fullName string) (schemaName, name string) {
	parts := strings.SplitN(fullName, ".", 2)
	if len(parts) == 2 {
		return parts[0], parts[1]
	}
	return "public", parts[0]
}

// MapPgType maps PostgreSQL type names to simplified types for LLMs.
func MapPgType(udtName, dataType string) string {
	// Normalize: lowercase, strip leading underscore (array indicator in PG).
	t := strings.ToLower(udtName)
	isArray := false
	if strings.HasPrefix(t, "_") {
		isArray = true
		t = t[1:]
	}
	// Also check if data_type says "ARRAY".
	if strings.ToUpper(dataType) == "ARRAY" {
		isArray = true
	}

	base := mapBaseType(t)

	if isArray {
		return base + "[]"
	}
	return base
}

// mapBaseType maps a single PG base type to a simplified type.
func mapBaseType(t string) string {
	switch t {
	// String types
	case "text", "varchar", "character varying", "char", "character",
		"bpchar", "uuid", "citext", "name", "xml", "tsvector", "tsquery",
		"inet", "cidr", "macaddr", "macaddr8", "bit", "varbit":
		return "string"

	// Integer types
	case "int2", "int4", "int8", "smallint", "integer", "bigint",
		"serial", "bigserial", "smallserial",
		"serial2", "serial4", "serial8",
		"oid", "xid", "cid", "tid":
		return "integer"

	// Decimal types
	case "numeric", "decimal", "float4", "float8",
		"real", "double precision", "money":
		return "decimal"

	// Boolean
	case "bool", "boolean":
		return "boolean"

	// Datetime types
	case "timestamp", "timestamptz", "timestamp without time zone",
		"timestamp with time zone",
		"date", "time", "timetz", "time without time zone",
		"time with time zone", "interval":
		return "datetime"

	// Binary
	case "bytea":
		return "binary"

	// JSON
	case "json", "jsonb":
		return "json"

	default:
		// Unknown types fall back to string to stay safe.
		return "string"
	}
}
