package mysql

import (
	"context"
	"fmt"
	"strings"

	"github.com/conduitdb/conduit/internal/schema"
)

// ListTables returns a summary of all tables and views in the current database.
// Uses information_schema.tables for metadata and row count estimates.
func (c *MySQLConnector) ListTables(ctx context.Context) ([]schema.TableSummary, error) {
	dbName, err := c.schemaName(ctx)
	if err != nil {
		return nil, err
	}

	query := `
		SELECT
			t.TABLE_NAME,
			CASE t.TABLE_TYPE
				WHEN 'BASE TABLE' THEN 'table'
				WHEN 'VIEW' THEN 'view'
				WHEN 'SYSTEM VIEW' THEN 'view'
				ELSE 'table'
			END AS table_type,
			COALESCE(t.TABLE_ROWS, 0) AS row_estimate
		FROM information_schema.tables t
		WHERE t.TABLE_SCHEMA = ?
		ORDER BY t.TABLE_NAME
	`

	rows, err := c.db.QueryContext(ctx, query, dbName)
	if err != nil {
		return nil, fmt.Errorf("mysql: list tables failed: %w", err)
	}
	defer rows.Close()

	var tables []schema.TableSummary
	for rows.Next() {
		var t schema.TableSummary
		if err := rows.Scan(&t.Name, &t.Type, &t.RowCount); err != nil {
			return nil, fmt.Errorf("mysql: scan table summary: %w", err)
		}
		tables = append(tables, t)
	}
	return tables, rows.Err()
}

// DescribeTable returns full detail for a single table or view,
// including columns, primary keys, foreign keys, and indexes.
func (c *MySQLConnector) DescribeTable(ctx context.Context, tableName string) (*schema.TableDetail, error) {
	dbName, err := c.schemaName(ctx)
	if err != nil {
		return nil, err
	}

	detail := &schema.TableDetail{
		Name:   tableName,
		Schema: dbName,
	}

	// Fetch row count estimate.
	var rowCount int64
	err = c.db.QueryRowContext(ctx, `
		SELECT COALESCE(TABLE_ROWS, 0)
		FROM information_schema.tables
		WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ?
	`, dbName, tableName).Scan(&rowCount)
	if err != nil {
		// Not fatal — views may not have accurate stats.
		rowCount = 0
	}
	detail.RowCount = rowCount

	// Fetch table comment/description.
	var desc *string
	_ = c.db.QueryRowContext(ctx, `
		SELECT TABLE_COMMENT
		FROM information_schema.tables
		WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ?
			AND TABLE_COMMENT != ''
	`, dbName, tableName).Scan(&desc)
	if desc != nil {
		detail.Description = *desc
	}

	// Fetch columns.
	columns, err := c.describeColumns(ctx, dbName, tableName)
	if err != nil {
		return nil, err
	}
	detail.Columns = columns

	// Fetch primary key columns.
	pkCols, err := c.describePrimaryKey(ctx, dbName, tableName)
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
	fks, err := c.describeForeignKeys(ctx, dbName, tableName)
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
	indexes, err := c.describeIndexes(ctx, dbName, tableName)
	if err != nil {
		return nil, err
	}
	detail.Indexes = indexes

	return detail, nil
}

// describeColumns fetches column metadata from information_schema.
func (c *MySQLConnector) describeColumns(ctx context.Context, dbName, tableName string) ([]schema.ColumnInfo, error) {
	query := `
		SELECT
			c.COLUMN_NAME,
			c.COLUMN_TYPE,
			c.DATA_TYPE,
			c.IS_NULLABLE,
			COALESCE(c.COLUMN_DEFAULT, '')
		FROM information_schema.columns c
		WHERE c.TABLE_SCHEMA = ?
			AND c.TABLE_NAME = ?
		ORDER BY c.ORDINAL_POSITION
	`

	rows, err := c.db.QueryContext(ctx, query, dbName, tableName)
	if err != nil {
		return nil, fmt.Errorf("mysql: describe columns failed: %w", err)
	}
	defer rows.Close()

	var columns []schema.ColumnInfo
	for rows.Next() {
		var (
			name       string
			columnType string
			dataType   string
			isNullable string
			dflt       string
		)
		if err := rows.Scan(&name, &columnType, &dataType, &isNullable, &dflt); err != nil {
			return nil, fmt.Errorf("mysql: scan column: %w", err)
		}

		columns = append(columns, schema.ColumnInfo{
			Name:     name,
			Type:     mapMySQLType(columnType, dataType),
			Nullable: isNullable == "YES",
			Default:  dflt,
		})
	}
	return columns, rows.Err()
}

// describePrimaryKey returns the column names in the primary key.
func (c *MySQLConnector) describePrimaryKey(ctx context.Context, dbName, tableName string) ([]string, error) {
	query := `
		SELECT kcu.COLUMN_NAME
		FROM information_schema.table_constraints tc
		JOIN information_schema.key_column_usage kcu
			ON tc.CONSTRAINT_NAME = kcu.CONSTRAINT_NAME
			AND tc.TABLE_SCHEMA = kcu.TABLE_SCHEMA
			AND tc.TABLE_NAME = kcu.TABLE_NAME
		WHERE tc.TABLE_SCHEMA = ?
			AND tc.TABLE_NAME = ?
			AND tc.CONSTRAINT_TYPE = 'PRIMARY KEY'
		ORDER BY kcu.ORDINAL_POSITION
	`

	rows, err := c.db.QueryContext(ctx, query, dbName, tableName)
	if err != nil {
		return nil, fmt.Errorf("mysql: describe primary key failed: %w", err)
	}
	defer rows.Close()

	var cols []string
	for rows.Next() {
		var col string
		if err := rows.Scan(&col); err != nil {
			return nil, fmt.Errorf("mysql: scan pk column: %w", err)
		}
		cols = append(cols, col)
	}
	return cols, rows.Err()
}

// describeForeignKeys returns all foreign key relationships for a table.
func (c *MySQLConnector) describeForeignKeys(ctx context.Context, dbName, tableName string) ([]schema.FKInfo, error) {
	query := `
		SELECT
			kcu.COLUMN_NAME,
			kcu.REFERENCED_TABLE_NAME AS ref_table,
			kcu.REFERENCED_COLUMN_NAME AS ref_column
		FROM information_schema.key_column_usage kcu
		WHERE kcu.TABLE_SCHEMA = ?
			AND kcu.TABLE_NAME = ?
			AND kcu.REFERENCED_TABLE_NAME IS NOT NULL
		ORDER BY kcu.ORDINAL_POSITION
	`

	rows, err := c.db.QueryContext(ctx, query, dbName, tableName)
	if err != nil {
		return nil, fmt.Errorf("mysql: describe foreign keys failed: %w", err)
	}
	defer rows.Close()

	var fks []schema.FKInfo
	for rows.Next() {
		var fk schema.FKInfo
		if err := rows.Scan(&fk.Column, &fk.RefTable, &fk.RefColumn); err != nil {
			return nil, fmt.Errorf("mysql: scan fk: %w", err)
		}
		fks = append(fks, fk)
	}
	return fks, rows.Err()
}

// describeIndexes returns the index names for a table.
func (c *MySQLConnector) describeIndexes(ctx context.Context, dbName, tableName string) ([]string, error) {
	query := `
		SELECT DISTINCT INDEX_NAME
		FROM information_schema.statistics
		WHERE TABLE_SCHEMA = ?
			AND TABLE_NAME = ?
		ORDER BY INDEX_NAME
	`

	rows, err := c.db.QueryContext(ctx, query, dbName, tableName)
	if err != nil {
		return nil, fmt.Errorf("mysql: describe indexes failed: %w", err)
	}
	defer rows.Close()

	var indexes []string
	for rows.Next() {
		var idx string
		if err := rows.Scan(&idx); err != nil {
			return nil, fmt.Errorf("mysql: scan index: %w", err)
		}
		indexes = append(indexes, idx)
	}
	return indexes, rows.Err()
}

// ListProcedures returns stored procedures and functions from information_schema.routines.
func (c *MySQLConnector) ListProcedures(ctx context.Context) ([]schema.ProcedureSummary, error) {
	dbName, err := c.schemaName(ctx)
	if err != nil {
		return nil, err
	}

	query := `
		SELECT
			r.ROUTINE_NAME,
			LOWER(r.ROUTINE_TYPE) AS proc_type,
			(
				SELECT COUNT(*)
				FROM information_schema.parameters p
				WHERE p.SPECIFIC_SCHEMA = r.ROUTINE_SCHEMA
					AND p.SPECIFIC_NAME = r.ROUTINE_NAME
					AND p.ORDINAL_POSITION > 0
			) AS param_count
		FROM information_schema.routines r
		WHERE r.ROUTINE_SCHEMA = ?
		ORDER BY r.ROUTINE_NAME
	`

	rows, err := c.db.QueryContext(ctx, query, dbName)
	if err != nil {
		return nil, fmt.Errorf("mysql: list procedures failed: %w", err)
	}
	defer rows.Close()

	var procs []schema.ProcedureSummary
	for rows.Next() {
		var p schema.ProcedureSummary
		if err := rows.Scan(&p.Name, &p.Type, &p.Params); err != nil {
			return nil, fmt.Errorf("mysql: scan procedure summary: %w", err)
		}
		procs = append(procs, p)
	}
	return procs, rows.Err()
}

// DescribeProcedure returns detailed information about a stored procedure/function.
func (c *MySQLConnector) DescribeProcedure(ctx context.Context, name string) (*schema.ProcedureDetail, error) {
	dbName, err := c.schemaName(ctx)
	if err != nil {
		return nil, err
	}

	detail := &schema.ProcedureDetail{
		Name: name,
	}

	// Fetch procedure metadata from information_schema.routines.
	var routineType string
	var returnType *string
	err = c.db.QueryRowContext(ctx, `
		SELECT
			LOWER(r.ROUTINE_TYPE),
			r.DTD_IDENTIFIER
		FROM information_schema.routines r
		WHERE r.ROUTINE_SCHEMA = ?
			AND r.ROUTINE_NAME = ?
		LIMIT 1
	`, dbName, name).Scan(&routineType, &returnType)
	if err != nil {
		return nil, fmt.Errorf("mysql: describe procedure %q failed: %w", name, err)
	}
	detail.Type = routineType
	if returnType != nil && *returnType != "" {
		detail.Returns = mapMySQLType(*returnType, *returnType)
	}

	// Fetch parameter information from information_schema.parameters.
	paramRows, err := c.db.QueryContext(ctx, `
		SELECT
			COALESCE(p.PARAMETER_NAME, CONCAT('$', p.ORDINAL_POSITION)),
			COALESCE(p.DTD_IDENTIFIER, p.DATA_TYPE),
			p.DATA_TYPE,
			p.PARAMETER_MODE,
			''
		FROM information_schema.parameters p
		WHERE p.SPECIFIC_SCHEMA = ?
			AND p.SPECIFIC_NAME = ?
			AND p.ORDINAL_POSITION > 0
		ORDER BY p.ORDINAL_POSITION
	`, dbName, name)
	if err != nil {
		// Not all MySQL versions populate this fully; treat as non-fatal.
		return detail, nil
	}
	defer paramRows.Close()

	for paramRows.Next() {
		var (
			pName    string
			dtdIdent string
			dataType string
			mode     *string
			dflt     string
		)
		if err := paramRows.Scan(&pName, &dtdIdent, &dataType, &mode, &dflt); err != nil {
			return nil, fmt.Errorf("mysql: scan procedure param: %w", err)
		}

		direction := "in"
		if mode != nil {
			switch strings.ToUpper(*mode) {
			case "OUT":
				direction = "out"
			case "INOUT":
				direction = "inout"
			}
		}

		detail.Parameters = append(detail.Parameters, schema.ParamInfo{
			Name:      pName,
			Type:      mapMySQLType(dtdIdent, dataType),
			Direction: direction,
			Default:   dflt,
		})
	}

	return detail, paramRows.Err()
}

// mapMySQLType maps MySQL type names to simplified types for LLMs.
// columnType is the full COLUMN_TYPE (e.g., "tinyint(1)"), dataType is the base DATA_TYPE.
func mapMySQLType(columnType, dataType string) string {
	ct := strings.ToLower(columnType)
	dt := strings.ToLower(dataType)

	// Special case: tinyint(1) is conventionally boolean in MySQL.
	if ct == "tinyint(1)" {
		return "boolean"
	}

	switch dt {
	// Integer types
	case "tinyint", "smallint", "mediumint", "int", "integer", "bigint",
		"serial", "bit":
		return "integer"

	// Decimal types
	case "decimal", "numeric", "float", "double", "real":
		return "decimal"

	// Boolean (explicit bool type alias)
	case "boolean", "bool":
		return "boolean"

	// String types
	case "char", "varchar", "tinytext", "text", "mediumtext", "longtext",
		"enum", "set", "uuid":
		return "string"

	// Datetime types
	case "date", "datetime", "timestamp", "time", "year":
		return "datetime"

	// Binary types
	case "binary", "varbinary", "tinyblob", "blob", "mediumblob",
		"longblob":
		return "binary"

	// JSON
	case "json":
		return "json"

	// Geometry types
	case "geometry", "point", "linestring", "polygon",
		"multipoint", "multilinestring", "multipolygon",
		"geometrycollection":
		return "string"

	default:
		// Unknown types fall back to string to stay safe.
		return "string"
	}
}
