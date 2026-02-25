package snowflake

import (
	"context"
	"fmt"
	"strings"

	"github.com/conduitdb/conduit/internal/schema"
)

// ListTables returns a summary of all tables and views in the current database/schema.
// Uses INFORMATION_SCHEMA.TABLES for metadata including row count estimates.
func (c *SnowflakeConnector) ListTables(ctx context.Context) ([]schema.TableSummary, error) {
	schemaFilter := c.schemaFilter()
	placeholders := make([]string, len(schemaFilter))
	args := make([]any, len(schemaFilter))
	for i, s := range schemaFilter {
		placeholders[i] = "?"
		args[i] = s
	}

	query := fmt.Sprintf(`
		SELECT
			TABLE_SCHEMA || '.' || TABLE_NAME AS full_name,
			CASE TABLE_TYPE
				WHEN 'BASE TABLE' THEN 'table'
				WHEN 'VIEW' THEN 'view'
				WHEN 'MATERIALIZED VIEW' THEN 'materialized_view'
				ELSE LOWER(TABLE_TYPE)
			END AS table_type,
			COALESCE(ROW_COUNT, 0) AS row_count
		FROM %s.INFORMATION_SCHEMA.TABLES
		WHERE TABLE_SCHEMA IN (%s)
			AND TABLE_TYPE IN ('BASE TABLE', 'VIEW', 'MATERIALIZED VIEW')
		ORDER BY TABLE_SCHEMA, TABLE_NAME
	`, c.quoteDB(), strings.Join(placeholders, ", "))

	rows, err := c.db.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("snowflake: list tables failed: %w", err)
	}
	defer rows.Close()

	var tables []schema.TableSummary
	for rows.Next() {
		var t schema.TableSummary
		if err := rows.Scan(&t.Name, &t.Type, &t.RowCount); err != nil {
			return nil, fmt.Errorf("snowflake: scan table summary: %w", err)
		}
		tables = append(tables, t)
	}
	return tables, rows.Err()
}

// DescribeTable returns full detail for a single table or view,
// including columns, primary keys, foreign keys, and row count.
func (c *SnowflakeConnector) DescribeTable(ctx context.Context, tableName string) (*schema.TableDetail, error) {
	schemaName, tblName := c.splitTableName(tableName)

	detail := &schema.TableDetail{
		Name:   tableName,
		Schema: schemaName,
	}

	// Fetch row count from INFORMATION_SCHEMA.TABLES.
	var rowCount int64
	err := c.db.QueryRowContext(ctx, fmt.Sprintf(`
		SELECT COALESCE(ROW_COUNT, 0)
		FROM %s.INFORMATION_SCHEMA.TABLES
		WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ?
	`, c.quoteDB()), schemaName, tblName).Scan(&rowCount)
	if err != nil {
		// Not fatal — views may not have row counts.
		rowCount = 0
	}
	detail.RowCount = rowCount

	// Fetch table comment/description.
	var desc *string
	_ = c.db.QueryRowContext(ctx, fmt.Sprintf(`
		SELECT COMMENT
		FROM %s.INFORMATION_SCHEMA.TABLES
		WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ?
	`, c.quoteDB()), schemaName, tblName).Scan(&desc)
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

	// Fetch foreign keys (imported keys).
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

	return detail, nil
}

// describeColumns fetches column metadata from INFORMATION_SCHEMA.COLUMNS.
func (c *SnowflakeConnector) describeColumns(ctx context.Context, schemaName, tableName string) ([]schema.ColumnInfo, error) {
	query := fmt.Sprintf(`
		SELECT
			COLUMN_NAME,
			DATA_TYPE,
			IS_NULLABLE,
			COALESCE(COLUMN_DEFAULT, '')
		FROM %s.INFORMATION_SCHEMA.COLUMNS
		WHERE TABLE_SCHEMA = ?
			AND TABLE_NAME = ?
		ORDER BY ORDINAL_POSITION
	`, c.quoteDB())

	rows, err := c.db.QueryContext(ctx, query, schemaName, tableName)
	if err != nil {
		return nil, fmt.Errorf("snowflake: describe columns failed: %w", err)
	}
	defer rows.Close()

	var columns []schema.ColumnInfo
	for rows.Next() {
		var (
			name       string
			dataType   string
			isNullable string
			dflt       string
		)
		if err := rows.Scan(&name, &dataType, &isNullable, &dflt); err != nil {
			return nil, fmt.Errorf("snowflake: scan column: %w", err)
		}

		columns = append(columns, schema.ColumnInfo{
			Name:     name,
			Type:     mapSnowflakeType(dataType),
			Nullable: isNullable == "YES",
			Default:  dflt,
		})
	}
	return columns, rows.Err()
}

// describePrimaryKey returns the column names in the primary key.
// Uses SHOW PRIMARY KEYS which is the reliable way in Snowflake.
func (c *SnowflakeConnector) describePrimaryKey(ctx context.Context, schemaName, tableName string) ([]string, error) {
	query := fmt.Sprintf(`
		SELECT COLUMN_NAME
		FROM %s.INFORMATION_SCHEMA.TABLE_CONSTRAINTS tc
		JOIN %s.INFORMATION_SCHEMA.KEY_COLUMN_USAGE kcu
			ON tc.CONSTRAINT_NAME = kcu.CONSTRAINT_NAME
			AND tc.TABLE_SCHEMA = kcu.TABLE_SCHEMA
			AND tc.TABLE_NAME = kcu.TABLE_NAME
		WHERE tc.TABLE_SCHEMA = ?
			AND tc.TABLE_NAME = ?
			AND tc.CONSTRAINT_TYPE = 'PRIMARY KEY'
		ORDER BY kcu.ORDINAL_POSITION
	`, c.quoteDB(), c.quoteDB())

	rows, err := c.db.QueryContext(ctx, query, schemaName, tableName)
	if err != nil {
		// Snowflake may not support constraints on all table types; treat as non-fatal.
		return nil, nil
	}
	defer rows.Close()

	var cols []string
	for rows.Next() {
		var col string
		if err := rows.Scan(&col); err != nil {
			return nil, fmt.Errorf("snowflake: scan pk column: %w", err)
		}
		cols = append(cols, col)
	}
	return cols, rows.Err()
}

// describeForeignKeys returns all foreign key (imported key) relationships for a table.
func (c *SnowflakeConnector) describeForeignKeys(ctx context.Context, schemaName, tableName string) ([]schema.FKInfo, error) {
	query := fmt.Sprintf(`
		SELECT
			fk.COLUMN_NAME,
			fk.REFERENCED_TABLE_SCHEMA || '.' || fk.REFERENCED_TABLE_NAME AS ref_table,
			fk.REFERENCED_COLUMN_NAME AS ref_column
		FROM %s.INFORMATION_SCHEMA.REFERENTIAL_CONSTRAINTS rc
		JOIN %s.INFORMATION_SCHEMA.KEY_COLUMN_USAGE fk
			ON rc.CONSTRAINT_NAME = fk.CONSTRAINT_NAME
			AND rc.CONSTRAINT_SCHEMA = fk.CONSTRAINT_SCHEMA
		WHERE fk.TABLE_SCHEMA = ?
			AND fk.TABLE_NAME = ?
		ORDER BY fk.ORDINAL_POSITION
	`, c.quoteDB(), c.quoteDB())

	rows, err := c.db.QueryContext(ctx, query, schemaName, tableName)
	if err != nil {
		// Foreign keys may not be present; treat as non-fatal.
		return nil, nil
	}
	defer rows.Close()

	var fks []schema.FKInfo
	for rows.Next() {
		var fk schema.FKInfo
		if err := rows.Scan(&fk.Column, &fk.RefTable, &fk.RefColumn); err != nil {
			return nil, fmt.Errorf("snowflake: scan fk: %w", err)
		}
		fks = append(fks, fk)
	}
	return fks, rows.Err()
}

// ListProcedures returns stored procedures from INFORMATION_SCHEMA.PROCEDURES.
func (c *SnowflakeConnector) ListProcedures(ctx context.Context) ([]schema.ProcedureSummary, error) {
	schemaFilter := c.schemaFilter()
	placeholders := make([]string, len(schemaFilter))
	args := make([]any, len(schemaFilter))
	for i, s := range schemaFilter {
		placeholders[i] = "?"
		args[i] = s
	}

	query := fmt.Sprintf(`
		SELECT
			PROCEDURE_SCHEMA || '.' || PROCEDURE_NAME AS full_name,
			'procedure' AS proc_type,
			0 AS param_count
		FROM %s.INFORMATION_SCHEMA.PROCEDURES
		WHERE PROCEDURE_SCHEMA IN (%s)
		ORDER BY PROCEDURE_SCHEMA, PROCEDURE_NAME
	`, c.quoteDB(), strings.Join(placeholders, ", "))

	rows, err := c.db.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("snowflake: list procedures failed: %w", err)
	}
	defer rows.Close()

	var procs []schema.ProcedureSummary
	for rows.Next() {
		var p schema.ProcedureSummary
		if err := rows.Scan(&p.Name, &p.Type, &p.Params); err != nil {
			return nil, fmt.Errorf("snowflake: scan procedure summary: %w", err)
		}
		procs = append(procs, p)
	}
	return procs, rows.Err()
}

// DescribeProcedure returns detailed information about a stored procedure.
// Snowflake's INFORMATION_SCHEMA provides limited procedure metadata compared to PostgreSQL,
// so this returns basic information available from INFORMATION_SCHEMA.PROCEDURES.
func (c *SnowflakeConnector) DescribeProcedure(ctx context.Context, name string) (*schema.ProcedureDetail, error) {
	schemaName, procName := c.splitTableName(name)

	detail := &schema.ProcedureDetail{
		Name: name,
		Type: "procedure",
	}

	// Fetch procedure metadata from INFORMATION_SCHEMA.PROCEDURES.
	var argumentSignature string
	var returnType string
	err := c.db.QueryRowContext(ctx, fmt.Sprintf(`
		SELECT
			COALESCE(ARGUMENT_SIGNATURE, ''),
			COALESCE(DATA_TYPE, 'void')
		FROM %s.INFORMATION_SCHEMA.PROCEDURES
		WHERE PROCEDURE_SCHEMA = ?
			AND PROCEDURE_NAME = ?
		LIMIT 1
	`, c.quoteDB()), schemaName, procName).Scan(&argumentSignature, &returnType)
	if err != nil {
		return nil, fmt.Errorf("snowflake: describe procedure %q failed: %w", name, err)
	}
	detail.Returns = mapSnowflakeType(returnType)

	// Parse argument signature (format: "ARG1 TYPE1, ARG2 TYPE2").
	if argumentSignature != "" && argumentSignature != "()" {
		// Strip surrounding parentheses if present.
		sig := strings.TrimSpace(argumentSignature)
		sig = strings.TrimPrefix(sig, "(")
		sig = strings.TrimSuffix(sig, ")")

		if sig != "" {
			params := strings.Split(sig, ",")
			for _, param := range params {
				param = strings.TrimSpace(param)
				parts := strings.Fields(param)
				if len(parts) >= 2 {
					detail.Parameters = append(detail.Parameters, schema.ParamInfo{
						Name:      parts[0],
						Type:      mapSnowflakeType(strings.Join(parts[1:], " ")),
						Direction: "in",
					})
				} else if len(parts) == 1 {
					detail.Parameters = append(detail.Parameters, schema.ParamInfo{
						Name:      parts[0],
						Type:      "string",
						Direction: "in",
					})
				}
			}
		}
	}

	return detail, nil
}

// splitTableName splits a "schema.name" string. Defaults schema to the connector's configured schema.
func (c *SnowflakeConnector) splitTableName(fullName string) (schemaName, name string) {
	parts := strings.SplitN(fullName, ".", 2)
	if len(parts) == 2 {
		return parts[0], parts[1]
	}
	return c.schemaName, parts[0]
}

// schemaFilter returns the list of schemas to include in introspection.
// Defaults to the current schema (typically "PUBLIC") if none configured.
func (c *SnowflakeConnector) schemaFilter() []string {
	if len(c.cfg.Schemas) > 0 {
		return c.cfg.Schemas
	}
	return []string{c.schemaName}
}

// quoteDB returns the database name quoted for use in INFORMATION_SCHEMA queries.
func (c *SnowflakeConnector) quoteDB() string {
	return `"` + strings.ReplaceAll(c.databaseName, `"`, `""`) + `"`
}

// mapSnowflakeType maps Snowflake data types to simplified types for LLMs.
func mapSnowflakeType(dataType string) string {
	t := strings.ToUpper(strings.TrimSpace(dataType))

	// Handle parameterized types like NUMBER(38,0), VARCHAR(100).
	if idx := strings.IndexByte(t, '('); idx != -1 {
		t = t[:idx]
	}

	switch t {
	// Integer types
	case "NUMBER", "INT", "INTEGER", "BIGINT", "SMALLINT", "TINYINT", "BYTEINT":
		return "integer"

	// Decimal/float types
	case "FLOAT", "FLOAT4", "FLOAT8", "DOUBLE", "DOUBLE PRECISION", "REAL",
		"NUMERIC", "DECIMAL":
		return "decimal"

	// String types
	case "VARCHAR", "STRING", "TEXT", "CHAR", "CHARACTER":
		return "string"

	// Boolean
	case "BOOLEAN":
		return "boolean"

	// Datetime types
	case "TIMESTAMP", "TIMESTAMP_LTZ", "TIMESTAMP_NTZ", "TIMESTAMP_TZ",
		"DATE", "TIME", "DATETIME":
		return "datetime"

	// Binary
	case "BINARY", "VARBINARY":
		return "binary"

	// Semi-structured (JSON-like)
	case "VARIANT", "OBJECT", "ARRAY":
		return "json"

	// Geography/Geometry
	case "GEOGRAPHY", "GEOMETRY":
		return "string"

	default:
		// Unknown types fall back to string to stay safe.
		return "string"
	}
}
