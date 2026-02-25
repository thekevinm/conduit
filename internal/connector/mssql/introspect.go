package mssql

import (
	"context"
	"fmt"
	"strings"

	"github.com/conduitdb/conduit/internal/schema"
)

// ListTables returns a summary of all tables and views in the configured schemas.
// Uses sys.dm_db_partition_stats for row count estimates (avoids COUNT(*) overhead).
func (c *MSSQLConnector) ListTables(ctx context.Context) ([]schema.TableSummary, error) {
	schemaPlaceholders, schemaArgs := c.schemaPlaceholders(1)

	query := fmt.Sprintf(`
		SELECT
			s.name + '.' + t.name AS full_name,
			CASE t.type
				WHEN 'U' THEN 'table'
				WHEN 'V' THEN 'view'
			END AS table_type,
			COALESCE(p.row_count, 0) AS row_estimate
		FROM sys.tables t
		INNER JOIN sys.schemas s ON s.schema_id = t.schema_id
		LEFT JOIN (
			SELECT object_id, SUM(row_count) AS row_count
			FROM sys.dm_db_partition_stats
			WHERE index_id IN (0, 1)
			GROUP BY object_id
		) p ON p.object_id = t.object_id
		WHERE s.name IN (%s)

		UNION ALL

		SELECT
			s.name + '.' + v.name AS full_name,
			'view' AS table_type,
			0 AS row_estimate
		FROM sys.views v
		INNER JOIN sys.schemas s ON s.schema_id = v.schema_id
		WHERE s.name IN (%s)

		ORDER BY full_name
	`, schemaPlaceholders, schemaPlaceholders)

	// Views query reuses the same schema placeholders, so double the args.
	args := append(schemaArgs, schemaArgs...)

	rows, err := c.db.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("mssql: list tables failed: %w", err)
	}
	defer rows.Close()

	var tables []schema.TableSummary
	for rows.Next() {
		var t schema.TableSummary
		if err := rows.Scan(&t.Name, &t.Type, &t.RowCount); err != nil {
			return nil, fmt.Errorf("mssql: scan table summary: %w", err)
		}
		tables = append(tables, t)
	}
	return tables, rows.Err()
}

// DescribeTable returns full detail for a single table or view,
// including columns, primary keys, foreign keys, and indexes.
func (c *MSSQLConnector) DescribeTable(ctx context.Context, tableName string) (*schema.TableDetail, error) {
	schemaName, tblName := splitTableName(tableName)

	detail := &schema.TableDetail{
		Name:   tableName,
		Schema: schemaName,
	}

	// Fetch row count estimate from sys.dm_db_partition_stats.
	var rowCount int64
	err := c.db.QueryRowContext(ctx, `
		SELECT COALESCE(SUM(p.row_count), 0)
		FROM sys.tables t
		INNER JOIN sys.schemas s ON s.schema_id = t.schema_id
		INNER JOIN sys.dm_db_partition_stats p ON p.object_id = t.object_id
		WHERE s.name = @p1 AND t.name = @p2
			AND p.index_id IN (0, 1)
	`, schemaName, tblName).Scan(&rowCount)
	if err != nil {
		// Not fatal — views won't have stats.
		rowCount = 0
	}
	detail.RowCount = rowCount

	// Fetch table description from extended properties.
	var desc *string
	_ = c.db.QueryRowContext(ctx, `
		SELECT CAST(ep.value AS NVARCHAR(MAX))
		FROM sys.extended_properties ep
		INNER JOIN sys.tables t ON t.object_id = ep.major_id
		INNER JOIN sys.schemas s ON s.schema_id = t.schema_id
		WHERE s.name = @p1 AND t.name = @p2
			AND ep.minor_id = 0
			AND ep.name = 'MS_Description'
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

// describeColumns fetches column metadata from INFORMATION_SCHEMA.COLUMNS.
func (c *MSSQLConnector) describeColumns(ctx context.Context, schemaName, tableName string) ([]schema.ColumnInfo, error) {
	query := `
		SELECT
			c.COLUMN_NAME,
			c.DATA_TYPE,
			c.IS_NULLABLE,
			COALESCE(c.COLUMN_DEFAULT, '')
		FROM INFORMATION_SCHEMA.COLUMNS c
		WHERE c.TABLE_SCHEMA = @p1
			AND c.TABLE_NAME = @p2
		ORDER BY c.ORDINAL_POSITION
	`

	rows, err := c.db.QueryContext(ctx, query, schemaName, tableName)
	if err != nil {
		return nil, fmt.Errorf("mssql: describe columns failed: %w", err)
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
			return nil, fmt.Errorf("mssql: scan column: %w", err)
		}

		columns = append(columns, schema.ColumnInfo{
			Name:     name,
			Type:     mapMSSQLType(dataType),
			Nullable: isNullable == "YES",
			Default:  dflt,
		})
	}
	return columns, rows.Err()
}

// describePrimaryKey returns the column names in the primary key.
func (c *MSSQLConnector) describePrimaryKey(ctx context.Context, schemaName, tableName string) ([]string, error) {
	query := `
		SELECT kcu.COLUMN_NAME
		FROM INFORMATION_SCHEMA.TABLE_CONSTRAINTS tc
		INNER JOIN INFORMATION_SCHEMA.KEY_COLUMN_USAGE kcu
			ON tc.CONSTRAINT_NAME = kcu.CONSTRAINT_NAME
			AND tc.TABLE_SCHEMA = kcu.TABLE_SCHEMA
		WHERE tc.TABLE_SCHEMA = @p1
			AND tc.TABLE_NAME = @p2
			AND tc.CONSTRAINT_TYPE = 'PRIMARY KEY'
		ORDER BY kcu.ORDINAL_POSITION
	`

	rows, err := c.db.QueryContext(ctx, query, schemaName, tableName)
	if err != nil {
		return nil, fmt.Errorf("mssql: describe primary key failed: %w", err)
	}
	defer rows.Close()

	var cols []string
	for rows.Next() {
		var col string
		if err := rows.Scan(&col); err != nil {
			return nil, fmt.Errorf("mssql: scan pk column: %w", err)
		}
		cols = append(cols, col)
	}
	return cols, rows.Err()
}

// describeForeignKeys returns all foreign key relationships for a table.
func (c *MSSQLConnector) describeForeignKeys(ctx context.Context, schemaName, tableName string) ([]schema.FKInfo, error) {
	query := `
		SELECT
			COL_NAME(fkc.parent_object_id, fkc.parent_column_id) AS column_name,
			SCHEMA_NAME(rt.schema_id) + '.' + rt.name AS ref_table,
			COL_NAME(fkc.referenced_object_id, fkc.referenced_column_id) AS ref_column
		FROM sys.foreign_keys fk
		INNER JOIN sys.foreign_key_columns fkc ON fk.object_id = fkc.constraint_object_id
		INNER JOIN sys.tables t ON t.object_id = fk.parent_object_id
		INNER JOIN sys.schemas s ON s.schema_id = t.schema_id
		INNER JOIN sys.tables rt ON rt.object_id = fk.referenced_object_id
		WHERE s.name = @p1 AND t.name = @p2
		ORDER BY fkc.constraint_column_id
	`

	rows, err := c.db.QueryContext(ctx, query, schemaName, tableName)
	if err != nil {
		return nil, fmt.Errorf("mssql: describe foreign keys failed: %w", err)
	}
	defer rows.Close()

	var fks []schema.FKInfo
	for rows.Next() {
		var fk schema.FKInfo
		if err := rows.Scan(&fk.Column, &fk.RefTable, &fk.RefColumn); err != nil {
			return nil, fmt.Errorf("mssql: scan fk: %w", err)
		}
		fks = append(fks, fk)
	}
	return fks, rows.Err()
}

// describeIndexes returns the index names for a table.
func (c *MSSQLConnector) describeIndexes(ctx context.Context, schemaName, tableName string) ([]string, error) {
	query := `
		SELECT i.name
		FROM sys.indexes i
		INNER JOIN sys.tables t ON t.object_id = i.object_id
		INNER JOIN sys.schemas s ON s.schema_id = t.schema_id
		WHERE s.name = @p1 AND t.name = @p2
			AND i.name IS NOT NULL
		ORDER BY i.name
	`

	rows, err := c.db.QueryContext(ctx, query, schemaName, tableName)
	if err != nil {
		return nil, fmt.Errorf("mssql: describe indexes failed: %w", err)
	}
	defer rows.Close()

	var indexes []string
	for rows.Next() {
		var idx string
		if err := rows.Scan(&idx); err != nil {
			return nil, fmt.Errorf("mssql: scan index: %w", err)
		}
		indexes = append(indexes, idx)
	}
	return indexes, rows.Err()
}

// ListProcedures returns stored procedures from sys.procedures.
func (c *MSSQLConnector) ListProcedures(ctx context.Context) ([]schema.ProcedureSummary, error) {
	schemaPlaceholders, schemaArgs := c.schemaPlaceholders(1)

	query := fmt.Sprintf(`
		SELECT
			s.name + '.' + p.name AS full_name,
			'procedure' AS proc_type,
			(
				SELECT COUNT(*)
				FROM sys.parameters pa
				WHERE pa.object_id = p.object_id
					AND pa.parameter_id > 0
			) AS param_count
		FROM sys.procedures p
		INNER JOIN sys.schemas s ON s.schema_id = p.schema_id
		WHERE s.name IN (%s)
		ORDER BY s.name, p.name
	`, schemaPlaceholders)

	rows, err := c.db.QueryContext(ctx, query, schemaArgs...)
	if err != nil {
		return nil, fmt.Errorf("mssql: list procedures failed: %w", err)
	}
	defer rows.Close()

	var procs []schema.ProcedureSummary
	for rows.Next() {
		var p schema.ProcedureSummary
		if err := rows.Scan(&p.Name, &p.Type, &p.Params); err != nil {
			return nil, fmt.Errorf("mssql: scan procedure summary: %w", err)
		}
		procs = append(procs, p)
	}
	return procs, rows.Err()
}

// DescribeProcedure returns detailed information about a stored procedure.
func (c *MSSQLConnector) DescribeProcedure(ctx context.Context, name string) (*schema.ProcedureDetail, error) {
	schemaName, procName := splitTableName(name)

	detail := &schema.ProcedureDetail{
		Name: name,
		Type: "procedure",
	}

	// Verify the procedure exists.
	var objectID int
	err := c.db.QueryRowContext(ctx, `
		SELECT p.object_id
		FROM sys.procedures p
		INNER JOIN sys.schemas s ON s.schema_id = p.schema_id
		WHERE s.name = @p1 AND p.name = @p2
	`, schemaName, procName).Scan(&objectID)
	if err != nil {
		return nil, fmt.Errorf("mssql: describe procedure %q failed: %w", name, err)
	}

	// Fetch parameter information from sys.parameters.
	paramRows, err := c.db.QueryContext(ctx, `
		SELECT
			pa.name,
			TYPE_NAME(pa.user_type_id) AS type_name,
			CASE
				WHEN pa.is_output = 1 THEN 'INOUT'
				ELSE 'IN'
			END AS direction,
			COALESCE(pa.default_value, '') AS default_value
		FROM sys.parameters pa
		WHERE pa.object_id = @p1
			AND pa.parameter_id > 0
		ORDER BY pa.parameter_id
	`, objectID)
	if err != nil {
		// Not fatal; return what we have.
		return detail, nil
	}
	defer paramRows.Close()

	for paramRows.Next() {
		var (
			pName    string
			typeName string
			mode     string
			dflt     string
		)
		if err := paramRows.Scan(&pName, &typeName, &mode, &dflt); err != nil {
			return nil, fmt.Errorf("mssql: scan procedure param: %w", err)
		}

		// Strip leading '@' from parameter name.
		pName = strings.TrimPrefix(pName, "@")

		direction := "in"
		switch strings.ToUpper(mode) {
		case "OUT":
			direction = "out"
		case "INOUT":
			direction = "inout"
		}

		detail.Parameters = append(detail.Parameters, schema.ParamInfo{
			Name:      pName,
			Type:      mapMSSQLType(typeName),
			Direction: direction,
			Default:   dflt,
		})
	}

	return detail, paramRows.Err()
}

// mapMSSQLType maps SQL Server type names to simplified types for LLMs.
func mapMSSQLType(dataType string) string {
	t := strings.ToLower(strings.TrimSpace(dataType))

	switch t {
	// String types
	case "char", "varchar", "text", "nchar", "nvarchar", "ntext",
		"sysname", "xml", "uniqueidentifier":
		return "string"

	// Integer types
	case "int", "bigint", "smallint", "tinyint":
		return "integer"

	// Decimal types
	case "decimal", "numeric", "float", "real", "money", "smallmoney":
		return "decimal"

	// Boolean
	case "bit":
		return "boolean"

	// Datetime types
	case "datetime", "datetime2", "smalldatetime", "date",
		"time", "datetimeoffset":
		return "datetime"

	// Binary types
	case "binary", "varbinary", "image":
		return "binary"

	// JSON (SQL Server 2016+)
	case "json":
		return "json"

	default:
		// Unknown types fall back to string to stay safe.
		return "string"
	}
}
