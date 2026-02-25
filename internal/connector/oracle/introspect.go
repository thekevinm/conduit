package oracle

import (
	"context"
	"fmt"
	"strings"

	"github.com/conduitdb/conduit/internal/schema"
)

// ListTables returns a summary of all tables and views in the configured schema(s).
// Uses ALL_TABLES.NUM_ROWS for row count estimates (avoids COUNT(*) overhead).
func (c *OracleConnector) ListTables(ctx context.Context) ([]schema.TableSummary, error) {
	ownerPlaceholders, ownerArgs := c.ownerPlaceholders(1)

	query := fmt.Sprintf(`
		SELECT
			t.OWNER || '.' || t.TABLE_NAME AS full_name,
			'table' AS table_type,
			COALESCE(t.NUM_ROWS, 0) AS row_estimate
		FROM ALL_TABLES t
		WHERE t.OWNER IN (%s)
		UNION ALL
		SELECT
			v.OWNER || '.' || v.VIEW_NAME AS full_name,
			'view' AS table_type,
			0 AS row_estimate
		FROM ALL_VIEWS v
		WHERE v.OWNER IN (%s)
		ORDER BY full_name
	`, ownerPlaceholders, ownerPlaceholders)

	// Build combined args (owner args appear twice — once for tables, once for views).
	args := make([]any, 0, len(ownerArgs)*2)
	args = append(args, ownerArgs...)
	args = append(args, ownerArgs...)

	rows, err := c.db.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("oracle: list tables failed: %w", err)
	}
	defer rows.Close()

	var tables []schema.TableSummary
	for rows.Next() {
		var t schema.TableSummary
		if err := rows.Scan(&t.Name, &t.Type, &t.RowCount); err != nil {
			return nil, fmt.Errorf("oracle: scan table summary: %w", err)
		}
		tables = append(tables, t)
	}
	return tables, rows.Err()
}

// DescribeTable returns full detail for a single table or view,
// including columns, primary keys, foreign keys, and indexes.
func (c *OracleConnector) DescribeTable(ctx context.Context, tableName string) (*schema.TableDetail, error) {
	ownerName, tblName := splitTableName(tableName, c.owner)

	detail := &schema.TableDetail{
		Name:   tableName,
		Schema: ownerName,
	}

	// Fetch row count estimate from ALL_TABLES.
	var rowCount int64
	err := c.db.QueryRowContext(ctx, `
		SELECT COALESCE(t.NUM_ROWS, 0)
		FROM ALL_TABLES t
		WHERE t.OWNER = :1 AND t.TABLE_NAME = :2
	`, ownerName, tblName).Scan(&rowCount)
	if err != nil {
		// Not fatal — views won't have stats.
		rowCount = 0
	}
	detail.RowCount = rowCount

	// Fetch table comment/description.
	var desc *string
	_ = c.db.QueryRowContext(ctx, `
		SELECT COMMENTS
		FROM ALL_TAB_COMMENTS
		WHERE OWNER = :1 AND TABLE_NAME = :2 AND COMMENTS IS NOT NULL
	`, ownerName, tblName).Scan(&desc)
	if desc != nil {
		detail.Description = *desc
	}

	// Fetch columns.
	columns, err := c.describeColumns(ctx, ownerName, tblName)
	if err != nil {
		return nil, err
	}
	detail.Columns = columns

	// Fetch primary key columns.
	pkCols, err := c.describePrimaryKey(ctx, ownerName, tblName)
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
	fks, err := c.describeForeignKeys(ctx, ownerName, tblName)
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
	indexes, err := c.describeIndexes(ctx, ownerName, tblName)
	if err != nil {
		return nil, err
	}
	detail.Indexes = indexes

	return detail, nil
}

// describeColumns fetches column metadata from ALL_TAB_COLUMNS.
func (c *OracleConnector) describeColumns(ctx context.Context, owner, tableName string) ([]schema.ColumnInfo, error) {
	query := `
		SELECT
			c.COLUMN_NAME,
			c.DATA_TYPE,
			c.DATA_PRECISION,
			c.DATA_SCALE,
			c.NULLABLE,
			COALESCE(c.DATA_DEFAULT, '')
		FROM ALL_TAB_COLUMNS c
		WHERE c.OWNER = :1
			AND c.TABLE_NAME = :2
		ORDER BY c.COLUMN_ID
	`

	rows, err := c.db.QueryContext(ctx, query, owner, tableName)
	if err != nil {
		return nil, fmt.Errorf("oracle: describe columns failed: %w", err)
	}
	defer rows.Close()

	var columns []schema.ColumnInfo
	for rows.Next() {
		var (
			name      string
			dataType  string
			precision *int
			scale     *int
			nullable  string
			dflt      string
		)
		if err := rows.Scan(&name, &dataType, &precision, &scale, &nullable, &dflt); err != nil {
			return nil, fmt.Errorf("oracle: scan column: %w", err)
		}

		columns = append(columns, schema.ColumnInfo{
			Name:     name,
			Type:     mapOracleType(dataType, precision, scale),
			Nullable: nullable == "Y",
			Default:  strings.TrimSpace(dflt),
		})
	}
	return columns, rows.Err()
}

// describePrimaryKey returns the column names in the primary key.
func (c *OracleConnector) describePrimaryKey(ctx context.Context, owner, tableName string) ([]string, error) {
	query := `
		SELECT cc.COLUMN_NAME
		FROM ALL_CONSTRAINTS con
		JOIN ALL_CONS_COLUMNS cc
			ON con.CONSTRAINT_NAME = cc.CONSTRAINT_NAME
			AND con.OWNER = cc.OWNER
		WHERE con.OWNER = :1
			AND con.TABLE_NAME = :2
			AND con.CONSTRAINT_TYPE = 'P'
		ORDER BY cc.POSITION
	`

	rows, err := c.db.QueryContext(ctx, query, owner, tableName)
	if err != nil {
		return nil, fmt.Errorf("oracle: describe primary key failed: %w", err)
	}
	defer rows.Close()

	var cols []string
	for rows.Next() {
		var col string
		if err := rows.Scan(&col); err != nil {
			return nil, fmt.Errorf("oracle: scan pk column: %w", err)
		}
		cols = append(cols, col)
	}
	return cols, rows.Err()
}

// describeForeignKeys returns all foreign key relationships for a table.
func (c *OracleConnector) describeForeignKeys(ctx context.Context, owner, tableName string) ([]schema.FKInfo, error) {
	query := `
		SELECT
			cc.COLUMN_NAME,
			rc.OWNER || '.' || rc.TABLE_NAME AS ref_table,
			rcc.COLUMN_NAME AS ref_column
		FROM ALL_CONSTRAINTS con
		JOIN ALL_CONS_COLUMNS cc
			ON con.CONSTRAINT_NAME = cc.CONSTRAINT_NAME
			AND con.OWNER = cc.OWNER
		JOIN ALL_CONSTRAINTS rcon
			ON con.R_CONSTRAINT_NAME = rcon.CONSTRAINT_NAME
			AND con.R_OWNER = rcon.OWNER
		JOIN ALL_CONS_COLUMNS rcc
			ON rcon.CONSTRAINT_NAME = rcc.CONSTRAINT_NAME
			AND rcon.OWNER = rcc.OWNER
			AND cc.POSITION = rcc.POSITION
		LEFT JOIN ALL_TABLES rc
			ON rcon.TABLE_NAME = rc.TABLE_NAME
			AND rcon.OWNER = rc.OWNER
		WHERE con.OWNER = :1
			AND con.TABLE_NAME = :2
			AND con.CONSTRAINT_TYPE = 'R'
		ORDER BY cc.POSITION
	`

	rows, err := c.db.QueryContext(ctx, query, owner, tableName)
	if err != nil {
		return nil, fmt.Errorf("oracle: describe foreign keys failed: %w", err)
	}
	defer rows.Close()

	var fks []schema.FKInfo
	for rows.Next() {
		var fk schema.FKInfo
		if err := rows.Scan(&fk.Column, &fk.RefTable, &fk.RefColumn); err != nil {
			return nil, fmt.Errorf("oracle: scan fk: %w", err)
		}
		fks = append(fks, fk)
	}
	return fks, rows.Err()
}

// describeIndexes returns the index names for a table.
func (c *OracleConnector) describeIndexes(ctx context.Context, owner, tableName string) ([]string, error) {
	query := `
		SELECT DISTINCT ic.INDEX_NAME
		FROM ALL_IND_COLUMNS ic
		WHERE ic.TABLE_OWNER = :1 AND ic.TABLE_NAME = :2
		ORDER BY ic.INDEX_NAME
	`

	rows, err := c.db.QueryContext(ctx, query, owner, tableName)
	if err != nil {
		return nil, fmt.Errorf("oracle: describe indexes failed: %w", err)
	}
	defer rows.Close()

	var indexes []string
	for rows.Next() {
		var idx string
		if err := rows.Scan(&idx); err != nil {
			return nil, fmt.Errorf("oracle: scan index: %w", err)
		}
		indexes = append(indexes, idx)
	}
	return indexes, rows.Err()
}

// ListProcedures returns stored procedures and functions from ALL_PROCEDURES.
func (c *OracleConnector) ListProcedures(ctx context.Context) ([]schema.ProcedureSummary, error) {
	ownerPlaceholders, ownerArgs := c.ownerPlaceholders(1)

	query := fmt.Sprintf(`
		SELECT
			p.OWNER || '.' || p.OBJECT_NAME AS full_name,
			CASE p.OBJECT_TYPE
				WHEN 'FUNCTION' THEN 'function'
				WHEN 'PROCEDURE' THEN 'procedure'
			END AS proc_type,
			(
				SELECT COUNT(*)
				FROM ALL_ARGUMENTS a
				WHERE a.OWNER = p.OWNER
					AND a.OBJECT_NAME = p.OBJECT_NAME
					AND a.DATA_LEVEL = 0
					AND a.ARGUMENT_NAME IS NOT NULL
			) AS param_count
		FROM ALL_PROCEDURES p
		WHERE p.OWNER IN (%s)
			AND p.OBJECT_TYPE IN ('PROCEDURE', 'FUNCTION')
			AND p.PROCEDURE_NAME IS NULL
		ORDER BY p.OWNER, p.OBJECT_NAME
	`, ownerPlaceholders)

	rows, err := c.db.QueryContext(ctx, query, ownerArgs...)
	if err != nil {
		return nil, fmt.Errorf("oracle: list procedures failed: %w", err)
	}
	defer rows.Close()

	var procs []schema.ProcedureSummary
	for rows.Next() {
		var p schema.ProcedureSummary
		if err := rows.Scan(&p.Name, &p.Type, &p.Params); err != nil {
			return nil, fmt.Errorf("oracle: scan procedure summary: %w", err)
		}
		procs = append(procs, p)
	}
	return procs, rows.Err()
}

// DescribeProcedure returns detailed information about a stored procedure/function.
func (c *OracleConnector) DescribeProcedure(ctx context.Context, name string) (*schema.ProcedureDetail, error) {
	ownerName, procName := splitTableName(name, c.owner)

	detail := &schema.ProcedureDetail{
		Name: name,
	}

	// Fetch procedure metadata from ALL_PROCEDURES.
	var objectType string
	err := c.db.QueryRowContext(ctx, `
		SELECT
			CASE p.OBJECT_TYPE
				WHEN 'FUNCTION' THEN 'function'
				WHEN 'PROCEDURE' THEN 'procedure'
			END
		FROM ALL_PROCEDURES p
		WHERE p.OWNER = :1
			AND p.OBJECT_NAME = :2
			AND p.PROCEDURE_NAME IS NULL
	`, ownerName, procName).Scan(&objectType)
	if err != nil {
		return nil, fmt.Errorf("oracle: describe procedure %q failed: %w", name, err)
	}
	detail.Type = objectType

	// Fetch parameter information from ALL_ARGUMENTS.
	paramRows, err := c.db.QueryContext(ctx, `
		SELECT
			a.ARGUMENT_NAME,
			a.DATA_TYPE,
			a.DATA_PRECISION,
			a.DATA_SCALE,
			a.IN_OUT,
			a.DEFAULTED
		FROM ALL_ARGUMENTS a
		WHERE a.OWNER = :1
			AND a.OBJECT_NAME = :2
			AND a.DATA_LEVEL = 0
		ORDER BY a.POSITION
	`, ownerName, procName)
	if err != nil {
		// Not all configurations populate this fully; treat as non-fatal.
		return detail, nil
	}
	defer paramRows.Close()

	for paramRows.Next() {
		var (
			pName     *string
			dataType  string
			precision *int
			scale     *int
			inOut     string
			defaulted string
		)
		if err := paramRows.Scan(&pName, &dataType, &precision, &scale, &inOut, &defaulted); err != nil {
			return nil, fmt.Errorf("oracle: scan procedure param: %w", err)
		}

		// POSITION 0 with no ARGUMENT_NAME is the function return value.
		if pName == nil {
			detail.Returns = mapOracleType(dataType, precision, scale)
			continue
		}

		direction := "in"
		switch strings.ToUpper(inOut) {
		case "OUT":
			direction = "out"
		case "IN/OUT":
			direction = "inout"
		}

		dflt := ""
		if strings.ToUpper(defaulted) == "Y" {
			dflt = "(has default)"
		}

		detail.Parameters = append(detail.Parameters, schema.ParamInfo{
			Name:      *pName,
			Type:      mapOracleType(dataType, precision, scale),
			Direction: direction,
			Default:   dflt,
		})
	}

	return detail, paramRows.Err()
}

// splitTableName splits an "OWNER.NAME" string. Defaults owner to the provided default.
// Oracle stores identifiers in UPPERCASE by default.
func splitTableName(fullName string, defaultOwner string) (owner, name string) {
	parts := strings.SplitN(fullName, ".", 2)
	if len(parts) == 2 {
		return strings.ToUpper(parts[0]), strings.ToUpper(parts[1])
	}
	return defaultOwner, strings.ToUpper(parts[0])
}

// mapOracleType maps Oracle data types to simplified types for LLMs.
func mapOracleType(dataType string, precision, scale *int) string {
	t := strings.ToUpper(strings.TrimSpace(dataType))

	switch {
	// NUMBER with scale 0 and precision <= 10 is effectively an integer.
	case t == "NUMBER":
		if scale != nil && *scale == 0 && precision != nil && *precision <= 10 {
			return "integer"
		}
		return "decimal"

	// Integer types.
	case t == "INTEGER" || t == "SMALLINT" || t == "PLS_INTEGER" || t == "BINARY_INTEGER":
		return "integer"

	// Float types.
	case t == "FLOAT" || t == "BINARY_FLOAT" || t == "BINARY_DOUBLE":
		return "decimal"

	// String types.
	case t == "VARCHAR2" || t == "NVARCHAR2" || t == "CHAR" || t == "NCHAR" ||
		t == "CLOB" || t == "NCLOB" || t == "LONG" || t == "XMLTYPE" || t == "ROWID" ||
		t == "UROWID":
		return "string"

	// Datetime types.
	case t == "DATE" || strings.HasPrefix(t, "TIMESTAMP") || strings.HasPrefix(t, "INTERVAL"):
		return "datetime"

	// Binary types.
	case t == "BLOB" || t == "RAW" || t == "LONG RAW" || t == "BFILE":
		return "binary"

	default:
		// Unknown types fall back to string to stay safe.
		return "string"
	}
}
