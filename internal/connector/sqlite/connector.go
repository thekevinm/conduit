// Package sqlite implements the Connector interface for SQLite databases
// using the pure-Go modernc.org/sqlite driver (no CGO required).
package sqlite

import (
	"context"
	"database/sql"
	"fmt"
	"strings"

	"github.com/conduitdb/conduit/internal/connector"
	"github.com/conduitdb/conduit/internal/schema"
	_ "modernc.org/sqlite"
)

func init() {
	connector.Register("sqlite", func() connector.Connector {
		return &Connector{}
	})
}

// Connector implements connector.Connector for SQLite.
type Connector struct {
	db     *sql.DB
	dbPath string
}

func (c *Connector) Open(ctx context.Context, cfg connector.ConnectionConfig) error {
	// Strip sqlite:// prefix from DSN.
	dsn := cfg.DSN
	dsn = strings.TrimPrefix(dsn, "sqlite://")
	dsn = strings.TrimPrefix(dsn, "sqlite3://")

	db, err := sql.Open("sqlite", dsn)
	if err != nil {
		return fmt.Errorf("sqlite open: %w", err)
	}
	db.SetMaxOpenConns(1) // SQLite only supports one writer
	c.db = db
	c.dbPath = dsn
	return nil
}

func (c *Connector) Close() error {
	if c.db != nil {
		return c.db.Close()
	}
	return nil
}

func (c *Connector) Ping(ctx context.Context) error {
	return c.db.PingContext(ctx)
}

func (c *Connector) DriverName() string { return "sqlite" }

func (c *Connector) QuoteIdentifier(name string) string {
	return `"` + strings.ReplaceAll(name, `"`, `""`) + `"`
}

func (c *Connector) ParameterPlaceholder(index int) string {
	return "?"
}

func (c *Connector) ListTables(ctx context.Context) ([]schema.TableSummary, error) {
	rows, err := c.db.QueryContext(ctx, `
		SELECT name FROM sqlite_master
		WHERE type='table' AND name NOT LIKE 'sqlite_%'
		ORDER BY name`)
	if err != nil {
		return nil, fmt.Errorf("list tables: %w", err)
	}
	defer rows.Close()

	var tables []schema.TableSummary
	for rows.Next() {
		var name string
		if err := rows.Scan(&name); err != nil {
			return nil, err
		}
		// Get row count.
		var count int64
		if err := c.db.QueryRowContext(ctx,
			fmt.Sprintf("SELECT COUNT(*) FROM %s", c.QuoteIdentifier(name))).Scan(&count); err != nil {
			count = -1
		}
		tables = append(tables, schema.TableSummary{
			Name:     name,
			RowCount: count,
			Type:     "table",
		})
	}
	return tables, rows.Err()
}

func (c *Connector) DescribeTable(ctx context.Context, tableName string) (*schema.TableDetail, error) {
	detail := &schema.TableDetail{
		Name:   tableName,
		Schema: "main",
	}

	// Get column info via PRAGMA.
	rows, err := c.db.QueryContext(ctx,
		fmt.Sprintf("PRAGMA table_info(%s)", c.QuoteIdentifier(tableName)))
	if err != nil {
		return nil, fmt.Errorf("describe table: %w", err)
	}
	defer rows.Close()

	for rows.Next() {
		var cid int
		var name, colType string
		var notNull, pk int
		var dfltValue sql.NullString
		if err := rows.Scan(&cid, &name, &colType, &notNull, &dfltValue, &pk); err != nil {
			return nil, err
		}
		col := schema.ColumnInfo{
			Name:     name,
			Type:     mapSQLiteType(colType),
			Nullable: notNull == 0,
			PK:       pk > 0,
		}
		if dfltValue.Valid {
			col.Default = dfltValue.String
		}
		if pk > 0 {
			detail.PrimaryKey = append(detail.PrimaryKey, name)
		}
		detail.Columns = append(detail.Columns, col)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}

	// Get foreign keys.
	fkRows, err := c.db.QueryContext(ctx,
		fmt.Sprintf("PRAGMA foreign_key_list(%s)", c.QuoteIdentifier(tableName)))
	if err == nil {
		defer fkRows.Close()
		for fkRows.Next() {
			var id, seq int
			var table, from, to, onUpdate, onDelete, match string
			if err := fkRows.Scan(&id, &seq, &table, &from, &to, &onUpdate, &onDelete, &match); err != nil {
				continue
			}
			detail.ForeignKeys = append(detail.ForeignKeys, schema.FKInfo{
				Column:    from,
				RefTable:  table,
				RefColumn: to,
			})
		}
	}

	// Get indexes.
	idxRows, err := c.db.QueryContext(ctx,
		fmt.Sprintf("PRAGMA index_list(%s)", c.QuoteIdentifier(tableName)))
	if err == nil {
		defer idxRows.Close()
		for idxRows.Next() {
			var seq int
			var name, origin string
			var unique, partial int
			if err := idxRows.Scan(&seq, &name, &unique, &origin, &partial); err != nil {
				continue
			}
			detail.Indexes = append(detail.Indexes, name)
		}
	}

	// Get row count.
	if err := c.db.QueryRowContext(ctx,
		fmt.Sprintf("SELECT COUNT(*) FROM %s", c.QuoteIdentifier(tableName))).Scan(&detail.RowCount); err != nil {
		detail.RowCount = -1
	}

	return detail, nil
}

func (c *Connector) ListProcedures(ctx context.Context) ([]schema.ProcedureSummary, error) {
	return nil, nil // SQLite doesn't have stored procedures
}

func (c *Connector) DescribeProcedure(ctx context.Context, name string) (*schema.ProcedureDetail, error) {
	return nil, fmt.Errorf("SQLite does not support stored procedures")
}

func (c *Connector) Select(ctx context.Context, req connector.SelectRequest) (*connector.ResultSet, error) {
	query, args := c.buildSelect(req)
	rows, err := c.db.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("select: %w", err)
	}
	defer rows.Close()
	return scanResultSet(rows)
}

func (c *Connector) Insert(ctx context.Context, req connector.InsertRequest) (*connector.MutationResult, error) {
	if len(req.Rows) == 0 {
		return &connector.MutationResult{}, nil
	}
	var totalAffected int64
	for _, row := range req.Rows {
		cols := make([]string, 0, len(row))
		vals := make([]any, 0, len(row))
		placeholders := make([]string, 0, len(row))
		i := 0
		for col, val := range row {
			cols = append(cols, c.QuoteIdentifier(col))
			vals = append(vals, val)
			placeholders = append(placeholders, "?")
			i++
		}
		query := fmt.Sprintf("INSERT INTO %s (%s) VALUES (%s)",
			c.QuoteIdentifier(req.Table),
			strings.Join(cols, ", "),
			strings.Join(placeholders, ", "))
		result, err := c.db.ExecContext(ctx, query, vals...)
		if err != nil {
			return nil, fmt.Errorf("insert: %w", err)
		}
		n, _ := result.RowsAffected()
		totalAffected += n
	}
	return &connector.MutationResult{RowsAffected: totalAffected}, nil
}

func (c *Connector) Update(ctx context.Context, req connector.UpdateRequest) (*connector.MutationResult, error) {
	setClauses := make([]string, 0, len(req.Set))
	args := make([]any, 0, len(req.Set)+1)
	for col, val := range req.Set {
		setClauses = append(setClauses, fmt.Sprintf("%s = ?", c.QuoteIdentifier(col)))
		args = append(args, val)
	}
	query := fmt.Sprintf("UPDATE %s SET %s WHERE %s",
		c.QuoteIdentifier(req.Table),
		strings.Join(setClauses, ", "),
		req.Filter)
	result, err := c.db.ExecContext(ctx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("update: %w", err)
	}
	n, _ := result.RowsAffected()
	return &connector.MutationResult{RowsAffected: n}, nil
}

func (c *Connector) Delete(ctx context.Context, req connector.DeleteRequest) (*connector.MutationResult, error) {
	query := fmt.Sprintf("DELETE FROM %s WHERE %s",
		c.QuoteIdentifier(req.Table), req.Filter)
	result, err := c.db.ExecContext(ctx, query)
	if err != nil {
		return nil, fmt.Errorf("delete: %w", err)
	}
	n, _ := result.RowsAffected()
	return &connector.MutationResult{RowsAffected: n}, nil
}

func (c *Connector) CallProcedure(ctx context.Context, req connector.ProcedureCallRequest) (*connector.ResultSet, error) {
	return nil, fmt.Errorf("SQLite does not support stored procedures")
}

func (c *Connector) buildSelect(req connector.SelectRequest) (string, []any) {
	cols := "*"
	if len(req.Columns) > 0 {
		quoted := make([]string, len(req.Columns))
		for i, col := range req.Columns {
			quoted[i] = c.QuoteIdentifier(col)
		}
		cols = strings.Join(quoted, ", ")
	}
	query := fmt.Sprintf("SELECT %s FROM %s", cols, c.QuoteIdentifier(req.Table))
	var args []any
	if req.Filter != "" {
		query += " WHERE " + req.Filter
	}
	if req.OrderBy != "" {
		query += " ORDER BY " + req.OrderBy
	}
	limit := req.Limit
	if limit <= 0 {
		limit = 100
	}
	query += fmt.Sprintf(" LIMIT %d", limit)
	if req.Offset > 0 {
		query += fmt.Sprintf(" OFFSET %d", req.Offset)
	}
	return query, args
}

func scanResultSet(rows *sql.Rows) (*connector.ResultSet, error) {
	cols, err := rows.Columns()
	if err != nil {
		return nil, err
	}
	rs := &connector.ResultSet{Columns: cols}
	for rows.Next() {
		values := make([]any, len(cols))
		ptrs := make([]any, len(cols))
		for i := range values {
			ptrs[i] = &values[i]
		}
		if err := rows.Scan(ptrs...); err != nil {
			return nil, err
		}
		row := make(map[string]any, len(cols))
		for i, col := range cols {
			row[col] = values[i]
		}
		rs.Rows = append(rs.Rows, row)
	}
	return rs, rows.Err()
}

func mapSQLiteType(sqlType string) string {
	upper := strings.ToUpper(sqlType)
	switch {
	case strings.Contains(upper, "INT"):
		return "integer"
	case strings.Contains(upper, "REAL"), strings.Contains(upper, "FLOAT"), strings.Contains(upper, "DOUBLE"), strings.Contains(upper, "NUMERIC"), strings.Contains(upper, "DECIMAL"):
		return "decimal"
	case strings.Contains(upper, "BOOL"):
		return "boolean"
	case strings.Contains(upper, "DATE"), strings.Contains(upper, "TIME"):
		return "datetime"
	case strings.Contains(upper, "BLOB"):
		return "binary"
	case strings.Contains(upper, "JSON"):
		return "json"
	default:
		return "string"
	}
}
