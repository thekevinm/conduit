// Package mssql implements the Connector interface for Microsoft SQL Server.
package mssql

import (
	"context"
	"database/sql"
	"fmt"
	"strings"

	"github.com/conduitdb/conduit/internal/connector"

	_ "github.com/microsoft/go-mssqldb" // SQL Server driver
)

func init() {
	connector.Register("mssql", func() connector.Connector {
		return &MSSQLConnector{schemaName: "dbo"}
	})
}

// MSSQLConnector implements connector.Connector for Microsoft SQL Server.
type MSSQLConnector struct {
	db         *sql.DB
	cfg        connector.ConnectionConfig
	qb         *QueryBuilder
	readOnly   bool
	schemaName string
}

// Open establishes a connection to the SQL Server database.
func (c *MSSQLConnector) Open(ctx context.Context, cfg connector.ConnectionConfig) error {
	dsn := normalizeDSN(cfg.DSN)

	db, err := sql.Open("sqlserver", dsn)
	if err != nil {
		return fmt.Errorf("mssql: failed to open connection: %w", err)
	}

	if cfg.MaxOpenConns > 0 {
		db.SetMaxOpenConns(cfg.MaxOpenConns)
	}
	if cfg.MaxIdleConns > 0 {
		db.SetMaxIdleConns(cfg.MaxIdleConns)
	}
	if cfg.ConnMaxLifetime > 0 {
		db.SetConnMaxLifetime(cfg.ConnMaxLifetime)
	}

	if err := db.PingContext(ctx); err != nil {
		db.Close()
		return fmt.Errorf("mssql: failed to ping database: %w", err)
	}

	c.db = db
	c.cfg = cfg
	c.readOnly = cfg.ReadOnly
	c.qb = &QueryBuilder{}

	// Set default schema from config or default to "dbo".
	if len(cfg.Schemas) > 0 {
		c.schemaName = cfg.Schemas[0]
	}

	return nil
}

// Close closes the database connection pool.
func (c *MSSQLConnector) Close() error {
	if c.db != nil {
		return c.db.Close()
	}
	return nil
}

// Ping verifies the connection is still alive.
func (c *MSSQLConnector) Ping(ctx context.Context) error {
	if c.db == nil {
		return fmt.Errorf("mssql: connection not open")
	}
	return c.db.PingContext(ctx)
}

// DriverName returns the driver identifier.
func (c *MSSQLConnector) DriverName() string {
	return "mssql"
}

// QuoteIdentifier quotes an identifier for safe use in SQL.
// Handles schema-qualified names (e.g., [dbo].[users]).
func (c *MSSQLConnector) QuoteIdentifier(name string) string {
	return c.qb.QuoteIdentifier(name)
}

// ParameterPlaceholder returns the SQL Server placeholder format (@p1, @p2, ...).
func (c *MSSQLConnector) ParameterPlaceholder(index int) string {
	return c.qb.ParameterPlaceholder(index)
}

// Select executes a typed SELECT query.
func (c *MSSQLConnector) Select(ctx context.Context, req connector.SelectRequest) (*connector.ResultSet, error) {
	query, args := c.qb.BuildSelect(req)
	rows, err := c.db.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("mssql: select failed: %w", err)
	}
	defer rows.Close()

	return scanRows(rows)
}

// Insert executes a typed INSERT statement.
func (c *MSSQLConnector) Insert(ctx context.Context, req connector.InsertRequest) (*connector.MutationResult, error) {
	if c.readOnly {
		return nil, fmt.Errorf("mssql: insert denied — connection is read-only")
	}
	if len(req.Rows) == 0 {
		return &connector.MutationResult{RowsAffected: 0}, nil
	}

	query, args := c.qb.BuildInsert(req)
	result, err := c.db.ExecContext(ctx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("mssql: insert failed: %w", err)
	}

	affected, _ := result.RowsAffected()
	return &connector.MutationResult{RowsAffected: affected}, nil
}

// Update executes a typed UPDATE statement.
func (c *MSSQLConnector) Update(ctx context.Context, req connector.UpdateRequest) (*connector.MutationResult, error) {
	if c.readOnly {
		return nil, fmt.Errorf("mssql: update denied — connection is read-only")
	}
	if req.Filter == "" {
		return nil, fmt.Errorf("mssql: update requires a filter (refusing unfiltered update)")
	}

	query, args := c.qb.BuildUpdate(req)
	result, err := c.db.ExecContext(ctx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("mssql: update failed: %w", err)
	}

	affected, _ := result.RowsAffected()
	return &connector.MutationResult{RowsAffected: affected}, nil
}

// Delete executes a typed DELETE statement.
func (c *MSSQLConnector) Delete(ctx context.Context, req connector.DeleteRequest) (*connector.MutationResult, error) {
	if c.readOnly {
		return nil, fmt.Errorf("mssql: delete denied — connection is read-only")
	}
	if req.Filter == "" {
		return nil, fmt.Errorf("mssql: delete requires a filter (refusing unfiltered delete)")
	}

	query, args := c.qb.BuildDelete(req)
	result, err := c.db.ExecContext(ctx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("mssql: delete failed: %w", err)
	}

	affected, _ := result.RowsAffected()
	return &connector.MutationResult{RowsAffected: affected}, nil
}

// CallProcedure executes a stored procedure using EXEC syntax.
func (c *MSSQLConnector) CallProcedure(ctx context.Context, req connector.ProcedureCallRequest) (*connector.ResultSet, error) {
	query, args := c.qb.BuildProcedureCall(req)
	rows, err := c.db.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("mssql: call procedure %q failed: %w", req.Name, err)
	}
	defer rows.Close()

	return scanRows(rows)
}

// scanRows converts *sql.Rows into a ResultSet.
func scanRows(rows *sql.Rows) (*connector.ResultSet, error) {
	cols, err := rows.Columns()
	if err != nil {
		return nil, fmt.Errorf("mssql: failed to get columns: %w", err)
	}

	result := &connector.ResultSet{
		Columns: cols,
		Rows:    make([]map[string]any, 0),
	}

	for rows.Next() {
		values := make([]any, len(cols))
		ptrs := make([]any, len(cols))
		for i := range values {
			ptrs[i] = &values[i]
		}
		if err := rows.Scan(ptrs...); err != nil {
			return nil, fmt.Errorf("mssql: scan failed: %w", err)
		}

		row := make(map[string]any, len(cols))
		for i, col := range cols {
			val := values[i]
			// Convert []byte to string for JSON serialization friendliness.
			if b, ok := val.([]byte); ok {
				row[col] = string(b)
			} else {
				row[col] = val
			}
		}
		result.Rows = append(result.Rows, row)
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("mssql: row iteration failed: %w", err)
	}

	return result, nil
}

// schemaFilter returns the list of schemas to include in introspection.
// Defaults to ["dbo"] if none configured.
func (c *MSSQLConnector) schemaFilter() []string {
	if len(c.cfg.Schemas) > 0 {
		return c.cfg.Schemas
	}
	return []string{"dbo"}
}

// schemaPlaceholders generates a comma-separated list of @pN placeholders
// and returns the corresponding args slice for use in schema filter queries.
func (c *MSSQLConnector) schemaPlaceholders(startIdx int) (string, []any) {
	schemas := c.schemaFilter()
	placeholders := make([]string, len(schemas))
	args := make([]any, len(schemas))
	for i, s := range schemas {
		placeholders[i] = fmt.Sprintf("@p%d", startIdx+i)
		args[i] = s
	}
	return strings.Join(placeholders, ", "), args
}

// splitTableName splits a "schema.name" string. Defaults schema to "dbo".
func splitTableName(fullName string) (schemaName, name string) {
	parts := strings.SplitN(fullName, ".", 2)
	if len(parts) == 2 {
		return parts[0], parts[1]
	}
	return "dbo", parts[0]
}

// normalizeDSN converts various MSSQL DSN formats to the sqlserver:// format
// expected by the go-mssqldb driver.
func normalizeDSN(dsn string) string {
	// Strip mssql:// prefix and replace with sqlserver://.
	if strings.HasPrefix(dsn, "mssql://") {
		dsn = "sqlserver://" + strings.TrimPrefix(dsn, "mssql://")
	}
	return dsn
}
