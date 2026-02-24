// Package postgres implements the Connector interface for PostgreSQL using pgx.
package postgres

import (
	"context"
	"database/sql"
	"fmt"
	"strings"

	"github.com/conduitdb/conduit/internal/connector"

	_ "github.com/jackc/pgx/v5/stdlib" // PostgreSQL driver
)

func init() {
	connector.Register("postgres", func() connector.Connector {
		return &PostgresConnector{}
	})
}

// PostgresConnector implements connector.Connector for PostgreSQL.
type PostgresConnector struct {
	db       *sql.DB
	cfg      connector.ConnectionConfig
	qb       *QueryBuilder
	readOnly bool
}

// Open establishes a connection to the PostgreSQL database.
func (c *PostgresConnector) Open(ctx context.Context, cfg connector.ConnectionConfig) error {
	db, err := sql.Open("pgx", cfg.DSN)
	if err != nil {
		return fmt.Errorf("postgres: failed to open connection: %w", err)
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
		return fmt.Errorf("postgres: failed to ping database: %w", err)
	}

	c.db = db
	c.cfg = cfg
	c.readOnly = cfg.ReadOnly
	c.qb = &QueryBuilder{}
	return nil
}

// Close closes the database connection pool.
func (c *PostgresConnector) Close() error {
	if c.db != nil {
		return c.db.Close()
	}
	return nil
}

// Ping verifies the connection is still alive.
func (c *PostgresConnector) Ping(ctx context.Context) error {
	if c.db == nil {
		return fmt.Errorf("postgres: connection not open")
	}
	return c.db.PingContext(ctx)
}

// DriverName returns the driver identifier.
func (c *PostgresConnector) DriverName() string {
	return "postgres"
}

// QuoteIdentifier quotes an identifier for safe use in SQL.
// Handles schema-qualified names (e.g., "public"."users").
func (c *PostgresConnector) QuoteIdentifier(name string) string {
	return c.qb.QuoteIdentifier(name)
}

// ParameterPlaceholder returns the PostgreSQL placeholder format ($1, $2, ...).
func (c *PostgresConnector) ParameterPlaceholder(index int) string {
	return c.qb.ParameterPlaceholder(index)
}

// Select executes a typed SELECT query.
func (c *PostgresConnector) Select(ctx context.Context, req connector.SelectRequest) (*connector.ResultSet, error) {
	query, args := c.qb.BuildSelect(req)
	rows, err := c.db.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("postgres: select failed: %w", err)
	}
	defer rows.Close()

	return scanRows(rows)
}

// Insert executes a typed INSERT statement.
func (c *PostgresConnector) Insert(ctx context.Context, req connector.InsertRequest) (*connector.MutationResult, error) {
	if c.readOnly {
		return nil, fmt.Errorf("postgres: insert denied — connection is read-only")
	}
	if len(req.Rows) == 0 {
		return &connector.MutationResult{RowsAffected: 0}, nil
	}

	query, args := c.qb.BuildInsert(req)
	result, err := c.db.ExecContext(ctx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("postgres: insert failed: %w", err)
	}

	affected, _ := result.RowsAffected()
	return &connector.MutationResult{RowsAffected: affected}, nil
}

// Update executes a typed UPDATE statement.
func (c *PostgresConnector) Update(ctx context.Context, req connector.UpdateRequest) (*connector.MutationResult, error) {
	if c.readOnly {
		return nil, fmt.Errorf("postgres: update denied — connection is read-only")
	}
	if req.Filter == "" {
		return nil, fmt.Errorf("postgres: update requires a filter (refusing unfiltered update)")
	}

	query, args := c.qb.BuildUpdate(req)
	result, err := c.db.ExecContext(ctx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("postgres: update failed: %w", err)
	}

	affected, _ := result.RowsAffected()
	return &connector.MutationResult{RowsAffected: affected}, nil
}

// Delete executes a typed DELETE statement.
func (c *PostgresConnector) Delete(ctx context.Context, req connector.DeleteRequest) (*connector.MutationResult, error) {
	if c.readOnly {
		return nil, fmt.Errorf("postgres: delete denied — connection is read-only")
	}
	if req.Filter == "" {
		return nil, fmt.Errorf("postgres: delete requires a filter (refusing unfiltered delete)")
	}

	query, args := c.qb.BuildDelete(req)
	result, err := c.db.ExecContext(ctx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("postgres: delete failed: %w", err)
	}

	affected, _ := result.RowsAffected()
	return &connector.MutationResult{RowsAffected: affected}, nil
}

// CallProcedure executes a stored procedure or function.
func (c *PostgresConnector) CallProcedure(ctx context.Context, req connector.ProcedureCallRequest) (*connector.ResultSet, error) {
	query, args := c.qb.BuildProcedureCall(req)
	rows, err := c.db.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("postgres: call procedure %q failed: %w", req.Name, err)
	}
	defer rows.Close()

	return scanRows(rows)
}

// scanRows converts *sql.Rows into a ResultSet.
func scanRows(rows *sql.Rows) (*connector.ResultSet, error) {
	cols, err := rows.Columns()
	if err != nil {
		return nil, fmt.Errorf("postgres: failed to get columns: %w", err)
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
			return nil, fmt.Errorf("postgres: scan failed: %w", err)
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
		return nil, fmt.Errorf("postgres: row iteration failed: %w", err)
	}

	return result, nil
}

// schemaFilter returns the list of schemas to include in introspection.
// Defaults to ["public"] if none configured.
func (c *PostgresConnector) schemaFilter() []string {
	if len(c.cfg.Schemas) > 0 {
		return c.cfg.Schemas
	}
	return []string{"public"}
}

// schemaPlaceholders generates a comma-separated list of $N placeholders
// and returns the corresponding args slice for use in schema filter queries.
func (c *PostgresConnector) schemaPlaceholders(startIdx int) (string, []any) {
	schemas := c.schemaFilter()
	placeholders := make([]string, len(schemas))
	args := make([]any, len(schemas))
	for i, s := range schemas {
		placeholders[i] = fmt.Sprintf("$%d", startIdx+i)
		args[i] = s
	}
	return strings.Join(placeholders, ", "), args
}
