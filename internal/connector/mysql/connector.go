// Package mysql implements the Connector interface for MySQL using go-sql-driver/mysql.
package mysql

import (
	"context"
	"database/sql"
	"fmt"
	"strings"

	"github.com/conduitdb/conduit/internal/connector"

	_ "github.com/go-sql-driver/mysql" // MySQL driver
)

func init() {
	connector.Register("mysql", func() connector.Connector {
		return &MySQLConnector{}
	})
}

// MySQLConnector implements connector.Connector for MySQL.
type MySQLConnector struct {
	db       *sql.DB
	cfg      connector.ConnectionConfig
	qb       *QueryBuilder
	readOnly bool
}

// Open establishes a connection to the MySQL database.
func (c *MySQLConnector) Open(ctx context.Context, cfg connector.ConnectionConfig) error {
	// Strip mysql:// prefix — go-sql-driver/mysql expects the raw DSN format
	// (user:pass@tcp(host:port)/dbname?params).
	dsn := cfg.DSN
	dsn = strings.TrimPrefix(dsn, "mysql://")

	db, err := sql.Open("mysql", dsn)
	if err != nil {
		return fmt.Errorf("mysql: failed to open connection: %w", err)
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
		return fmt.Errorf("mysql: failed to ping database: %w", err)
	}

	c.db = db
	c.cfg = cfg
	c.readOnly = cfg.ReadOnly
	c.qb = &QueryBuilder{}
	return nil
}

// Close closes the database connection pool.
func (c *MySQLConnector) Close() error {
	if c.db != nil {
		return c.db.Close()
	}
	return nil
}

// Ping verifies the connection is still alive.
func (c *MySQLConnector) Ping(ctx context.Context) error {
	if c.db == nil {
		return fmt.Errorf("mysql: connection not open")
	}
	return c.db.PingContext(ctx)
}

// DriverName returns the driver identifier.
func (c *MySQLConnector) DriverName() string {
	return "mysql"
}

// QuoteIdentifier quotes an identifier for safe use in SQL.
// Handles schema-qualified names (e.g., `mydb`.`users`).
func (c *MySQLConnector) QuoteIdentifier(name string) string {
	return c.qb.QuoteIdentifier(name)
}

// ParameterPlaceholder returns the MySQL placeholder format (?).
func (c *MySQLConnector) ParameterPlaceholder(index int) string {
	return c.qb.ParameterPlaceholder(index)
}

// Select executes a typed SELECT query.
func (c *MySQLConnector) Select(ctx context.Context, req connector.SelectRequest) (*connector.ResultSet, error) {
	query, args := c.qb.BuildSelect(req)
	rows, err := c.db.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("mysql: select failed: %w", err)
	}
	defer rows.Close()

	return scanRows(rows)
}

// Insert executes a typed INSERT statement.
func (c *MySQLConnector) Insert(ctx context.Context, req connector.InsertRequest) (*connector.MutationResult, error) {
	if c.readOnly {
		return nil, fmt.Errorf("mysql: insert denied — connection is read-only")
	}
	if len(req.Rows) == 0 {
		return &connector.MutationResult{RowsAffected: 0}, nil
	}

	query, args := c.qb.BuildInsert(req)
	result, err := c.db.ExecContext(ctx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("mysql: insert failed: %w", err)
	}

	affected, _ := result.RowsAffected()
	return &connector.MutationResult{RowsAffected: affected}, nil
}

// Update executes a typed UPDATE statement.
func (c *MySQLConnector) Update(ctx context.Context, req connector.UpdateRequest) (*connector.MutationResult, error) {
	if c.readOnly {
		return nil, fmt.Errorf("mysql: update denied — connection is read-only")
	}
	if req.Filter == "" {
		return nil, fmt.Errorf("mysql: update requires a filter (refusing unfiltered update)")
	}

	query, args := c.qb.BuildUpdate(req)
	result, err := c.db.ExecContext(ctx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("mysql: update failed: %w", err)
	}

	affected, _ := result.RowsAffected()
	return &connector.MutationResult{RowsAffected: affected}, nil
}

// Delete executes a typed DELETE statement.
func (c *MySQLConnector) Delete(ctx context.Context, req connector.DeleteRequest) (*connector.MutationResult, error) {
	if c.readOnly {
		return nil, fmt.Errorf("mysql: delete denied — connection is read-only")
	}
	if req.Filter == "" {
		return nil, fmt.Errorf("mysql: delete requires a filter (refusing unfiltered delete)")
	}

	query, args := c.qb.BuildDelete(req)
	result, err := c.db.ExecContext(ctx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("mysql: delete failed: %w", err)
	}

	affected, _ := result.RowsAffected()
	return &connector.MutationResult{RowsAffected: affected}, nil
}

// CallProcedure executes a stored procedure using the CALL statement.
func (c *MySQLConnector) CallProcedure(ctx context.Context, req connector.ProcedureCallRequest) (*connector.ResultSet, error) {
	query, args := c.qb.BuildProcedureCall(req)
	rows, err := c.db.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("mysql: call procedure %q failed: %w", req.Name, err)
	}
	defer rows.Close()

	return scanRows(rows)
}

// scanRows converts *sql.Rows into a ResultSet.
func scanRows(rows *sql.Rows) (*connector.ResultSet, error) {
	cols, err := rows.Columns()
	if err != nil {
		return nil, fmt.Errorf("mysql: failed to get columns: %w", err)
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
			return nil, fmt.Errorf("mysql: scan failed: %w", err)
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
		return nil, fmt.Errorf("mysql: row iteration failed: %w", err)
	}

	return result, nil
}

// schemaName returns the current database name from the connection.
func (c *MySQLConnector) schemaName(ctx context.Context) (string, error) {
	var dbName string
	err := c.db.QueryRowContext(ctx, "SELECT DATABASE()").Scan(&dbName)
	if err != nil {
		return "", fmt.Errorf("mysql: failed to get current database: %w", err)
	}
	return dbName, nil
}
