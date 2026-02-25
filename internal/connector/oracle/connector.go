// Package oracle implements the Connector interface for Oracle Database
// using the pure-Go go-ora driver.
package oracle

import (
	"context"
	"database/sql"
	"fmt"
	"strings"

	"github.com/conduitdb/conduit/internal/connector"

	_ "github.com/sijms/go-ora/v2" // Oracle driver
)

func init() {
	connector.Register("oracle", func() connector.Connector {
		return &OracleConnector{}
	})
}

// OracleConnector implements connector.Connector for Oracle Database.
type OracleConnector struct {
	db       *sql.DB
	cfg      connector.ConnectionConfig
	qb       *QueryBuilder
	readOnly bool
	owner    string // Current schema owner (UPPERCASE)
}

// Open establishes a connection to the Oracle database.
func (c *OracleConnector) Open(ctx context.Context, cfg connector.ConnectionConfig) error {
	// go-ora expects the DSN without the oracle:// scheme prefix.
	dsn := cfg.DSN
	dsn = strings.TrimPrefix(dsn, "oracle://")

	db, err := sql.Open("oracle", dsn)
	if err != nil {
		return fmt.Errorf("oracle: failed to open connection: %w", err)
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
		return fmt.Errorf("oracle: failed to ping database: %w", err)
	}

	// Detect the current schema owner.
	var owner string
	err = db.QueryRowContext(ctx,
		"SELECT SYS_CONTEXT('USERENV', 'CURRENT_SCHEMA') FROM DUAL").Scan(&owner)
	if err != nil {
		db.Close()
		return fmt.Errorf("oracle: failed to detect current schema: %w", err)
	}

	c.db = db
	c.cfg = cfg
	c.readOnly = cfg.ReadOnly
	c.owner = strings.ToUpper(owner)
	c.qb = &QueryBuilder{}
	return nil
}

// Close closes the database connection pool.
func (c *OracleConnector) Close() error {
	if c.db != nil {
		return c.db.Close()
	}
	return nil
}

// Ping verifies the connection is still alive.
func (c *OracleConnector) Ping(ctx context.Context) error {
	if c.db == nil {
		return fmt.Errorf("oracle: connection not open")
	}
	return c.db.PingContext(ctx)
}

// DriverName returns the driver identifier.
func (c *OracleConnector) DriverName() string {
	return "oracle"
}

// QuoteIdentifier quotes an identifier for safe use in SQL.
// Handles owner-qualified names (e.g., "HR"."EMPLOYEES").
func (c *OracleConnector) QuoteIdentifier(name string) string {
	return c.qb.QuoteIdentifier(name)
}

// ParameterPlaceholder returns the Oracle placeholder format (:1, :2, ...).
func (c *OracleConnector) ParameterPlaceholder(index int) string {
	return c.qb.ParameterPlaceholder(index)
}

// Select executes a typed SELECT query.
func (c *OracleConnector) Select(ctx context.Context, req connector.SelectRequest) (*connector.ResultSet, error) {
	query, args := c.qb.BuildSelect(req)
	rows, err := c.db.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("oracle: select failed: %w", err)
	}
	defer rows.Close()

	return scanRows(rows)
}

// Insert executes a typed INSERT statement.
func (c *OracleConnector) Insert(ctx context.Context, req connector.InsertRequest) (*connector.MutationResult, error) {
	if c.readOnly {
		return nil, fmt.Errorf("oracle: insert denied — connection is read-only")
	}
	if len(req.Rows) == 0 {
		return &connector.MutationResult{RowsAffected: 0}, nil
	}

	query, args := c.qb.BuildInsert(req)
	result, err := c.db.ExecContext(ctx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("oracle: insert failed: %w", err)
	}

	affected, _ := result.RowsAffected()
	return &connector.MutationResult{RowsAffected: affected}, nil
}

// Update executes a typed UPDATE statement.
func (c *OracleConnector) Update(ctx context.Context, req connector.UpdateRequest) (*connector.MutationResult, error) {
	if c.readOnly {
		return nil, fmt.Errorf("oracle: update denied — connection is read-only")
	}
	if req.Filter == "" {
		return nil, fmt.Errorf("oracle: update requires a filter (refusing unfiltered update)")
	}

	query, args := c.qb.BuildUpdate(req)
	result, err := c.db.ExecContext(ctx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("oracle: update failed: %w", err)
	}

	affected, _ := result.RowsAffected()
	return &connector.MutationResult{RowsAffected: affected}, nil
}

// Delete executes a typed DELETE statement.
func (c *OracleConnector) Delete(ctx context.Context, req connector.DeleteRequest) (*connector.MutationResult, error) {
	if c.readOnly {
		return nil, fmt.Errorf("oracle: delete denied — connection is read-only")
	}
	if req.Filter == "" {
		return nil, fmt.Errorf("oracle: delete requires a filter (refusing unfiltered delete)")
	}

	query, args := c.qb.BuildDelete(req)
	result, err := c.db.ExecContext(ctx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("oracle: delete failed: %w", err)
	}

	affected, _ := result.RowsAffected()
	return &connector.MutationResult{RowsAffected: affected}, nil
}

// CallProcedure executes a stored procedure using a PL/SQL anonymous block.
func (c *OracleConnector) CallProcedure(ctx context.Context, req connector.ProcedureCallRequest) (*connector.ResultSet, error) {
	query, args := c.qb.BuildProcedureCall(req)
	rows, err := c.db.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("oracle: call procedure %q failed: %w", req.Name, err)
	}
	defer rows.Close()

	return scanRows(rows)
}

// scanRows converts *sql.Rows into a ResultSet.
func scanRows(rows *sql.Rows) (*connector.ResultSet, error) {
	cols, err := rows.Columns()
	if err != nil {
		return nil, fmt.Errorf("oracle: failed to get columns: %w", err)
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
			return nil, fmt.Errorf("oracle: scan failed: %w", err)
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
		return nil, fmt.Errorf("oracle: row iteration failed: %w", err)
	}

	return result, nil
}

// ownerFilter returns the schema owner(s) to use for introspection.
// If Schemas is configured, returns those; otherwise uses the detected current owner.
func (c *OracleConnector) ownerFilter() []string {
	if len(c.cfg.Schemas) > 0 {
		// Oracle stores schema names in UPPERCASE.
		owners := make([]string, len(c.cfg.Schemas))
		for i, s := range c.cfg.Schemas {
			owners[i] = strings.ToUpper(s)
		}
		return owners
	}
	return []string{c.owner}
}

// ownerPlaceholders generates a comma-separated list of :N placeholders
// and returns the corresponding args slice for use in owner filter queries.
func (c *OracleConnector) ownerPlaceholders(startIdx int) (string, []any) {
	owners := c.ownerFilter()
	placeholders := make([]string, len(owners))
	args := make([]any, len(owners))
	for i, o := range owners {
		placeholders[i] = fmt.Sprintf(":%d", startIdx+i)
		args[i] = o
	}
	return strings.Join(placeholders, ", "), args
}
