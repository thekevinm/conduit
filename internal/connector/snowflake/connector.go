// Package snowflake implements the Connector interface for Snowflake using gosnowflake.
package snowflake

import (
	"context"
	"database/sql"
	"fmt"
	"strings"

	"github.com/conduitdb/conduit/internal/connector"

	_ "github.com/snowflakedb/gosnowflake" // Snowflake driver
)

func init() {
	connector.Register("snowflake", func() connector.Connector {
		return &SnowflakeConnector{}
	})
}

// SnowflakeConnector implements connector.Connector for Snowflake.
type SnowflakeConnector struct {
	db           *sql.DB
	cfg          connector.ConnectionConfig
	qb           *QueryBuilder
	readOnly     bool
	schemaName   string
	databaseName string
}

// Open establishes a connection to the Snowflake database.
func (c *SnowflakeConnector) Open(ctx context.Context, cfg connector.ConnectionConfig) error {
	// Strip snowflake:// prefix — the gosnowflake driver expects
	// the DSN in format: user:pass@account/database/schema?warehouse=WH
	dsn := cfg.DSN
	dsn = strings.TrimPrefix(dsn, "snowflake://")

	db, err := sql.Open("snowflake", dsn)
	if err != nil {
		return fmt.Errorf("snowflake: failed to open connection: %w", err)
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
		return fmt.Errorf("snowflake: failed to ping database: %w", err)
	}

	c.db = db
	c.cfg = cfg
	c.readOnly = cfg.ReadOnly
	c.qb = &QueryBuilder{}

	// Detect current database and schema from the active session.
	var dbName, schName string
	if err := db.QueryRowContext(ctx, "SELECT CURRENT_DATABASE(), CURRENT_SCHEMA()").Scan(&dbName, &schName); err != nil {
		// Fall back to reasonable defaults if introspection fails.
		dbName = "UNKNOWN"
		schName = "PUBLIC"
	}
	c.databaseName = dbName
	c.schemaName = schName

	return nil
}

// Close closes the database connection pool.
func (c *SnowflakeConnector) Close() error {
	if c.db != nil {
		return c.db.Close()
	}
	return nil
}

// Ping verifies the connection is still alive.
func (c *SnowflakeConnector) Ping(ctx context.Context) error {
	if c.db == nil {
		return fmt.Errorf("snowflake: connection not open")
	}
	return c.db.PingContext(ctx)
}

// DriverName returns the driver identifier.
func (c *SnowflakeConnector) DriverName() string {
	return "snowflake"
}

// QuoteIdentifier quotes an identifier for safe use in SQL.
// Handles schema-qualified and three-level names (e.g., "DB"."SCHEMA"."TABLE").
func (c *SnowflakeConnector) QuoteIdentifier(name string) string {
	return c.qb.QuoteIdentifier(name)
}

// ParameterPlaceholder returns the Snowflake placeholder format (?).
func (c *SnowflakeConnector) ParameterPlaceholder(index int) string {
	return c.qb.ParameterPlaceholder(index)
}

// Select executes a typed SELECT query.
func (c *SnowflakeConnector) Select(ctx context.Context, req connector.SelectRequest) (*connector.ResultSet, error) {
	query, args := c.qb.BuildSelect(req)
	rows, err := c.db.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("snowflake: select failed: %w", err)
	}
	defer rows.Close()

	return scanRows(rows)
}

// Insert executes a typed INSERT statement.
func (c *SnowflakeConnector) Insert(ctx context.Context, req connector.InsertRequest) (*connector.MutationResult, error) {
	if c.readOnly {
		return nil, fmt.Errorf("snowflake: insert denied — connection is read-only")
	}
	if len(req.Rows) == 0 {
		return &connector.MutationResult{RowsAffected: 0}, nil
	}

	query, args := c.qb.BuildInsert(req)
	result, err := c.db.ExecContext(ctx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("snowflake: insert failed: %w", err)
	}

	affected, _ := result.RowsAffected()
	return &connector.MutationResult{RowsAffected: affected}, nil
}

// Update executes a typed UPDATE statement.
func (c *SnowflakeConnector) Update(ctx context.Context, req connector.UpdateRequest) (*connector.MutationResult, error) {
	if c.readOnly {
		return nil, fmt.Errorf("snowflake: update denied — connection is read-only")
	}
	if req.Filter == "" {
		return nil, fmt.Errorf("snowflake: update requires a filter (refusing unfiltered update)")
	}

	query, args := c.qb.BuildUpdate(req)
	result, err := c.db.ExecContext(ctx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("snowflake: update failed: %w", err)
	}

	affected, _ := result.RowsAffected()
	return &connector.MutationResult{RowsAffected: affected}, nil
}

// Delete executes a typed DELETE statement.
func (c *SnowflakeConnector) Delete(ctx context.Context, req connector.DeleteRequest) (*connector.MutationResult, error) {
	if c.readOnly {
		return nil, fmt.Errorf("snowflake: delete denied — connection is read-only")
	}
	if req.Filter == "" {
		return nil, fmt.Errorf("snowflake: delete requires a filter (refusing unfiltered delete)")
	}

	query, args := c.qb.BuildDelete(req)
	result, err := c.db.ExecContext(ctx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("snowflake: delete failed: %w", err)
	}

	affected, _ := result.RowsAffected()
	return &connector.MutationResult{RowsAffected: affected}, nil
}

// CallProcedure executes a stored procedure using CALL syntax.
func (c *SnowflakeConnector) CallProcedure(ctx context.Context, req connector.ProcedureCallRequest) (*connector.ResultSet, error) {
	query, args := c.qb.BuildProcedureCall(req)
	rows, err := c.db.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("snowflake: call procedure %q failed: %w", req.Name, err)
	}
	defer rows.Close()

	return scanRows(rows)
}

// scanRows converts *sql.Rows into a ResultSet.
func scanRows(rows *sql.Rows) (*connector.ResultSet, error) {
	cols, err := rows.Columns()
	if err != nil {
		return nil, fmt.Errorf("snowflake: failed to get columns: %w", err)
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
			return nil, fmt.Errorf("snowflake: scan failed: %w", err)
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
		return nil, fmt.Errorf("snowflake: row iteration failed: %w", err)
	}

	return result, nil
}
