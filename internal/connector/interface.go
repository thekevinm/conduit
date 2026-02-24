package connector

import (
	"context"
	"time"

	"github.com/conduitdb/conduit/internal/schema"
)

// Connector defines the interface all database drivers must implement.
type Connector interface {
	// Lifecycle
	Open(ctx context.Context, cfg ConnectionConfig) error
	Close() error
	Ping(ctx context.Context) error

	// Introspection
	ListTables(ctx context.Context) ([]schema.TableSummary, error)
	DescribeTable(ctx context.Context, tableName string) (*schema.TableDetail, error)
	ListProcedures(ctx context.Context) ([]schema.ProcedureSummary, error)
	DescribeProcedure(ctx context.Context, name string) (*schema.ProcedureDetail, error)

	// CRUD
	Select(ctx context.Context, req SelectRequest) (*ResultSet, error)
	Insert(ctx context.Context, req InsertRequest) (*MutationResult, error)
	Update(ctx context.Context, req UpdateRequest) (*MutationResult, error)
	Delete(ctx context.Context, req DeleteRequest) (*MutationResult, error)

	// Stored procedures
	CallProcedure(ctx context.Context, req ProcedureCallRequest) (*ResultSet, error)

	// SQL dialect helpers
	DriverName() string
	QuoteIdentifier(name string) string
	ParameterPlaceholder(index int) string
}

// ConnectionConfig holds database connection settings.
type ConnectionConfig struct {
	DSN             string
	MaxOpenConns    int
	MaxIdleConns    int
	ConnMaxLifetime time.Duration
	ReadOnly        bool
	Schemas         []string
	ExcludeTables   []string
	IncludeTables   []string
}

// SelectRequest represents a typed query request.
type SelectRequest struct {
	Table   string
	Columns []string
	Filter  string
	OrderBy string
	Limit   int
	Offset  int
}

// InsertRequest represents a typed insert request.
type InsertRequest struct {
	Table string
	Rows  []map[string]any
}

// UpdateRequest represents a typed update request.
type UpdateRequest struct {
	Table  string
	Filter string
	Set    map[string]any
}

// DeleteRequest represents a typed delete request.
type DeleteRequest struct {
	Table  string
	Filter string
}

// ProcedureCallRequest represents a stored procedure call.
type ProcedureCallRequest struct {
	Name   string
	Params map[string]any
}

// ResultSet holds query results.
type ResultSet struct {
	Columns []string         `json:"columns"`
	Rows    []map[string]any `json:"rows"`
	Total   int64            `json:"total,omitempty"`
}

// MutationResult holds the result of an insert/update/delete.
type MutationResult struct {
	RowsAffected int64            `json:"rows_affected"`
	Returning    []map[string]any `json:"returning,omitempty"`
}
