package query

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	"github.com/conduitdb/conduit/internal/connector"
	"github.com/conduitdb/conduit/internal/schema"
)

// Engine orchestrates query execution. It validates inputs, parses filters,
// applies PII masking, and delegates to the underlying connector. All user
// input passes through sanitization and parameterization before reaching the
// database.
type Engine struct {
	connector   connector.Connector
	cache       *schema.Cache
	validator   *Validator
	piiDetector *schema.PIIDetector
	maskPII     bool
	logger      *slog.Logger
}

// EngineConfig configures the query engine.
type EngineConfig struct {
	// Limits defines query complexity constraints.
	Limits Limits

	// MaskPII enables PII detection and masking on query results.
	MaskPII bool
}

// NewEngine creates a query engine wired to the given connector and schema cache.
func NewEngine(conn connector.Connector, cache *schema.Cache, cfg EngineConfig, logger *slog.Logger) *Engine {
	if logger == nil {
		logger = slog.Default()
	}
	return &Engine{
		connector:   conn,
		cache:       cache,
		validator:   NewValidator(cfg.Limits),
		piiDetector: schema.NewPIIDetector(),
		maskPII:     cfg.MaskPII,
		logger:      logger,
	}
}

// Select executes a validated, parameterized SELECT query.
func (e *Engine) Select(ctx context.Context, req connector.SelectRequest) (*connector.ResultSet, error) {
	// Validate the request.
	if err := e.validator.ValidateSelect(req.Table, req.Limit, req.Offset); err != nil {
		return nil, err
	}
	if err := e.validator.ValidateColumns(req.Columns); err != nil {
		return nil, err
	}

	// Clamp the limit.
	req.Limit = e.validator.ClampLimit(req.Limit)

	// Validate ORDER BY if present.
	if req.OrderBy != "" {
		if err := SanitizeOrderBy(req.OrderBy); err != nil {
			return nil, err
		}
	}

	// Parse and validate the filter (the raw filter string gets replaced with
	// a parameterized version by the connector — our job is to reject
	// dangerous input before it reaches the connector).
	if req.Filter != "" {
		if err := SanitizeFilterInput(req.Filter); err != nil {
			return nil, err
		}
	}

	// Apply query timeout.
	queryCtx, cancel := context.WithTimeout(ctx, e.validator.QueryTimeout())
	defer cancel()

	start := time.Now()
	rs, err := e.connector.Select(queryCtx, req)
	elapsed := time.Since(start)

	if err != nil {
		e.logger.Warn("query failed",
			slog.String("table", req.Table),
			slog.Duration("elapsed", elapsed),
			slog.String("error", err.Error()))
		return nil, fmt.Errorf("query execution failed: %w", err)
	}

	e.logger.Debug("query executed",
		slog.String("table", req.Table),
		slog.Int("rows", len(rs.Rows)),
		slog.Duration("elapsed", elapsed))

	// Apply PII masking if enabled.
	if e.maskPII && len(rs.Rows) > 0 {
		if err := e.applyPIIMasking(ctx, req.Table, rs); err != nil {
			e.logger.Warn("PII masking failed, returning unmasked results",
				slog.String("table", req.Table),
				slog.String("error", err.Error()))
		}
	}

	return rs, nil
}

// Insert executes a validated INSERT operation.
func (e *Engine) Insert(ctx context.Context, req connector.InsertRequest) (*connector.MutationResult, error) {
	if err := e.validator.ValidateWrite(req.Table); err != nil {
		return nil, err
	}
	if len(req.Rows) == 0 {
		return nil, &ValidationError{Field: "rows", Message: "at least one row is required"}
	}

	queryCtx, cancel := context.WithTimeout(ctx, e.validator.QueryTimeout())
	defer cancel()

	return e.connector.Insert(queryCtx, req)
}

// Update executes a validated UPDATE operation.
func (e *Engine) Update(ctx context.Context, req connector.UpdateRequest) (*connector.MutationResult, error) {
	if err := e.validator.ValidateWrite(req.Table); err != nil {
		return nil, err
	}
	if req.Filter == "" {
		return nil, &ValidationError{Field: "filter", Message: "a filter is required for UPDATE operations (use describe_table to see rows)"}
	}
	if len(req.Set) == 0 {
		return nil, &ValidationError{Field: "set", Message: "at least one column must be set"}
	}
	if err := SanitizeFilterInput(req.Filter); err != nil {
		return nil, err
	}

	queryCtx, cancel := context.WithTimeout(ctx, e.validator.QueryTimeout())
	defer cancel()

	return e.connector.Update(queryCtx, req)
}

// Delete executes a validated DELETE operation.
func (e *Engine) Delete(ctx context.Context, req connector.DeleteRequest) (*connector.MutationResult, error) {
	if err := e.validator.ValidateWrite(req.Table); err != nil {
		return nil, err
	}
	if req.Filter == "" {
		return nil, &ValidationError{Field: "filter", Message: "a filter is required for DELETE operations"}
	}
	if err := SanitizeFilterInput(req.Filter); err != nil {
		return nil, err
	}

	queryCtx, cancel := context.WithTimeout(ctx, e.validator.QueryTimeout())
	defer cancel()

	return e.connector.Delete(queryCtx, req)
}

// applyPIIMasking detects and masks PII columns in query results.
func (e *Engine) applyPIIMasking(ctx context.Context, tableName string, rs *connector.ResultSet) error {
	td, err := e.cache.DescribeTable(ctx, tableName)
	if err != nil {
		return fmt.Errorf("failed to get table detail for PII detection: %w", err)
	}

	// Get columns that need masking.
	maskedCols := e.piiDetector.MaskedColumns(td)
	excludedCols := e.piiDetector.ExcludedColumns(td)

	if len(maskedCols) == 0 && len(excludedCols) == 0 {
		return nil
	}

	// Remove excluded columns from results.
	for _, row := range rs.Rows {
		for col := range excludedCols {
			delete(row, col)
		}
	}

	// Remove excluded columns from the column list.
	if len(excludedCols) > 0 {
		filtered := make([]string, 0, len(rs.Columns))
		for _, col := range rs.Columns {
			if !excludedCols[col] {
				filtered = append(filtered, col)
			}
		}
		rs.Columns = filtered
	}

	// Mask sensitive values.
	schema.MaskRows(rs.Rows, maskedCols)

	return nil
}

// Validator returns the engine's validator for external use.
func (e *Engine) Validator() *Validator {
	return e.validator
}
