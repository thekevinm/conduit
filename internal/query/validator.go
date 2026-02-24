package query

import (
	"fmt"
	"time"
)

// Limits defines the complexity constraints applied to queries.
type Limits struct {
	// MaxRows caps the number of rows a single query can return. Default: 1000.
	MaxRows int

	// MaxResultSizeBytes caps the approximate result payload size. Default: 10MB.
	MaxResultSizeBytes int64

	// QueryTimeout is the maximum time a single query may execute. Default: 30s.
	QueryTimeout time.Duration

	// MaxFilterDepth limits nested expression depth. Default: 10.
	MaxFilterDepth int

	// AllowWrites enables INSERT/UPDATE/DELETE operations. Default: false.
	AllowWrites bool
}

// DefaultLimits returns conservative query limits suitable for production.
func DefaultLimits() Limits {
	return Limits{
		MaxRows:            1000,
		MaxResultSizeBytes: 10 * 1024 * 1024, // 10 MB
		QueryTimeout:       30 * time.Second,
		MaxFilterDepth:     10,
		AllowWrites:        false,
	}
}

// ValidationError is returned when a query fails validation.
type ValidationError struct {
	Field   string
	Message string
}

func (e *ValidationError) Error() string {
	return fmt.Sprintf("query validation failed on %s: %s", e.Field, e.Message)
}

// Validator enforces query complexity limits.
type Validator struct {
	limits Limits
}

// NewValidator creates a validator with the given limits.
func NewValidator(limits Limits) *Validator {
	if limits.MaxRows <= 0 {
		limits.MaxRows = 1000
	}
	if limits.MaxResultSizeBytes <= 0 {
		limits.MaxResultSizeBytes = 10 * 1024 * 1024
	}
	if limits.QueryTimeout <= 0 {
		limits.QueryTimeout = 30 * time.Second
	}
	if limits.MaxFilterDepth <= 0 {
		limits.MaxFilterDepth = 10
	}
	return &Validator{limits: limits}
}

// ValidateSelect checks that a select query meets complexity limits.
func (v *Validator) ValidateSelect(table string, limit int, offset int) error {
	if table == "" {
		return &ValidationError{Field: "table", Message: "table name is required"}
	}
	if err := ValidateIdentifier(table); err != nil {
		return &ValidationError{Field: "table", Message: err.Error()}
	}

	if limit <= 0 {
		// Will use default max.
		return nil
	}
	if limit > v.limits.MaxRows {
		return &ValidationError{
			Field:   "limit",
			Message: fmt.Sprintf("limit %d exceeds maximum of %d", limit, v.limits.MaxRows),
		}
	}
	if offset < 0 {
		return &ValidationError{
			Field:   "offset",
			Message: "offset must not be negative",
		}
	}
	return nil
}

// ValidateWrite checks that write operations are permitted.
func (v *Validator) ValidateWrite(table string) error {
	if !v.limits.AllowWrites {
		return &ValidationError{
			Field:   "operation",
			Message: "write operations are disabled (start with --allow-writes to enable)",
		}
	}
	if table == "" {
		return &ValidationError{Field: "table", Message: "table name is required"}
	}
	if err := ValidateIdentifier(table); err != nil {
		return &ValidationError{Field: "table", Message: err.Error()}
	}
	return nil
}

// ClampLimit enforces the maximum row limit, applying a default if needed.
func (v *Validator) ClampLimit(requested int) int {
	if requested <= 0 || requested > v.limits.MaxRows {
		return v.limits.MaxRows
	}
	return requested
}

// QueryTimeout returns the configured query timeout.
func (v *Validator) QueryTimeout() time.Duration {
	return v.limits.QueryTimeout
}

// MaxRows returns the configured maximum rows.
func (v *Validator) MaxRows() int {
	return v.limits.MaxRows
}

// AllowWrites returns whether write operations are enabled.
func (v *Validator) AllowWrites() bool {
	return v.limits.AllowWrites
}

// ValidateResultSize checks whether a result set exceeds the size limit.
// The size is approximate — it uses the number of rows and a rough per-row
// estimate.
func (v *Validator) ValidateResultSize(rowCount int, estimatedRowBytes int) error {
	if estimatedRowBytes <= 0 {
		estimatedRowBytes = 512 // conservative default
	}
	totalBytes := int64(rowCount) * int64(estimatedRowBytes)
	if totalBytes > v.limits.MaxResultSizeBytes {
		return &ValidationError{
			Field: "result_size",
			Message: fmt.Sprintf("estimated result size %dMB exceeds limit of %dMB",
				totalBytes/(1024*1024),
				v.limits.MaxResultSizeBytes/(1024*1024)),
		}
	}
	return nil
}

// ValidateColumns checks that requested columns are valid identifiers.
func (v *Validator) ValidateColumns(columns []string) error {
	for _, col := range columns {
		if col == "*" {
			continue
		}
		if err := ValidateIdentifier(col); err != nil {
			return &ValidationError{
				Field:   "columns",
				Message: fmt.Sprintf("invalid column name: %s", err.Error()),
			}
		}
	}
	return nil
}
