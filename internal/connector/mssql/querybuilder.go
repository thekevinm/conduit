package mssql

import (
	"fmt"
	"sort"
	"strings"

	"github.com/conduitdb/conduit/internal/connector"
)

// QueryBuilder generates SQL Server-specific SQL with @pN parameterized placeholders.
type QueryBuilder struct{}

// QuoteIdentifier quotes a SQL identifier with square brackets.
// Handles schema-qualified names like [dbo].[users].
// Escapes embedded closing brackets by doubling them per SQL Server convention.
func (qb *QueryBuilder) QuoteIdentifier(name string) string {
	parts := strings.SplitN(name, ".", 2)
	for i, p := range parts {
		parts[i] = "[" + strings.ReplaceAll(p, "]", "]]") + "]"
	}
	return strings.Join(parts, ".")
}

// ParameterPlaceholder returns a SQL Server parameter placeholder (@p1, @p2, ...).
// Index is 1-based.
func (qb *QueryBuilder) ParameterPlaceholder(index int) string {
	return fmt.Sprintf("@p%d", index)
}

// BuildSelect builds a SELECT query from a SelectRequest.
// Uses OFFSET/FETCH NEXT for pagination (requires ORDER BY in SQL Server).
// Returns the query string and parameter values.
func (qb *QueryBuilder) BuildSelect(req connector.SelectRequest) (string, []any) {
	var sb strings.Builder
	var args []any

	// SELECT columns
	sb.WriteString("SELECT ")
	if len(req.Columns) == 0 {
		sb.WriteString("*")
	} else {
		quoted := make([]string, len(req.Columns))
		for i, col := range req.Columns {
			quoted[i] = qb.QuoteIdentifier(col)
		}
		sb.WriteString(strings.Join(quoted, ", "))
	}

	// FROM table
	sb.WriteString(" FROM ")
	sb.WriteString(qb.QuoteIdentifier(req.Table))

	// WHERE (pass-through filter — the filter parser upstream handles safety)
	if req.Filter != "" {
		sb.WriteString(" WHERE ")
		sb.WriteString(req.Filter)
	}

	// ORDER BY — required for OFFSET/FETCH NEXT in SQL Server.
	needsPagination := req.Limit > 0 || req.Offset > 0
	if req.OrderBy != "" {
		sb.WriteString(" ORDER BY ")
		sb.WriteString(req.OrderBy)
	} else if needsPagination {
		// SQL Server requires ORDER BY for OFFSET/FETCH NEXT.
		// Use a deterministic no-op ordering.
		sb.WriteString(" ORDER BY (SELECT NULL)")
	}

	// OFFSET / FETCH NEXT (SQL Server 2012+ pagination syntax)
	if needsPagination {
		offset := req.Offset
		args = append(args, offset)
		sb.WriteString(fmt.Sprintf(" OFFSET @p%d ROWS", len(args)))

		if req.Limit > 0 {
			args = append(args, req.Limit)
			sb.WriteString(fmt.Sprintf(" FETCH NEXT @p%d ROWS ONLY", len(args)))
		}
	}

	return sb.String(), args
}

// BuildInsert builds an INSERT statement for one or more rows.
// Uses a single multi-row VALUES clause for efficiency.
// Column order is deterministic (sorted).
func (qb *QueryBuilder) BuildInsert(req connector.InsertRequest) (string, []any) {
	if len(req.Rows) == 0 {
		return "", nil
	}

	// Collect all unique column names across all rows.
	colSet := make(map[string]bool)
	for _, row := range req.Rows {
		for col := range row {
			colSet[col] = true
		}
	}
	cols := make([]string, 0, len(colSet))
	for col := range colSet {
		cols = append(cols, col)
	}
	sort.Strings(cols)

	var sb strings.Builder
	var args []any

	sb.WriteString("INSERT INTO ")
	sb.WriteString(qb.QuoteIdentifier(req.Table))
	sb.WriteString(" (")
	quoted := make([]string, len(cols))
	for i, col := range cols {
		quoted[i] = qb.QuoteIdentifier(col)
	}
	sb.WriteString(strings.Join(quoted, ", "))
	sb.WriteString(") VALUES ")

	for rowIdx, row := range req.Rows {
		if rowIdx > 0 {
			sb.WriteString(", ")
		}
		sb.WriteString("(")
		for colIdx, col := range cols {
			if colIdx > 0 {
				sb.WriteString(", ")
			}
			val, ok := row[col]
			if !ok {
				sb.WriteString("DEFAULT")
			} else {
				args = append(args, val)
				sb.WriteString(fmt.Sprintf("@p%d", len(args)))
			}
		}
		sb.WriteString(")")
	}

	return sb.String(), args
}

// BuildUpdate builds an UPDATE statement.
// SET columns are sorted deterministically. Filter is passed through.
func (qb *QueryBuilder) BuildUpdate(req connector.UpdateRequest) (string, []any) {
	cols := make([]string, 0, len(req.Set))
	for col := range req.Set {
		cols = append(cols, col)
	}
	sort.Strings(cols)

	var sb strings.Builder
	var args []any

	sb.WriteString("UPDATE ")
	sb.WriteString(qb.QuoteIdentifier(req.Table))
	sb.WriteString(" SET ")

	for i, col := range cols {
		if i > 0 {
			sb.WriteString(", ")
		}
		args = append(args, req.Set[col])
		sb.WriteString(fmt.Sprintf("%s = @p%d", qb.QuoteIdentifier(col), len(args)))
	}

	if req.Filter != "" {
		sb.WriteString(" WHERE ")
		sb.WriteString(req.Filter)
	}

	return sb.String(), args
}

// BuildDelete builds a DELETE statement. Filter is required at the caller level.
func (qb *QueryBuilder) BuildDelete(req connector.DeleteRequest) (string, []any) {
	var sb strings.Builder

	sb.WriteString("DELETE FROM ")
	sb.WriteString(qb.QuoteIdentifier(req.Table))

	if req.Filter != "" {
		sb.WriteString(" WHERE ")
		sb.WriteString(req.Filter)
	}

	return sb.String(), nil
}

// BuildProcedureCall builds an EXEC statement for calling a stored procedure.
// SQL Server uses EXEC proc_name @param1 = @p1, @param2 = @p2 syntax.
func (qb *QueryBuilder) BuildProcedureCall(req connector.ProcedureCallRequest) (string, []any) {
	// Sort parameter names for deterministic ordering.
	paramNames := make([]string, 0, len(req.Params))
	for name := range req.Params {
		paramNames = append(paramNames, name)
	}
	sort.Strings(paramNames)

	var sb strings.Builder
	var args []any

	sb.WriteString("EXEC ")
	sb.WriteString(qb.QuoteIdentifier(req.Name))

	for i, name := range paramNames {
		if i > 0 {
			sb.WriteString(",")
		}
		args = append(args, req.Params[name])
		// Use named parameter syntax: @param_name = @pN
		sb.WriteString(fmt.Sprintf(" @%s = @p%d", name, len(args)))
	}

	return sb.String(), args
}
