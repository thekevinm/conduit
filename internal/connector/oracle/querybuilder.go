package oracle

import (
	"fmt"
	"sort"
	"strings"

	"github.com/conduitdb/conduit/internal/connector"
)

// QueryBuilder generates Oracle-specific SQL with :N parameterized placeholders.
type QueryBuilder struct{}

// QuoteIdentifier quotes a SQL identifier with double quotes.
// Handles owner-qualified names like "HR"."EMPLOYEES".
// Escapes embedded double quotes by doubling them per SQL standard.
func (qb *QueryBuilder) QuoteIdentifier(name string) string {
	parts := strings.SplitN(name, ".", 2)
	for i, p := range parts {
		parts[i] = `"` + strings.ReplaceAll(p, `"`, `""`) + `"`
	}
	return strings.Join(parts, ".")
}

// ParameterPlaceholder returns an Oracle parameter placeholder (:1, :2, ...).
// Index is 1-based.
func (qb *QueryBuilder) ParameterPlaceholder(index int) string {
	return fmt.Sprintf(":%d", index)
}

// BuildSelect builds a SELECT query from a SelectRequest.
// Uses Oracle 12c+ OFFSET/FETCH FIRST syntax for pagination.
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

	// ORDER BY
	if req.OrderBy != "" {
		sb.WriteString(" ORDER BY ")
		sb.WriteString(req.OrderBy)
	}

	// OFFSET / FETCH FIRST (Oracle 12c+ row-limiting clause)
	if req.Offset > 0 {
		args = append(args, req.Offset)
		sb.WriteString(fmt.Sprintf(" OFFSET :%d ROWS", len(args)))
	}

	if req.Limit > 0 {
		args = append(args, req.Limit)
		sb.WriteString(fmt.Sprintf(" FETCH FIRST :%d ROWS ONLY", len(args)))
	}

	return sb.String(), args
}

// BuildInsert builds an INSERT statement for one or more rows.
// For a single row, uses a standard INSERT INTO ... VALUES (...).
// For multiple rows, uses INSERT ALL ... SELECT FROM DUAL (Oracle multi-row syntax).
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

	if len(req.Rows) == 1 {
		// Single-row insert: standard syntax.
		sb.WriteString("INSERT INTO ")
		sb.WriteString(qb.QuoteIdentifier(req.Table))
		sb.WriteString(" (")
		quoted := make([]string, len(cols))
		for i, col := range cols {
			quoted[i] = qb.QuoteIdentifier(col)
		}
		sb.WriteString(strings.Join(quoted, ", "))
		sb.WriteString(") VALUES (")

		row := req.Rows[0]
		for colIdx, col := range cols {
			if colIdx > 0 {
				sb.WriteString(", ")
			}
			val, ok := row[col]
			if !ok {
				sb.WriteString("DEFAULT")
			} else {
				args = append(args, val)
				sb.WriteString(fmt.Sprintf(":%d", len(args)))
			}
		}
		sb.WriteString(")")
	} else {
		// Multi-row insert: Oracle INSERT ALL syntax.
		sb.WriteString("INSERT ALL")
		tableName := qb.QuoteIdentifier(req.Table)
		quoted := make([]string, len(cols))
		for i, col := range cols {
			quoted[i] = qb.QuoteIdentifier(col)
		}
		colList := strings.Join(quoted, ", ")

		for _, row := range req.Rows {
			sb.WriteString(" INTO ")
			sb.WriteString(tableName)
			sb.WriteString(" (")
			sb.WriteString(colList)
			sb.WriteString(") VALUES (")
			for colIdx, col := range cols {
				if colIdx > 0 {
					sb.WriteString(", ")
				}
				val, ok := row[col]
				if !ok {
					sb.WriteString("DEFAULT")
				} else {
					args = append(args, val)
					sb.WriteString(fmt.Sprintf(":%d", len(args)))
				}
			}
			sb.WriteString(")")
		}
		sb.WriteString(" SELECT 1 FROM DUAL")
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
		sb.WriteString(fmt.Sprintf("%s = :%d", qb.QuoteIdentifier(col), len(args)))
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

// BuildProcedureCall builds a PL/SQL anonymous block to call a stored procedure.
// Uses BEGIN procedure_name(:1, :2, ...); END; syntax.
func (qb *QueryBuilder) BuildProcedureCall(req connector.ProcedureCallRequest) (string, []any) {
	// Sort parameter names for deterministic ordering.
	paramNames := make([]string, 0, len(req.Params))
	for name := range req.Params {
		paramNames = append(paramNames, name)
	}
	sort.Strings(paramNames)

	var sb strings.Builder
	var args []any

	sb.WriteString("BEGIN ")
	sb.WriteString(qb.QuoteIdentifier(req.Name))
	sb.WriteString("(")

	for i, name := range paramNames {
		if i > 0 {
			sb.WriteString(", ")
		}
		args = append(args, req.Params[name])
		// Use named parameter association: param_name => :N
		sb.WriteString(fmt.Sprintf("%s => :%d", qb.QuoteIdentifier(name), len(args)))
	}

	sb.WriteString("); END;")

	return sb.String(), args
}
