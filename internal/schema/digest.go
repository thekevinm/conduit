package schema

import (
	"fmt"
	"strings"
)

// Digest produces a token-efficient, one-line summary of a table suitable for
// inclusion in an LLM prompt. The format is:
//
//	table_name (col1 type PK, col2 type, col3 type FK->ref_table.ref_col) ~1234 rows
//
// This minimizes token usage while giving the LLM enough context to generate
// correct queries.
func Digest(td *TableDetail) string {
	if td == nil {
		return ""
	}

	var b strings.Builder
	b.WriteString(td.Name)
	b.WriteString(" (")

	for i, col := range td.Columns {
		if i > 0 {
			b.WriteString(", ")
		}
		b.WriteString(col.Name)
		b.WriteByte(' ')
		b.WriteString(col.Type)

		if col.PK {
			b.WriteString(" PK")
		}
		if col.FK != "" {
			b.WriteString(" FK->")
			b.WriteString(col.FK)
		}
		if col.Nullable {
			b.WriteByte('?')
		}
	}

	b.WriteByte(')')

	if td.RowCount > 0 {
		b.WriteString(fmt.Sprintf(" ~%s rows", formatCount(td.RowCount)))
	}

	return b.String()
}

// DigestAll produces a newline-separated digest of multiple tables, suitable
// for a system prompt listing all available tables.
func DigestAll(tables []*TableDetail) string {
	if len(tables) == 0 {
		return ""
	}

	lines := make([]string, 0, len(tables))
	for _, td := range tables {
		if td != nil {
			lines = append(lines, Digest(td))
		}
	}
	return strings.Join(lines, "\n")
}

// DigestSummary produces a minimal one-line summary from a TableSummary,
// without column details. Useful for the list_tables tool response.
func DigestSummary(ts TableSummary) string {
	var b strings.Builder
	b.WriteString(ts.Name)
	if ts.Type != "" && ts.Type != "table" {
		b.WriteString(" [")
		b.WriteString(ts.Type)
		b.WriteByte(']')
	}
	if ts.RowCount > 0 {
		b.WriteString(fmt.Sprintf(" ~%s rows", formatCount(ts.RowCount)))
	}
	return b.String()
}

// formatCount formats a row count in a compact, human-friendly way.
func formatCount(n int64) string {
	switch {
	case n >= 1_000_000_000:
		return fmt.Sprintf("%.1fB", float64(n)/1_000_000_000)
	case n >= 1_000_000:
		return fmt.Sprintf("%.1fM", float64(n)/1_000_000)
	case n >= 10_000:
		return fmt.Sprintf("%.1fK", float64(n)/1_000)
	default:
		return fmt.Sprintf("%d", n)
	}
}
