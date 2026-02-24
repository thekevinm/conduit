// Package query provides query orchestration, filter parsing, input
// sanitization, and validation for the Conduit MCP server.
package query

import (
	"fmt"
	"regexp"
	"strings"
)

// InjectionError is returned when a SQL injection pattern is detected.
type InjectionError struct {
	Pattern string
	Input   string
}

func (e *InjectionError) Error() string {
	return fmt.Sprintf("potential SQL injection detected: %s", e.Pattern)
}

// dangerousPatterns lists regex patterns that indicate SQL injection attempts.
// Each entry is a pattern name and a compiled regex.
type dangerousPattern struct {
	name    string
	pattern *regexp.Regexp
}

var dangerousPatterns = []dangerousPattern{
	// Semicolons — statement termination / chaining.
	{"semicolon", regexp.MustCompile(`;`)},
	// Line comments.
	{"line comment (--)", regexp.MustCompile(`--`)},
	// Block comments.
	{"block comment open (/*)", regexp.MustCompile(`/\*`)},
	{"block comment close (*/)", regexp.MustCompile(`\*/`)},
	// Subqueries and set operations (case-insensitive, word boundaries).
	{"SELECT keyword", regexp.MustCompile(`(?i)\bSELECT\b`)},
	{"FROM keyword", regexp.MustCompile(`(?i)\bFROM\b`)},
	{"WHERE keyword", regexp.MustCompile(`(?i)\bWHERE\b`)},
	{"UNION keyword", regexp.MustCompile(`(?i)\bUNION\b`)},
	{"INTERSECT keyword", regexp.MustCompile(`(?i)\bINTERSECT\b`)},
	{"EXCEPT keyword", regexp.MustCompile(`(?i)\bEXCEPT\b`)},
	// DDL keywords.
	{"DROP keyword", regexp.MustCompile(`(?i)\bDROP\b`)},
	{"ALTER keyword", regexp.MustCompile(`(?i)\bALTER\b`)},
	{"CREATE keyword", regexp.MustCompile(`(?i)\bCREATE\b`)},
	{"TRUNCATE keyword", regexp.MustCompile(`(?i)\bTRUNCATE\b`)},
	// DML keywords (when present as values, not as part of our own operations).
	{"INSERT keyword", regexp.MustCompile(`(?i)\bINSERT\b`)},
	{"UPDATE keyword", regexp.MustCompile(`(?i)\bUPDATE\b`)},
	{"DELETE keyword", regexp.MustCompile(`(?i)\bDELETE\b`)},
	// EXEC / EXECUTE for stored procedure injection.
	{"EXEC keyword", regexp.MustCompile(`(?i)\bEXEC\b`)},
	{"EXECUTE keyword", regexp.MustCompile(`(?i)\bEXECUTE\b`)},
}

// identifierPattern validates that a string is a safe SQL identifier:
// starts with a letter or underscore, followed by letters, digits, or underscores.
// Allows dotted names for schema.table references.
var identifierPattern = regexp.MustCompile(`^[a-zA-Z_][a-zA-Z0-9_]*(\.[a-zA-Z_][a-zA-Z0-9_]*)?$`)

// ValidateIdentifier checks that a string is a safe SQL identifier.
// Returns an error if the identifier contains dangerous characters.
func ValidateIdentifier(name string) error {
	if name == "" {
		return fmt.Errorf("empty identifier")
	}
	if !identifierPattern.MatchString(name) {
		return fmt.Errorf("invalid identifier %q: must match [a-zA-Z_][a-zA-Z0-9_]*(.[a-zA-Z_][a-zA-Z0-9_]*)?", name)
	}
	// Also reject identifiers that are dangerous SQL keywords.
	upper := strings.ToUpper(name)
	for _, kw := range dangerousKeywords {
		if upper == kw {
			return fmt.Errorf("identifier %q is a reserved keyword", name)
		}
	}
	return nil
}

// dangerousKeywords is a list of SQL keywords that should never be used as
// bare identifiers in our generated queries.
var dangerousKeywords = []string{
	"SELECT", "FROM", "WHERE", "INSERT", "UPDATE", "DELETE",
	"DROP", "ALTER", "CREATE", "TRUNCATE",
	"UNION", "INTERSECT", "EXCEPT",
	"EXEC", "EXECUTE",
}

// SanitizeFilterInput checks a raw filter string for SQL injection patterns.
// This is a defense-in-depth check applied BEFORE parsing. The parser itself
// also produces only parameterized queries, so injection via the filter value
// path is not possible even if this check were bypassed.
func SanitizeFilterInput(input string) error {
	// Strip out string literals before checking for dangerous patterns,
	// since string values like 'SELECT' should not trigger false positives.
	stripped := stripStringLiterals(input)

	for _, dp := range dangerousPatterns {
		if dp.pattern.MatchString(stripped) {
			return &InjectionError{
				Pattern: dp.name,
				Input:   input,
			}
		}
	}
	return nil
}

// SanitizeOrderBy validates an ORDER BY clause. Only allows comma-separated
// identifiers with optional ASC/DESC suffix.
func SanitizeOrderBy(orderBy string) error {
	if orderBy == "" {
		return nil
	}

	parts := strings.Split(orderBy, ",")
	for _, part := range parts {
		part = strings.TrimSpace(part)
		if part == "" {
			continue
		}
		tokens := strings.Fields(part)
		if len(tokens) == 0 || len(tokens) > 2 {
			return fmt.Errorf("invalid ORDER BY clause: %q", part)
		}
		if err := ValidateIdentifier(tokens[0]); err != nil {
			return fmt.Errorf("invalid ORDER BY column: %w", err)
		}
		if len(tokens) == 2 {
			dir := strings.ToUpper(tokens[1])
			if dir != "ASC" && dir != "DESC" {
				return fmt.Errorf("invalid ORDER BY direction: %q (expected ASC or DESC)", tokens[1])
			}
		}
	}
	return nil
}

// stripStringLiterals replaces all single-quoted string literals with a
// placeholder so that string content doesn't trigger injection detection.
// This correctly handles escaped quotes ('').
func stripStringLiterals(s string) string {
	var b strings.Builder
	inString := false
	i := 0
	for i < len(s) {
		if !inString {
			if s[i] == '\'' {
				inString = true
				b.WriteString("'__STR__'")
				i++
			} else {
				b.WriteByte(s[i])
				i++
			}
		} else {
			// Inside a string literal.
			if i+1 < len(s) && s[i] == '\'' && s[i+1] == '\'' {
				// Escaped quote — skip both.
				i += 2
			} else if s[i] == '\'' {
				// End of string literal.
				inString = false
				i++
			} else {
				i++
			}
		}
	}
	return b.String()
}
