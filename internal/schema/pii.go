package schema

import (
	"fmt"
	"strings"
)

// PIIAction describes what to do with a column detected as containing PII.
type PIIAction int

const (
	// PIIActionNone means the column is not flagged as PII.
	PIIActionNone PIIAction = iota
	// PIIActionMask means the column value should be partially masked.
	PIIActionMask
	// PIIActionExclude means the column should be excluded from results entirely.
	PIIActionExclude
)

// String returns a human-readable representation of the PIIAction.
func (a PIIAction) String() string {
	switch a {
	case PIIActionMask:
		return "mask"
	case PIIActionExclude:
		return "exclude"
	default:
		return "none"
	}
}

// PIICategory identifies the type of PII detected.
type PIICategory string

const (
	PIICategoryEmail      PIICategory = "email"
	PIICategoryPhone      PIICategory = "phone"
	PIICategorySSN        PIICategory = "ssn"
	PIICategoryCreditCard PIICategory = "credit_card"
	PIICategoryPassword   PIICategory = "password"
	PIICategoryToken      PIICategory = "token"
)

// PIIMatch holds the result of PII detection for a single column.
type PIIMatch struct {
	Column   string      `json:"column"`
	Category PIICategory `json:"category"`
	Action   PIIAction   `json:"action"`
}

// piiPattern defines a detection rule: substring patterns to match against
// lowercase column names, and the resulting action.
type piiPattern struct {
	category PIICategory
	patterns []string
	action   PIIAction
}

// builtinPatterns is the default set of PII detection rules, ordered by
// specificity (exclude patterns first so they take priority).
var builtinPatterns = []piiPattern{
	// Exclude patterns — these columns should never be returned.
	{
		category: PIICategoryPassword,
		patterns: []string{"password", "passwd", "secret"},
		action:   PIIActionExclude,
	},
	{
		category: PIICategoryToken,
		patterns: []string{"token", "api_key", "secret_key"},
		action:   PIIActionExclude,
	},
	// Mask patterns — partial redaction.
	{
		category: PIICategoryEmail,
		patterns: []string{"email", "e_mail"},
		action:   PIIActionMask,
	},
	{
		category: PIICategoryPhone,
		patterns: []string{"phone", "mobile", "tel"},
		action:   PIIActionMask,
	},
	{
		category: PIICategorySSN,
		patterns: []string{"ssn", "social_security"},
		action:   PIIActionMask,
	},
	{
		category: PIICategoryCreditCard,
		patterns: []string{"card_number", "cc_num"},
		action:   PIIActionMask,
	},
}

// PIIDetector identifies PII columns by matching column names against known
// sensitive patterns.
type PIIDetector struct {
	patterns []piiPattern
}

// NewPIIDetector creates a detector with the built-in pattern set.
func NewPIIDetector() *PIIDetector {
	return &PIIDetector{
		patterns: builtinPatterns,
	}
}

// DetectColumn checks whether a single column name matches any PII pattern.
// Returns a PIIMatch with PIIActionNone if no match is found.
func (d *PIIDetector) DetectColumn(columnName string) PIIMatch {
	lower := strings.ToLower(columnName)
	for _, p := range d.patterns {
		for _, pat := range p.patterns {
			if strings.Contains(lower, pat) {
				return PIIMatch{
					Column:   columnName,
					Category: p.category,
					Action:   p.action,
				}
			}
		}
	}
	return PIIMatch{Column: columnName, Action: PIIActionNone}
}

// DetectTable scans all columns in a TableDetail and returns matches for any
// PII columns found. Non-PII columns are omitted from the result.
func (d *PIIDetector) DetectTable(td *TableDetail) []PIIMatch {
	if td == nil {
		return nil
	}
	var matches []PIIMatch
	for _, col := range td.Columns {
		m := d.DetectColumn(col.Name)
		if m.Action != PIIActionNone {
			matches = append(matches, m)
		}
	}
	return matches
}

// ExcludedColumns returns the set of column names that should be excluded from
// query results for the given table.
func (d *PIIDetector) ExcludedColumns(td *TableDetail) map[string]bool {
	excluded := make(map[string]bool)
	for _, col := range td.Columns {
		m := d.DetectColumn(col.Name)
		if m.Action == PIIActionExclude {
			excluded[col.Name] = true
		}
	}
	return excluded
}

// MaskedColumns returns the set of column names that need value masking.
func (d *PIIDetector) MaskedColumns(td *TableDetail) map[string]PIICategory {
	masked := make(map[string]PIICategory)
	for _, col := range td.Columns {
		m := d.DetectColumn(col.Name)
		if m.Action == PIIActionMask {
			masked[col.Name] = m.Category
		}
	}
	return masked
}

// MaskValue applies category-appropriate masking to a string value.
func MaskValue(category PIICategory, value string) string {
	if value == "" {
		return ""
	}

	switch category {
	case PIICategoryEmail:
		return maskEmail(value)
	case PIICategoryPhone:
		return maskPhone(value)
	case PIICategorySSN:
		return maskSSN(value)
	case PIICategoryCreditCard:
		return maskCreditCard(value)
	default:
		return "***"
	}
}

// maskEmail masks an email address, preserving the first character and domain.
// Example: "kevin@example.com" -> "k***@example.com"
func maskEmail(email string) string {
	at := strings.LastIndex(email, "@")
	if at <= 0 {
		return "***"
	}
	return string(email[0]) + "***" + email[at:]
}

// maskPhone preserves only the last 4 digits.
// Example: "555-123-4567" -> "***-***-4567"
func maskPhone(phone string) string {
	digits := extractDigits(phone)
	if len(digits) < 4 {
		return "***"
	}
	last4 := digits[len(digits)-4:]
	return fmt.Sprintf("***-***-%s", last4)
}

// maskSSN preserves only the last 4 digits.
// Example: "123-45-6789" -> "***-**-6789"
func maskSSN(ssn string) string {
	digits := extractDigits(ssn)
	if len(digits) < 4 {
		return "***"
	}
	last4 := digits[len(digits)-4:]
	return fmt.Sprintf("***-**-%s", last4)
}

// maskCreditCard preserves only the last 4 digits.
// Example: "4111111111111111" -> "****-****-****-1111"
func maskCreditCard(cc string) string {
	digits := extractDigits(cc)
	if len(digits) < 4 {
		return "***"
	}
	last4 := digits[len(digits)-4:]
	return fmt.Sprintf("****-****-****-%s", last4)
}

// extractDigits returns only the digit characters from a string.
func extractDigits(s string) string {
	var b strings.Builder
	for _, r := range s {
		if r >= '0' && r <= '9' {
			b.WriteRune(r)
		}
	}
	return b.String()
}

// FilterColumns returns a new slice of ColumnInfo with excluded columns
// removed. This is useful for building SELECT column lists.
func (d *PIIDetector) FilterColumns(columns []ColumnInfo) []ColumnInfo {
	filtered := make([]ColumnInfo, 0, len(columns))
	for _, col := range columns {
		m := d.DetectColumn(col.Name)
		if m.Action != PIIActionExclude {
			filtered = append(filtered, col)
		}
	}
	return filtered
}

// MaskRow applies PII masking to a single result row in-place, given a map
// of column->category for columns that need masking.
func MaskRow(row map[string]any, maskedCols map[string]PIICategory) {
	for col, category := range maskedCols {
		if val, ok := row[col]; ok {
			if s, ok := val.(string); ok {
				row[col] = MaskValue(category, s)
			} else {
				row[col] = "***"
			}
		}
	}
}

// MaskRows applies PII masking to all rows in-place.
func MaskRows(rows []map[string]any, maskedCols map[string]PIICategory) {
	for _, row := range rows {
		MaskRow(row, maskedCols)
	}
}
