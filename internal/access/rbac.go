package access

import (
	"fmt"
	"strings"
)

// Verb represents a database operation type.
type Verb string

const (
	VerbSelect Verb = "SELECT"
	VerbInsert Verb = "INSERT"
	VerbUpdate Verb = "UPDATE"
	VerbDelete Verb = "DELETE"
)

// Role defines permissions for a set of tables.
type Role struct {
	Name            string        `yaml:"name"`
	MaxRowsPerQuery int           `yaml:"max_rows_per_query"`
	Tables          []TablePolicy `yaml:"tables"`
}

// TablePolicy defines access rules for a specific table or wildcard.
type TablePolicy struct {
	Name        string   `yaml:"name"`          // table name or "*" for wildcard
	Verbs       []string `yaml:"verbs"`         // SELECT, INSERT, UPDATE, DELETE
	DenyColumns []string `yaml:"deny_columns"`  // columns to hide entirely
	MaskColumns []string `yaml:"mask_columns"`  // columns to mask (PII)
	RowFilter   string   `yaml:"row_filter"`    // injected WHERE clause
}

// Engine evaluates access control policies.
type Engine struct {
	roles map[string]*Role
}

// NewEngine creates a new RBAC engine from a list of roles.
func NewEngine(roles []Role) *Engine {
	e := &Engine{
		roles: make(map[string]*Role, len(roles)),
	}
	for i := range roles {
		e.roles[roles[i].Name] = &roles[i]
	}
	return e
}

// CheckAccess verifies if a role has the given verb on a table.
func (e *Engine) CheckAccess(roleName, table string, verb Verb) error {
	role, ok := e.roles[roleName]
	if !ok {
		return fmt.Errorf("unknown role: %q", roleName)
	}

	verbStr := string(verb)

	// Check table-specific policies first, then wildcard
	for _, policy := range role.Tables {
		if policy.Name == table || policy.Name == "*" {
			for _, v := range policy.Verbs {
				if strings.EqualFold(v, verbStr) {
					return nil
				}
			}
			if policy.Name == table {
				return fmt.Errorf("role %q does not have %s access on table %q", roleName, verb, table)
			}
		}
	}

	return fmt.Errorf("role %q does not have %s access on table %q", roleName, verb, table)
}

// GetDeniedColumns returns columns that should be hidden for a role on a table.
func (e *Engine) GetDeniedColumns(roleName, table string) []string {
	role, ok := e.roles[roleName]
	if !ok {
		return nil
	}

	for _, policy := range role.Tables {
		if policy.Name == table || policy.Name == "*" {
			return policy.DenyColumns
		}
	}
	return nil
}

// GetMaskedColumns returns columns that should be masked for a role on a table.
func (e *Engine) GetMaskedColumns(roleName, table string) []string {
	role, ok := e.roles[roleName]
	if !ok {
		return nil
	}

	for _, policy := range role.Tables {
		if policy.Name == table || policy.Name == "*" {
			return policy.MaskColumns
		}
	}
	return nil
}

// GetRowFilter returns the row-level security filter for a role on a table.
func (e *Engine) GetRowFilter(roleName, table string) string {
	role, ok := e.roles[roleName]
	if !ok {
		return ""
	}

	for _, policy := range role.Tables {
		if policy.Name == table {
			return policy.RowFilter
		}
	}
	return ""
}

// GetMaxRows returns the max rows per query for a role.
func (e *Engine) GetMaxRows(roleName string) int {
	role, ok := e.roles[roleName]
	if !ok {
		return 0
	}
	return role.MaxRowsPerQuery
}
