package schema

// TableSummary is returned by list_tables — minimal token cost.
type TableSummary struct {
	Name     string `json:"name"`
	RowCount int64  `json:"rows"`
	Type     string `json:"type,omitempty"` // "table" | "view" | "materialized_view"
}

// TableDetail is returned by describe_table.
type TableDetail struct {
	Name        string       `json:"name"`
	Schema      string       `json:"schema,omitempty"`
	Columns     []ColumnInfo `json:"columns"`
	PrimaryKey  []string     `json:"pk,omitempty"`
	ForeignKeys []FKInfo     `json:"fks,omitempty"`
	Indexes     []string     `json:"indexes,omitempty"`
	RowCount    int64        `json:"rows"`
	Description string       `json:"description,omitempty"`
}

// ColumnInfo uses simplified types — LLMs don't need native DB type details.
type ColumnInfo struct {
	Name     string `json:"name"`
	Type     string `json:"type"`               // Simplified: string, integer, decimal, boolean, datetime, binary, json
	Nullable bool   `json:"nullable,omitempty"`
	PK       bool   `json:"pk,omitempty"`
	FK       string `json:"fk,omitempty"`        // "orders.customer_id" format
	Default  string `json:"default,omitempty"`
}

// FKInfo represents a foreign key relationship.
type FKInfo struct {
	Column    string `json:"col"`
	RefTable  string `json:"ref_table"`
	RefColumn string `json:"ref_col"`
}

// ProcedureSummary is returned by list_procedures.
type ProcedureSummary struct {
	Name   string `json:"name"`
	Type   string `json:"type"` // "procedure" | "function"
	Params int    `json:"params"`
}

// ProcedureDetail is returned by describe_procedure.
type ProcedureDetail struct {
	Name       string      `json:"name"`
	Type       string      `json:"type"`
	Parameters []ParamInfo `json:"parameters"`
	Returns    string      `json:"returns,omitempty"`
}

// ParamInfo describes a stored procedure parameter.
type ParamInfo struct {
	Name      string `json:"name"`
	Type      string `json:"type"`
	Direction string `json:"direction"` // "in", "out", "inout"
	Default   string `json:"default,omitempty"`
}
