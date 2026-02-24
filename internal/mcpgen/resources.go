package mcpgen

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/modelcontextprotocol/go-sdk/mcp"
)

// buildResources returns static resource definitions.
func (g *Generator) buildResources() []ResourceDef {
	return []ResourceDef{
		g.schemaTablesResource(),
		g.statsOverviewResource(),
	}
}

// buildResourceTemplates returns resource template definitions.
func (g *Generator) buildResourceTemplates() []ResourceTemplateDef {
	return []ResourceTemplateDef{
		g.schemaTableTemplate(),
	}
}

// --- schema://tables ---

func (g *Generator) schemaTablesResource() ResourceDef {
	return ResourceDef{
		Resource: &mcp.Resource{
			URI:         "schema://tables",
			Name:        "Database Schema",
			Description: "Complete database schema listing all tables, their columns, types, primary keys, foreign keys, and indexes as JSON.",
			MIMEType:    "application/json",
		},
		Handler: func(ctx context.Context, req *mcp.ReadResourceRequest) (*mcp.ReadResourceResult, error) {
			summaries, err := g.conn.ListTables(ctx)
			if err != nil {
				return nil, fmt.Errorf("failed to list tables: %w", err)
			}

			// Build full schema by describing each table.
			type fullSchema struct {
				Tables []any `json:"tables"`
			}

			var tables []any
			for _, s := range summaries {
				detail, err := g.conn.DescribeTable(ctx, s.Name)
				if err != nil {
					// Include the summary even if detail fails.
					tables = append(tables, s)
					continue
				}
				tables = append(tables, detail)
			}

			data, _ := json.MarshalIndent(fullSchema{Tables: tables}, "", "  ")

			return &mcp.ReadResourceResult{
				Contents: []*mcp.ResourceContents{
					{
						URI:      req.Params.URI,
						MIMEType: "application/json",
						Text:     string(data),
					},
				},
			}, nil
		},
	}
}

// --- schema://{table} ---

func (g *Generator) schemaTableTemplate() ResourceTemplateDef {
	return ResourceTemplateDef{
		Template: &mcp.ResourceTemplate{
			URITemplate: "schema://{table}",
			Name:        "Table Schema",
			Description: "Detailed schema for a specific table including columns, types, primary keys, foreign keys, and indexes.",
			MIMEType:    "application/json",
		},
		Handler: func(ctx context.Context, req *mcp.ReadResourceRequest) (*mcp.ReadResourceResult, error) {
			// Extract table name from URI: "schema://tablename"
			uri := req.Params.URI
			tableName := ""
			if len(uri) > len("schema://") {
				tableName = uri[len("schema://"):]
			}

			if tableName == "" || tableName == "tables" {
				// Redirect to the full schema resource.
				return nil, mcp.ResourceNotFoundError(uri)
			}

			detail, err := g.conn.DescribeTable(ctx, tableName)
			if err != nil {
				return nil, fmt.Errorf("failed to describe table %q: %w", tableName, err)
			}

			data, _ := json.MarshalIndent(detail, "", "  ")

			return &mcp.ReadResourceResult{
				Contents: []*mcp.ResourceContents{
					{
						URI:      req.Params.URI,
						MIMEType: "application/json",
						Text:     string(data),
					},
				},
			}, nil
		},
	}
}

// --- stats://overview ---

func (g *Generator) statsOverviewResource() ResourceDef {
	return ResourceDef{
		Resource: &mcp.Resource{
			URI:         "stats://overview",
			Name:        "Database Statistics",
			Description: "Overview statistics including table counts, total rows, and database driver information.",
			MIMEType:    "application/json",
		},
		Handler: func(ctx context.Context, req *mcp.ReadResourceRequest) (*mcp.ReadResourceResult, error) {
			summaries, err := g.conn.ListTables(ctx)
			if err != nil {
				return nil, fmt.Errorf("failed to list tables: %w", err)
			}

			var totalRows int64
			tableCounts := map[string]int{
				"table":             0,
				"view":              0,
				"materialized_view": 0,
			}

			for _, s := range summaries {
				totalRows += s.RowCount
				t := s.Type
				if t == "" {
					t = "table"
				}
				tableCounts[t]++
			}

			overview := map[string]any{
				"driver":       g.conn.DriverName(),
				"total_tables": len(summaries),
				"total_rows":   totalRows,
				"by_type":      tableCounts,
				"enabled_dynamic_tables": g.EnabledTables(),
			}

			data, _ := json.MarshalIndent(overview, "", "  ")

			return &mcp.ReadResourceResult{
				Contents: []*mcp.ResourceContents{
					{
						URI:      req.Params.URI,
						MIMEType: "application/json",
						Text:     string(data),
					},
				},
			}, nil
		},
	}
}
