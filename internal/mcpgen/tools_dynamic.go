package mcpgen

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"

	"github.com/conduitdb/conduit/internal/connector"
	"github.com/conduitdb/conduit/internal/schema"
	"github.com/modelcontextprotocol/go-sdk/mcp"
)

// buildDynamicTools generates Tier 2 per-table tools for a given table detail.
func (g *Generator) buildDynamicTools(detail *schema.TableDetail) []ToolDef {
	tools := []ToolDef{
		g.queryTableTool(detail),
	}

	// Only generate get_by_id if the table has a primary key.
	if len(detail.PrimaryKey) > 0 {
		tools = append(tools, g.getByIDTool(detail))
	}

	if g.config.AllowWrites {
		tools = append(tools,
			g.insertTableTool(detail),
			g.updateTableTool(detail),
			g.deleteTableTool(detail),
		)
	}

	return tools
}

// --- query_{table} ---

func (g *Generator) queryTableTool(detail *schema.TableDetail) ToolDef {
	// Build column enum for typed query.
	columnNames := make([]any, len(detail.Columns))
	for i, col := range detail.Columns {
		columnNames[i] = col.Name
	}

	// Build column descriptions for documentation.
	var colDescs []string
	for _, col := range detail.Columns {
		desc := fmt.Sprintf("%s (%s)", col.Name, col.Type)
		if col.Nullable {
			desc += " nullable"
		}
		if col.PK {
			desc += " PK"
		}
		if col.FK != "" {
			desc += " FK->" + col.FK
		}
		colDescs = append(colDescs, desc)
	}

	return ToolDef{
		Tool: &mcp.Tool{
			Name:        "query_" + detail.Name,
			Description: fmt.Sprintf("Query the %s table. Columns: %s", detail.Name, strings.Join(colDescs, ", ")),
			InputSchema: toolInputSchema(map[string]any{
				"columns": map[string]any{
					"type":        "array",
					"items":       map[string]any{"type": "string", "enum": columnNames},
					"description": "Columns to select (omit for all)",
				},
				"filter": map[string]any{
					"type":        "string",
					"description": "SQL WHERE clause condition",
				},
				"order_by": map[string]any{
					"type":        "string",
					"description": "SQL ORDER BY clause",
				},
				"limit": map[string]any{
					"type":    "integer",
					"default": 100,
				},
				"offset": map[string]any{
					"type":    "integer",
					"default": 0,
				},
			}, nil),
			Annotations: &mcp.ToolAnnotations{
				ReadOnlyHint:   true,
				OpenWorldHint:  boolPtr(false),
				IdempotentHint: true,
			},
		},
		Handler: g.makeQueryHandler(detail.Name),
	}
}

// makeQueryHandler creates a query handler for a specific table.
func (g *Generator) makeQueryHandler(tableName string) mcp.ToolHandler {
	return func(ctx context.Context, req *mcp.CallToolRequest) (*mcp.CallToolResult, error) {
		var args struct {
			Columns []string `json:"columns"`
			Filter  string   `json:"filter"`
			OrderBy string   `json:"order_by"`
			Limit   int      `json:"limit"`
			Offset  int      `json:"offset"`
		}
		if err := json.Unmarshal(req.Params.Arguments, &args); err != nil {
			result := &mcp.CallToolResult{}
			result.SetError(fmt.Errorf("invalid arguments: %w", err))
			return result, nil
		}

		limit := args.Limit
		if limit <= 0 {
			limit = 100
		}
		if limit > g.config.MaxRows {
			limit = g.config.MaxRows
		}

		rs, err := g.conn.Select(ctx, connector.SelectRequest{
			Table:   tableName,
			Columns: args.Columns,
			Filter:  args.Filter,
			OrderBy: args.OrderBy,
			Limit:   limit,
			Offset:  args.Offset,
		})
		if err != nil {
			result := &mcp.CallToolResult{}
			result.SetError(fmt.Errorf("query %s failed: %w", tableName, err))
			return result, nil
		}

		data, _ := json.Marshal(rs)
		return &mcp.CallToolResult{
			Content: []mcp.Content{&mcp.TextContent{Text: string(data)}},
		}, nil
	}
}

// --- get_{table}_by_id ---

func (g *Generator) getByIDTool(detail *schema.TableDetail) ToolDef {
	// Build PK properties.
	properties := make(map[string]any)
	var required []string
	for _, pkCol := range detail.PrimaryKey {
		colType := "string"
		for _, col := range detail.Columns {
			if col.Name == pkCol {
				colType = schemaTypeToJSON(col.Type)
				break
			}
		}
		properties[pkCol] = map[string]any{
			"type":        colType,
			"description": fmt.Sprintf("Primary key value for %s", pkCol),
		}
		required = append(required, pkCol)
	}

	pkDesc := strings.Join(detail.PrimaryKey, ", ")

	return ToolDef{
		Tool: &mcp.Tool{
			Name:        "get_" + detail.Name + "_by_id",
			Description: fmt.Sprintf("Get a single %s record by primary key (%s).", detail.Name, pkDesc),
			InputSchema: toolInputSchema(properties, required),
			Annotations: &mcp.ToolAnnotations{
				ReadOnlyHint:   true,
				OpenWorldHint:  boolPtr(false),
				IdempotentHint: true,
			},
		},
		Handler: g.makeGetByIDHandler(detail),
	}
}

func (g *Generator) makeGetByIDHandler(detail *schema.TableDetail) mcp.ToolHandler {
	tableName := detail.Name
	pkCols := detail.PrimaryKey

	return func(ctx context.Context, req *mcp.CallToolRequest) (*mcp.CallToolResult, error) {
		var args map[string]any
		if err := json.Unmarshal(req.Params.Arguments, &args); err != nil {
			result := &mcp.CallToolResult{}
			result.SetError(fmt.Errorf("invalid arguments: %w", err))
			return result, nil
		}

		// Build a filter from PK columns.
		var conditions []string
		for _, pk := range pkCols {
			val, ok := args[pk]
			if !ok {
				result := &mcp.CallToolResult{}
				result.SetError(fmt.Errorf("missing primary key value for %q", pk))
				return result, nil
			}
			// Format the value for the filter string.
			switch v := val.(type) {
			case string:
				conditions = append(conditions, fmt.Sprintf("%s = '%s'", pk, v))
			default:
				conditions = append(conditions, fmt.Sprintf("%s = %v", pk, v))
			}
		}

		filter := strings.Join(conditions, " AND ")

		rs, err := g.conn.Select(ctx, connector.SelectRequest{
			Table:  tableName,
			Filter: filter,
			Limit:  1,
		})
		if err != nil {
			result := &mcp.CallToolResult{}
			result.SetError(fmt.Errorf("get %s by ID failed: %w", tableName, err))
			return result, nil
		}

		if len(rs.Rows) == 0 {
			result := &mcp.CallToolResult{}
			result.SetError(fmt.Errorf("no %s record found with %s", tableName, filter))
			return result, nil
		}

		data, _ := json.Marshal(rs.Rows[0])
		return &mcp.CallToolResult{
			Content: []mcp.Content{&mcp.TextContent{Text: string(data)}},
		}, nil
	}
}

// --- insert_{table} ---

func (g *Generator) insertTableTool(detail *schema.TableDetail) ToolDef {
	// Build properties from columns (excluding auto-generated PKs).
	properties := make(map[string]any)
	for _, col := range detail.Columns {
		properties[col.Name] = map[string]any{
			"description": columnDescription(col),
		}
	}

	return ToolDef{
		Tool: &mcp.Tool{
			Name:        "insert_" + detail.Name,
			Description: fmt.Sprintf("Insert one or more rows into the %s table.", detail.Name),
			InputSchema: toolInputSchema(map[string]any{
				"rows": map[string]any{
					"type":        "array",
					"description": "Array of row objects to insert",
					"items": map[string]any{
						"type":       "object",
						"properties": properties,
					},
				},
			}, []string{"rows"}),
			Annotations: &mcp.ToolAnnotations{
				ReadOnlyHint:   false,
				OpenWorldHint:  boolPtr(false),
				IdempotentHint: false,
			},
		},
		Handler: g.makeInsertHandler(detail.Name),
	}
}

func (g *Generator) makeInsertHandler(tableName string) mcp.ToolHandler {
	return func(ctx context.Context, req *mcp.CallToolRequest) (*mcp.CallToolResult, error) {
		var args struct {
			Rows []map[string]any `json:"rows"`
		}
		if err := json.Unmarshal(req.Params.Arguments, &args); err != nil {
			result := &mcp.CallToolResult{}
			result.SetError(fmt.Errorf("invalid arguments: %w", err))
			return result, nil
		}

		if len(args.Rows) == 0 {
			result := &mcp.CallToolResult{}
			result.SetError(fmt.Errorf("at least one row is required"))
			return result, nil
		}

		mr, err := g.conn.Insert(ctx, connector.InsertRequest{
			Table: tableName,
			Rows:  args.Rows,
		})
		if err != nil {
			result := &mcp.CallToolResult{}
			result.SetError(fmt.Errorf("insert into %s failed: %w", tableName, err))
			return result, nil
		}

		data, _ := json.Marshal(mr)
		return &mcp.CallToolResult{
			Content: []mcp.Content{&mcp.TextContent{Text: string(data)}},
		}, nil
	}
}

// --- update_{table} ---

func (g *Generator) updateTableTool(detail *schema.TableDetail) ToolDef {
	// Build set properties from columns.
	properties := make(map[string]any)
	for _, col := range detail.Columns {
		properties[col.Name] = map[string]any{
			"description": columnDescription(col),
		}
	}

	return ToolDef{
		Tool: &mcp.Tool{
			Name:        "update_" + detail.Name,
			Description: fmt.Sprintf("Update rows in the %s table matching a filter condition.", detail.Name),
			InputSchema: toolInputSchema(map[string]any{
				"filter": map[string]any{
					"type":        "string",
					"description": "SQL WHERE clause to identify rows to update (REQUIRED to prevent accidental full-table updates)",
				},
				"set": map[string]any{
					"type":        "object",
					"description": "Column-value pairs to update",
					"properties":  properties,
				},
			}, []string{"filter", "set"}),
			Annotations: &mcp.ToolAnnotations{
				ReadOnlyHint:    false,
				DestructiveHint: boolPtr(true),
				OpenWorldHint:   boolPtr(false),
				IdempotentHint:  true,
			},
		},
		Handler: g.makeUpdateHandler(detail.Name),
	}
}

func (g *Generator) makeUpdateHandler(tableName string) mcp.ToolHandler {
	return func(ctx context.Context, req *mcp.CallToolRequest) (*mcp.CallToolResult, error) {
		var args struct {
			Filter string         `json:"filter"`
			Set    map[string]any `json:"set"`
		}
		if err := json.Unmarshal(req.Params.Arguments, &args); err != nil {
			result := &mcp.CallToolResult{}
			result.SetError(fmt.Errorf("invalid arguments: %w", err))
			return result, nil
		}

		if args.Filter == "" {
			result := &mcp.CallToolResult{}
			result.SetError(fmt.Errorf("filter is required to prevent accidental full-table updates"))
			return result, nil
		}

		if len(args.Set) == 0 {
			result := &mcp.CallToolResult{}
			result.SetError(fmt.Errorf("at least one column to update is required"))
			return result, nil
		}

		mr, err := g.conn.Update(ctx, connector.UpdateRequest{
			Table:  tableName,
			Filter: args.Filter,
			Set:    args.Set,
		})
		if err != nil {
			result := &mcp.CallToolResult{}
			result.SetError(fmt.Errorf("update %s failed: %w", tableName, err))
			return result, nil
		}

		data, _ := json.Marshal(mr)
		return &mcp.CallToolResult{
			Content: []mcp.Content{&mcp.TextContent{Text: string(data)}},
		}, nil
	}
}

// --- delete_{table} ---

func (g *Generator) deleteTableTool(detail *schema.TableDetail) ToolDef {
	return ToolDef{
		Tool: &mcp.Tool{
			Name:        "delete_" + detail.Name,
			Description: fmt.Sprintf("Delete rows from the %s table matching a filter condition.", detail.Name),
			InputSchema: toolInputSchema(map[string]any{
				"filter": map[string]any{
					"type":        "string",
					"description": "SQL WHERE clause to identify rows to delete (REQUIRED to prevent accidental full-table deletes)",
				},
			}, []string{"filter"}),
			Annotations: &mcp.ToolAnnotations{
				ReadOnlyHint:    false,
				DestructiveHint: boolPtr(true),
				OpenWorldHint:   boolPtr(false),
			},
		},
		Handler: g.makeDeleteHandler(detail.Name),
	}
}

func (g *Generator) makeDeleteHandler(tableName string) mcp.ToolHandler {
	return func(ctx context.Context, req *mcp.CallToolRequest) (*mcp.CallToolResult, error) {
		var args struct {
			Filter string `json:"filter"`
		}
		if err := json.Unmarshal(req.Params.Arguments, &args); err != nil {
			result := &mcp.CallToolResult{}
			result.SetError(fmt.Errorf("invalid arguments: %w", err))
			return result, nil
		}

		if args.Filter == "" {
			result := &mcp.CallToolResult{}
			result.SetError(fmt.Errorf("filter is required to prevent accidental full-table deletes"))
			return result, nil
		}

		mr, err := g.conn.Delete(ctx, connector.DeleteRequest{
			Table:  tableName,
			Filter: args.Filter,
		})
		if err != nil {
			result := &mcp.CallToolResult{}
			result.SetError(fmt.Errorf("delete from %s failed: %w", tableName, err))
			return result, nil
		}

		data, _ := json.Marshal(mr)
		return &mcp.CallToolResult{
			Content: []mcp.Content{&mcp.TextContent{Text: string(data)}},
		}, nil
	}
}

// --- helpers ---

// schemaTypeToJSON maps Conduit simplified types to JSON Schema types.
func schemaTypeToJSON(t string) string {
	switch t {
	case "integer":
		return "integer"
	case "decimal":
		return "number"
	case "boolean":
		return "boolean"
	default:
		return "string"
	}
}

// columnDescription generates a human-readable description for a column.
func columnDescription(col schema.ColumnInfo) string {
	parts := []string{col.Type}
	if col.Nullable {
		parts = append(parts, "nullable")
	}
	if col.PK {
		parts = append(parts, "primary key")
	}
	if col.FK != "" {
		parts = append(parts, "references "+col.FK)
	}
	if col.Default != "" {
		parts = append(parts, "default: "+col.Default)
	}
	return strings.Join(parts, ", ")
}
