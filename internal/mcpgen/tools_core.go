package mcpgen

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"

	"github.com/conduitdb/conduit/internal/connector"
	"github.com/modelcontextprotocol/go-sdk/mcp"
)

// buildCoreTools returns all Tier 1 core tool definitions.
func (g *Generator) buildCoreTools() []ToolDef {
	tools := []ToolDef{
		g.listTablesTool(),
		g.describeTableTool(),
		g.queryTool(),
		g.enableTableToolsTool(),
		g.refreshSchemaTool(),
		g.listProceduresTool(),
		g.callProcedureTool(),
	}

	if g.config.AllowRawSQL {
		tools = append(tools, g.rawSQLTool())
		if g.config.AllowWrites {
			tools = append(tools, g.executeSQLTool())
		}
	}

	return tools
}

// --- list_tables ---

func (g *Generator) listTablesTool() ToolDef {
	return ToolDef{
		Tool: &mcp.Tool{
			Name:        "list_tables",
			Description: "List all tables and views in the database with row counts. Use this first to discover available data.",
			InputSchema: toolInputSchema(map[string]any{}, nil),
			Annotations: &mcp.ToolAnnotations{
				ReadOnlyHint:    true,
				OpenWorldHint:   boolPtr(false),
				IdempotentHint:  true,
			},
		},
		Handler: func(ctx context.Context, req *mcp.CallToolRequest) (*mcp.CallToolResult, error) {
			summaries, err := g.conn.ListTables(ctx)
			if err != nil {
				result := &mcp.CallToolResult{}
				result.SetError(fmt.Errorf("failed to list tables: %w", err))
				return result, nil
			}

			data, _ := json.Marshal(summaries)
			return &mcp.CallToolResult{
				Content: []mcp.Content{&mcp.TextContent{Text: string(data)}},
			}, nil
		},
	}
}

// --- describe_table ---

func (g *Generator) describeTableTool() ToolDef {
	return ToolDef{
		Tool: &mcp.Tool{
			Name:        "describe_table",
			Description: "Get detailed schema for a table: columns, types, primary keys, foreign keys, and indexes.",
			InputSchema: toolInputSchema(map[string]any{
				"table": map[string]any{
					"type":        "string",
					"description": "Name of the table to describe",
				},
			}, []string{"table"}),
			Annotations: &mcp.ToolAnnotations{
				ReadOnlyHint:    true,
				OpenWorldHint:   boolPtr(false),
				IdempotentHint:  true,
			},
		},
		Handler: func(ctx context.Context, req *mcp.CallToolRequest) (*mcp.CallToolResult, error) {
			var args struct {
				Table string `json:"table"`
			}
			if err := json.Unmarshal(req.Params.Arguments, &args); err != nil {
				result := &mcp.CallToolResult{}
				result.SetError(fmt.Errorf("invalid arguments: %w", err))
				return result, nil
			}

			if args.Table == "" {
				result := &mcp.CallToolResult{}
				result.SetError(fmt.Errorf("table name is required"))
				return result, nil
			}

			detail, err := g.conn.DescribeTable(ctx, args.Table)
			if err != nil {
				result := &mcp.CallToolResult{}
				result.SetError(fmt.Errorf("failed to describe table %q: %w", args.Table, err))
				return result, nil
			}

			data, _ := json.Marshal(detail)
			return &mcp.CallToolResult{
				Content: []mcp.Content{&mcp.TextContent{Text: string(data)}},
			}, nil
		},
	}
}

// --- query ---

func (g *Generator) queryTool() ToolDef {
	return ToolDef{
		Tool: &mcp.Tool{
			Name:        "query",
			Description: "Query any table with optional filtering, ordering, and pagination. Returns rows as JSON.",
			InputSchema: toolInputSchema(map[string]any{
				"table": map[string]any{
					"type":        "string",
					"description": "Name of the table to query",
				},
				"columns": map[string]any{
					"type":        "array",
					"items":       map[string]any{"type": "string"},
					"description": "Columns to select (omit for all columns)",
				},
				"filter": map[string]any{
					"type":        "string",
					"description": "SQL WHERE clause condition (e.g., \"status = 'active' AND age > 30\")",
				},
				"order_by": map[string]any{
					"type":        "string",
					"description": "SQL ORDER BY clause (e.g., \"created_at DESC\")",
				},
				"limit": map[string]any{
					"type":        "integer",
					"description": "Maximum number of rows to return",
					"default":     100,
				},
				"offset": map[string]any{
					"type":        "integer",
					"description": "Number of rows to skip",
					"default":     0,
				},
			}, []string{"table"}),
			Annotations: &mcp.ToolAnnotations{
				ReadOnlyHint:    true,
				OpenWorldHint:   boolPtr(false),
				IdempotentHint:  true,
			},
		},
		Handler: func(ctx context.Context, req *mcp.CallToolRequest) (*mcp.CallToolResult, error) {
			var args struct {
				Table   string   `json:"table"`
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

			if args.Table == "" {
				result := &mcp.CallToolResult{}
				result.SetError(fmt.Errorf("table name is required"))
				return result, nil
			}

			// Apply max rows limit.
			limit := args.Limit
			if limit <= 0 {
				limit = 100
			}
			if limit > g.config.MaxRows {
				limit = g.config.MaxRows
			}

			rs, err := g.conn.Select(ctx, connector.SelectRequest{
				Table:   args.Table,
				Columns: args.Columns,
				Filter:  args.Filter,
				OrderBy: args.OrderBy,
				Limit:   limit,
				Offset:  args.Offset,
			})
			if err != nil {
				result := &mcp.CallToolResult{}
				result.SetError(fmt.Errorf("query failed: %w", err))
				return result, nil
			}

			data, _ := json.Marshal(rs)
			return &mcp.CallToolResult{
				Content: []mcp.Content{&mcp.TextContent{Text: string(data)}},
			}, nil
		},
	}
}

// --- enable_table_tools ---

func (g *Generator) enableTableToolsTool() ToolDef {
	return ToolDef{
		Tool: &mcp.Tool{
			Name:        "enable_table_tools",
			Description: fmt.Sprintf("Load typed, per-table CRUD tools for the specified tables. Max %d tables at once. After calling this, you'll get query_{table}, get_{table}_by_id, and (if writes are enabled) insert/update/delete tools.", MaxDynamicTables),
			InputSchema: toolInputSchema(map[string]any{
				"tables": map[string]any{
					"type":        "array",
					"items":       map[string]any{"type": "string"},
					"description": "List of table names to enable tools for",
				},
			}, []string{"tables"}),
			Annotations: &mcp.ToolAnnotations{
				ReadOnlyHint:    true,
				OpenWorldHint:   boolPtr(false),
			},
		},
		Handler: func(ctx context.Context, req *mcp.CallToolRequest) (*mcp.CallToolResult, error) {
			var args struct {
				Tables []string `json:"tables"`
			}
			if err := json.Unmarshal(req.Params.Arguments, &args); err != nil {
				result := &mcp.CallToolResult{}
				result.SetError(fmt.Errorf("invalid arguments: %w", err))
				return result, nil
			}

			if len(args.Tables) == 0 {
				result := &mcp.CallToolResult{}
				result.SetError(fmt.Errorf("at least one table name is required"))
				return result, nil
			}

			// Generate Tier 2 tools for the requested tables.
			toolDefs, err := g.DynamicToolsForTables(ctx, args.Tables)
			if err != nil {
				result := &mcp.CallToolResult{}
				result.SetError(err)
				return result, nil
			}

			// Register the tools on the MCP server via the callback.
			// The MCP SDK automatically sends tools/list_changed notifications
			// when AddTool is called.
			if g.OnRegisterTool != nil {
				for _, td := range toolDefs {
					g.OnRegisterTool(td.Tool, td.Handler)
				}
			}

			toolNames := make([]string, len(toolDefs))
			for i, td := range toolDefs {
				toolNames[i] = td.Tool.Name
			}

			response := map[string]any{
				"enabled_tables": args.Tables,
				"tools_added":   toolNames,
				"message":       fmt.Sprintf("Enabled %d tools for %d tables. Use tools/list to see the new tools.", len(toolNames), len(args.Tables)),
			}
			data, _ := json.Marshal(response)
			return &mcp.CallToolResult{
				Content: []mcp.Content{&mcp.TextContent{Text: string(data)}},
			}, nil
		},
	}
}

// --- refresh_schema ---

func (g *Generator) refreshSchemaTool() ToolDef {
	return ToolDef{
		Tool: &mcp.Tool{
			Name:        "refresh_schema",
			Description: "Force a refresh of the cached database schema. Use this if the schema has changed.",
			InputSchema: toolInputSchema(map[string]any{}, nil),
			Annotations: &mcp.ToolAnnotations{
				ReadOnlyHint:    true,
				OpenWorldHint:   boolPtr(false),
			},
		},
		Handler: func(ctx context.Context, req *mcp.CallToolRequest) (*mcp.CallToolResult, error) {
			g.InvalidateSchema()

			// Re-fetch summaries to verify the refresh worked.
			summaries, err := g.getTableSummaries(ctx)
			if err != nil {
				result := &mcp.CallToolResult{}
				result.SetError(fmt.Errorf("schema refresh failed: %w", err))
				return result, nil
			}

			response := map[string]any{
				"message":     "Schema cache refreshed successfully",
				"table_count": len(summaries),
			}
			data, _ := json.Marshal(response)
			return &mcp.CallToolResult{
				Content: []mcp.Content{&mcp.TextContent{Text: string(data)}},
			}, nil
		},
	}
}

// --- list_procedures ---

func (g *Generator) listProceduresTool() ToolDef {
	return ToolDef{
		Tool: &mcp.Tool{
			Name:        "list_procedures",
			Description: "List stored procedures and functions in the database.",
			InputSchema: toolInputSchema(map[string]any{}, nil),
			Annotations: &mcp.ToolAnnotations{
				ReadOnlyHint:    true,
				OpenWorldHint:   boolPtr(false),
				IdempotentHint:  true,
			},
		},
		Handler: func(ctx context.Context, req *mcp.CallToolRequest) (*mcp.CallToolResult, error) {
			procs, err := g.conn.ListProcedures(ctx)
			if err != nil {
				result := &mcp.CallToolResult{}
				result.SetError(fmt.Errorf("failed to list procedures: %w", err))
				return result, nil
			}

			data, _ := json.Marshal(procs)
			return &mcp.CallToolResult{
				Content: []mcp.Content{&mcp.TextContent{Text: string(data)}},
			}, nil
		},
	}
}

// --- call_procedure ---

func (g *Generator) callProcedureTool() ToolDef {
	return ToolDef{
		Tool: &mcp.Tool{
			Name:        "call_procedure",
			Description: "Call a stored procedure or function with the given parameters.",
			InputSchema: toolInputSchema(map[string]any{
				"name": map[string]any{
					"type":        "string",
					"description": "Name of the stored procedure or function to call",
				},
				"params": map[string]any{
					"type":        "object",
					"description": "Key-value pairs of parameter names and values",
				},
			}, []string{"name"}),
			Annotations: &mcp.ToolAnnotations{
				ReadOnlyHint:   false,
				OpenWorldHint:  boolPtr(false),
			},
		},
		Handler: func(ctx context.Context, req *mcp.CallToolRequest) (*mcp.CallToolResult, error) {
			var args struct {
				Name   string         `json:"name"`
				Params map[string]any `json:"params"`
			}
			if err := json.Unmarshal(req.Params.Arguments, &args); err != nil {
				result := &mcp.CallToolResult{}
				result.SetError(fmt.Errorf("invalid arguments: %w", err))
				return result, nil
			}

			if args.Name == "" {
				result := &mcp.CallToolResult{}
				result.SetError(fmt.Errorf("procedure name is required"))
				return result, nil
			}

			rs, err := g.conn.CallProcedure(ctx, connector.ProcedureCallRequest{
				Name:   args.Name,
				Params: args.Params,
			})
			if err != nil {
				result := &mcp.CallToolResult{}
				result.SetError(fmt.Errorf("procedure call failed: %w", err))
				return result, nil
			}

			data, _ := json.Marshal(rs)
			return &mcp.CallToolResult{
				Content: []mcp.Content{&mcp.TextContent{Text: string(data)}},
			}, nil
		},
	}
}

// --- raw_sql ---

func (g *Generator) rawSQLTool() ToolDef {
	return ToolDef{
		Tool: &mcp.Tool{
			Name:        "raw_sql",
			Description: "Execute a raw SQL SELECT query. Only available when --allow-raw-sql is enabled. For read-only queries only.",
			InputSchema: toolInputSchema(map[string]any{
				"sql": map[string]any{
					"type":        "string",
					"description": "SQL SELECT query to execute",
				},
			}, []string{"sql"}),
			Annotations: &mcp.ToolAnnotations{
				ReadOnlyHint:    true,
				OpenWorldHint:   boolPtr(false),
			},
		},
		Handler: func(ctx context.Context, req *mcp.CallToolRequest) (*mcp.CallToolResult, error) {
			var args struct {
				SQL string `json:"sql"`
			}
			if err := json.Unmarshal(req.Params.Arguments, &args); err != nil {
				result := &mcp.CallToolResult{}
				result.SetError(fmt.Errorf("invalid arguments: %w", err))
				return result, nil
			}

			if args.SQL == "" {
				result := &mcp.CallToolResult{}
				result.SetError(fmt.Errorf("sql query is required"))
				return result, nil
			}

			// Basic validation: only allow SELECT-like statements.
			upper := strings.TrimSpace(strings.ToUpper(args.SQL))
			if !strings.HasPrefix(upper, "SELECT") &&
				!strings.HasPrefix(upper, "WITH") &&
				!strings.HasPrefix(upper, "EXPLAIN") &&
				!strings.HasPrefix(upper, "SHOW") {
				result := &mcp.CallToolResult{}
				result.SetError(fmt.Errorf("raw_sql only allows SELECT, WITH, EXPLAIN, and SHOW statements"))
				return result, nil
			}

			// Use Select with the raw SQL passed as the table (the connector
			// will need to support raw SQL mode). For now we use a special
			// convention: if Table starts with "__raw__:", the connector treats
			// it as raw SQL.
			rs, err := g.conn.Select(ctx, connector.SelectRequest{
				Table: "__raw__:" + args.SQL,
				Limit: g.config.MaxRows,
			})
			if err != nil {
				result := &mcp.CallToolResult{}
				result.SetError(fmt.Errorf("raw SQL query failed: %w", err))
				return result, nil
			}

			data, _ := json.Marshal(rs)
			return &mcp.CallToolResult{
				Content: []mcp.Content{&mcp.TextContent{Text: string(data)}},
			}, nil
		},
	}
}

// --- execute_sql ---

func (g *Generator) executeSQLTool() ToolDef {
	return ToolDef{
		Tool: &mcp.Tool{
			Name:        "execute_sql",
			Description: "Execute arbitrary SQL (INSERT, UPDATE, DELETE, DDL). Only available when both --allow-raw-sql and --allow-writes are enabled. CAUTION: This can modify data.",
			InputSchema: toolInputSchema(map[string]any{
				"sql": map[string]any{
					"type":        "string",
					"description": "SQL statement to execute",
				},
			}, []string{"sql"}),
			Annotations: &mcp.ToolAnnotations{
				ReadOnlyHint:    false,
				DestructiveHint: boolPtr(true),
				OpenWorldHint:   boolPtr(false),
			},
		},
		Handler: func(ctx context.Context, req *mcp.CallToolRequest) (*mcp.CallToolResult, error) {
			var args struct {
				SQL string `json:"sql"`
			}
			if err := json.Unmarshal(req.Params.Arguments, &args); err != nil {
				result := &mcp.CallToolResult{}
				result.SetError(fmt.Errorf("invalid arguments: %w", err))
				return result, nil
			}

			if args.SQL == "" {
				result := &mcp.CallToolResult{}
				result.SetError(fmt.Errorf("sql statement is required"))
				return result, nil
			}

			// Use the same raw SQL convention with a mutation flag.
			rs, err := g.conn.Select(ctx, connector.SelectRequest{
				Table: "__exec__:" + args.SQL,
				Limit: g.config.MaxRows,
			})
			if err != nil {
				result := &mcp.CallToolResult{}
				result.SetError(fmt.Errorf("SQL execution failed: %w", err))
				return result, nil
			}

			data, _ := json.Marshal(rs)
			return &mcp.CallToolResult{
				Content: []mcp.Content{&mcp.TextContent{Text: string(data)}},
			}, nil
		},
	}
}
