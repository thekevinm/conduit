package mcpgen

import (
	"context"
	"fmt"

	"github.com/modelcontextprotocol/go-sdk/mcp"
)

// buildPrompts returns built-in prompt definitions for database exploration.
func (g *Generator) buildPrompts() []PromptDef {
	return []PromptDef{
		g.exploreDatabasePrompt(),
		g.analyzeTablePrompt(),
	}
}

// --- explore_database ---

func (g *Generator) exploreDatabasePrompt() PromptDef {
	return PromptDef{
		Prompt: &mcp.Prompt{
			Name:        "explore_database",
			Description: "Guide through exploring the database schema and data. Starts with listing tables and suggests next steps.",
			Arguments: []*mcp.PromptArgument{
				{
					Name:        "focus",
					Description: "Optional area of focus (e.g., 'user data', 'orders', 'analytics')",
					Required:    false,
				},
			},
		},
		Handler: func(ctx context.Context, req *mcp.GetPromptRequest) (*mcp.GetPromptResult, error) {
			focus := req.Params.Arguments["focus"]

			systemMessage := `You are a database exploration assistant connected to a live database via Conduit MCP server.

Your goal is to help the user understand and work with the database efficiently.

Follow this exploration workflow:
1. Start with list_tables to see all available tables
2. Use describe_table on interesting tables to understand their schema
3. Use query to sample data from key tables
4. Look for relationships via foreign keys
5. Suggest enabling table tools (enable_table_tools) for tables the user wants to work with frequently

Key tips:
- Always check the schema before querying
- Use limit to avoid retrieving too many rows
- Pay attention to foreign key relationships to understand data connections
- Look at row counts to understand data volume`

			if focus != "" {
				systemMessage += fmt.Sprintf("\n\nThe user is particularly interested in: %s. Prioritize tables and data related to this area.", focus)
			}

			userMessage := "I'd like to explore this database. Start by listing all available tables and give me an overview of the data."
			if focus != "" {
				userMessage = fmt.Sprintf("I'd like to explore the %s-related data in this database. Start by listing all tables and identify which ones are relevant.", focus)
			}

			return &mcp.GetPromptResult{
				Description: "Database exploration guide",
				Messages: []*mcp.PromptMessage{
					{
						Role:    "assistant",
						Content: &mcp.TextContent{Text: systemMessage},
					},
					{
						Role:    "user",
						Content: &mcp.TextContent{Text: userMessage},
					},
				},
			}, nil
		},
	}
}

// --- analyze_table ---

func (g *Generator) analyzeTablePrompt() PromptDef {
	return PromptDef{
		Prompt: &mcp.Prompt{
			Name:        "analyze_table",
			Description: "Perform a comprehensive analysis of a specific table: schema, data distribution, relationships, and anomalies.",
			Arguments: []*mcp.PromptArgument{
				{
					Name:        "table",
					Description: "Name of the table to analyze",
					Required:    true,
				},
			},
		},
		Handler: func(ctx context.Context, req *mcp.GetPromptRequest) (*mcp.GetPromptResult, error) {
			tableName := req.Params.Arguments["table"]
			if tableName == "" {
				return nil, fmt.Errorf("table name is required")
			}

			systemMessage := fmt.Sprintf(`You are a database analysis assistant. Perform a comprehensive analysis of the "%s" table.

Follow this analysis workflow:

1. **Schema Analysis**
   - Use describe_table to get the full schema
   - Document column types, nullability, and constraints
   - Identify primary keys and foreign key relationships

2. **Data Profile**
   - Use query to get a sample of rows (limit 10)
   - Check row count from list_tables
   - Look for null patterns in nullable columns

3. **Relationship Mapping**
   - Follow foreign keys to related tables
   - Describe each relationship (one-to-many, etc.)
   - Use describe_table on related tables

4. **Data Quality Check**
   - Look for unexpected nulls in non-nullable columns
   - Check for reasonable value ranges
   - Identify any obvious anomalies in the sample data

5. **Summary**
   - Provide a clear description of what this table represents
   - List key relationships
   - Suggest useful queries for working with this table
   - Recommend enabling table tools if the user plans to work with it extensively`, tableName)

			userMessage := fmt.Sprintf("Please perform a comprehensive analysis of the %s table.", tableName)

			return &mcp.GetPromptResult{
				Description: fmt.Sprintf("Comprehensive analysis of the %s table", tableName),
				Messages: []*mcp.PromptMessage{
					{
						Role:    "assistant",
						Content: &mcp.TextContent{Text: systemMessage},
					},
					{
						Role:    "user",
						Content: &mcp.TextContent{Text: userMessage},
					},
				},
			}, nil
		},
	}
}
