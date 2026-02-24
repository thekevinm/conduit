// Package mcpgen generates MCP tools, resources, and prompts from database
// schema information obtained via the connector interface.
package mcpgen

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"

	"github.com/conduitdb/conduit/internal/connector"
	"github.com/conduitdb/conduit/internal/schema"
	"github.com/modelcontextprotocol/go-sdk/mcp"
)

// MaxDynamicTables is the maximum number of tables that can have Tier 2 tools
// loaded simultaneously.
const MaxDynamicTables = 20

// GeneratorConfig controls what tools and features are generated.
type GeneratorConfig struct {
	AllowWrites bool
	AllowRawSQL bool
	MaskPII     bool
	MaxRows     int
}

// ToolDef bundles a Tool definition with its handler for registration.
type ToolDef struct {
	Tool    *mcp.Tool
	Handler mcp.ToolHandler
}

// ResourceDef bundles a Resource definition with its handler.
type ResourceDef struct {
	Resource *mcp.Resource
	Handler  mcp.ResourceHandler
}

// ResourceTemplateDef bundles a ResourceTemplate with its handler.
type ResourceTemplateDef struct {
	Template *mcp.ResourceTemplate
	Handler  mcp.ResourceHandler
}

// PromptDef bundles a Prompt definition with its handler.
type PromptDef struct {
	Prompt  *mcp.Prompt
	Handler mcp.PromptHandler
}

// ToolRegistrar is a callback for registering dynamically generated tools
// on the MCP server. It is called by the enable_table_tools handler.
type ToolRegistrar func(tool *mcp.Tool, handler mcp.ToolHandler)

// Generator produces MCP tools, resources, and prompts from database schema.
type Generator struct {
	conn   connector.Connector
	config GeneratorConfig

	// OnRegisterTool is called to register dynamically generated tools on the
	// MCP server. Must be set by the server layer before the enable_table_tools
	// handler is invoked.
	OnRegisterTool ToolRegistrar

	mu             sync.RWMutex
	enabledTables  map[string]bool // currently enabled tables for Tier 2 tools
	schemaCache    map[string]*schema.TableDetail
	tableSummaries []schema.TableSummary
}

// NewGenerator creates a generator backed by the given connector.
func NewGenerator(conn connector.Connector, cfg GeneratorConfig) *Generator {
	if cfg.MaxRows <= 0 {
		cfg.MaxRows = 1000
	}
	return &Generator{
		conn:          conn,
		config:        cfg,
		enabledTables: make(map[string]bool),
		schemaCache:   make(map[string]*schema.TableDetail),
	}
}

// CoreTools returns Tier 1 core tool definitions (always present).
func (g *Generator) CoreTools() []ToolDef {
	return g.buildCoreTools()
}

// DynamicToolsForTables generates Tier 2 per-table tools for the given tables.
// Returns an error if too many tables are requested or a table doesn't exist.
func (g *Generator) DynamicToolsForTables(ctx context.Context, tables []string) ([]ToolDef, error) {
	g.mu.Lock()
	defer g.mu.Unlock()

	// Count total tables that would be enabled.
	newCount := 0
	for _, t := range tables {
		if !g.enabledTables[t] {
			newCount++
		}
	}
	total := len(g.enabledTables) + newCount
	if total > MaxDynamicTables {
		return nil, fmt.Errorf("cannot enable %d tables: would exceed maximum of %d (currently %d enabled)",
			len(tables), MaxDynamicTables, len(g.enabledTables))
	}

	var allTools []ToolDef
	for _, tableName := range tables {
		// Get table detail (cached).
		detail, err := g.getTableDetail(ctx, tableName)
		if err != nil {
			return nil, fmt.Errorf("table %q: %w", tableName, err)
		}

		tools := g.buildDynamicTools(detail)
		allTools = append(allTools, tools...)
		g.enabledTables[tableName] = true
	}

	return allTools, nil
}

// DynamicToolNamesForTables returns the tool names that would be generated for
// the given tables. Used for removal.
func (g *Generator) DynamicToolNamesForTables(tables []string) []string {
	g.mu.Lock()
	defer g.mu.Unlock()

	var names []string
	for _, t := range tables {
		names = append(names, dynamicToolNames(t, g.config.AllowWrites)...)
		delete(g.enabledTables, t)
	}
	return names
}

// EnabledTables returns the list of currently enabled tables.
func (g *Generator) EnabledTables() []string {
	g.mu.RLock()
	defer g.mu.RUnlock()
	tables := make([]string, 0, len(g.enabledTables))
	for t := range g.enabledTables {
		tables = append(tables, t)
	}
	return tables
}

// Resources returns static resource definitions.
func (g *Generator) Resources() []ResourceDef {
	return g.buildResources()
}

// ResourceTemplates returns resource template definitions.
func (g *Generator) ResourceTemplates() []ResourceTemplateDef {
	return g.buildResourceTemplates()
}

// Prompts returns built-in prompt definitions.
func (g *Generator) Prompts() []PromptDef {
	return g.buildPrompts()
}

// InvalidateSchema clears the cached schema, forcing a refresh on next access.
func (g *Generator) InvalidateSchema() {
	g.mu.Lock()
	defer g.mu.Unlock()
	g.schemaCache = make(map[string]*schema.TableDetail)
	g.tableSummaries = nil
}

// getTableDetail returns the detail for a table, using the cache if available.
// Caller must hold g.mu.
func (g *Generator) getTableDetail(ctx context.Context, tableName string) (*schema.TableDetail, error) {
	if detail, ok := g.schemaCache[tableName]; ok {
		return detail, nil
	}
	detail, err := g.conn.DescribeTable(ctx, tableName)
	if err != nil {
		return nil, err
	}
	g.schemaCache[tableName] = detail
	return detail, nil
}

// getTableSummaries returns the list of tables, using the cache if available.
func (g *Generator) getTableSummaries(ctx context.Context) ([]schema.TableSummary, error) {
	g.mu.RLock()
	if g.tableSummaries != nil {
		cached := g.tableSummaries
		g.mu.RUnlock()
		return cached, nil
	}
	g.mu.RUnlock()

	summaries, err := g.conn.ListTables(ctx)
	if err != nil {
		return nil, err
	}

	g.mu.Lock()
	g.tableSummaries = summaries
	g.mu.Unlock()

	return summaries, nil
}

// dynamicToolNames returns the Tier 2 tool names that would be generated for a table.
func dynamicToolNames(table string, allowWrites bool) []string {
	names := []string{
		"query_" + table,
		"get_" + table + "_by_id",
	}
	if allowWrites {
		names = append(names,
			"insert_"+table,
			"update_"+table,
			"delete_"+table,
		)
	}
	return names
}

// Helper to create a bool pointer for use in ToolAnnotations.
func boolPtr(b bool) *bool {
	return &b
}

// toolInputSchema builds a JSON input schema map for use with Server.AddTool.
func toolInputSchema(properties map[string]any, required []string) json.RawMessage {
	schema := map[string]any{
		"type":       "object",
		"properties": properties,
	}
	if len(required) > 0 {
		schema["required"] = required
	}
	data, _ := json.Marshal(schema)
	return data
}
