// Package server wires up the official MCP Go SDK server with Conduit's
// connector and tool-generation layers.
package server

import (
	"context"
	"fmt"
	"log/slog"

	"github.com/conduitdb/conduit/internal/connector"
	"github.com/conduitdb/conduit/internal/mcpgen"
	"github.com/modelcontextprotocol/go-sdk/mcp"
)

// ServerConfig holds configuration for the MCP server.
type ServerConfig struct {
	Name         string
	Version      string
	AllowWrites  bool
	AllowRawSQL  bool
	MaskPII      bool
	MaxRows      int
	Instructions string
}

// DefaultConfig returns a ServerConfig with sensible defaults.
func DefaultConfig() ServerConfig {
	return ServerConfig{
		Name:    "conduit",
		Version: "0.1.0",
		MaxRows: 1000,
	}
}

// Server wraps an MCP server and its associated connector and tool generator.
type Server struct {
	mcpServer *mcp.Server
	config    ServerConfig
	conn      connector.Connector
	gen       *mcpgen.Generator
	logger    *slog.Logger
}

// New creates a new Conduit MCP server backed by the given connector.
// The connector should already be opened and ready.
func New(conn connector.Connector, cfg ServerConfig, logger *slog.Logger) *Server {
	if logger == nil {
		logger = slog.Default()
	}

	impl := &mcp.Implementation{
		Name:    cfg.Name,
		Version: cfg.Version,
	}

	opts := &mcp.ServerOptions{
		Instructions: cfg.Instructions,
		Logger:       logger,
	}

	mcpSrv := mcp.NewServer(impl, opts)

	gen := mcpgen.NewGenerator(conn, mcpgen.GeneratorConfig{
		AllowWrites: cfg.AllowWrites,
		AllowRawSQL: cfg.AllowRawSQL,
		MaskPII:     cfg.MaskPII,
		MaxRows:     cfg.MaxRows,
	})

	s := &Server{
		mcpServer: mcpSrv,
		config:    cfg,
		conn:      conn,
		gen:       gen,
		logger:    logger,
	}

	// Wire up the tool registration callback so enable_table_tools can
	// dynamically register Tier 2 tools on the MCP server.
	gen.OnRegisterTool = func(tool *mcp.Tool, handler mcp.ToolHandler) {
		mcpSrv.AddTool(tool, handler)
		logger.Debug("registered dynamic tool via callback", "name", tool.Name)
	}

	// Register Tier 1 core tools (always present).
	s.registerCoreTools()

	// Register resources.
	s.registerResources()

	// Register prompts.
	s.registerPrompts()

	return s
}

// MCPServer returns the underlying MCP server for direct transport wiring.
func (s *Server) MCPServer() *mcp.Server {
	return s.mcpServer
}

// Run runs the server over the given transport until it closes or the context
// is cancelled. For stdio usage, pass &mcp.StdioTransport{}.
func (s *Server) Run(ctx context.Context, transport mcp.Transport) error {
	s.logger.Info("conduit server starting",
		"name", s.config.Name,
		"version", s.config.Version,
		"allow_writes", s.config.AllowWrites,
		"allow_raw_sql", s.config.AllowRawSQL,
	)
	return s.mcpServer.Run(ctx, transport)
}

// registerCoreTools registers all Tier 1 core tools on the MCP server.
func (s *Server) registerCoreTools() {
	tools := s.gen.CoreTools()
	for _, t := range tools {
		s.mcpServer.AddTool(t.Tool, t.Handler)
		s.logger.Debug("registered core tool", "name", t.Tool.Name)
	}
	s.logger.Info(fmt.Sprintf("registered %d core tools", len(tools)))
}

// registerResources registers all resources and resource templates.
func (s *Server) registerResources() {
	resources := s.gen.Resources()
	for _, r := range resources {
		s.mcpServer.AddResource(r.Resource, r.Handler)
		s.logger.Debug("registered resource", "uri", r.Resource.URI)
	}

	templates := s.gen.ResourceTemplates()
	for _, t := range templates {
		s.mcpServer.AddResourceTemplate(t.Template, t.Handler)
		s.logger.Debug("registered resource template", "uri", t.Template.URITemplate)
	}
	s.logger.Info(fmt.Sprintf("registered %d resources and %d resource templates", len(resources), len(templates)))
}

// registerPrompts registers all built-in prompts.
func (s *Server) registerPrompts() {
	prompts := s.gen.Prompts()
	for _, p := range prompts {
		s.mcpServer.AddPrompt(p.Prompt, p.Handler)
		s.logger.Debug("registered prompt", "name", p.Prompt.Name)
	}
	s.logger.Info(fmt.Sprintf("registered %d prompts", len(prompts)))
}

// EnableTableTools dynamically registers Tier 2 per-table tools and notifies
// connected clients of the change. This is called by the enable_table_tools
// core tool handler.
func (s *Server) EnableTableTools(ctx context.Context, tables []string) error {
	toolDefs, err := s.gen.DynamicToolsForTables(ctx, tables)
	if err != nil {
		return err
	}

	for _, t := range toolDefs {
		s.mcpServer.AddTool(t.Tool, t.Handler)
		s.logger.Debug("registered dynamic tool", "name", t.Tool.Name)
	}
	s.logger.Info(fmt.Sprintf("enabled %d dynamic tools for %d tables", len(toolDefs), len(tables)))

	// tools/list_changed notification is automatically sent by the SDK
	// when AddTool is called.
	return nil
}

// DisableTableTools removes Tier 2 per-table tools.
func (s *Server) DisableTableTools(tables []string) {
	names := s.gen.DynamicToolNamesForTables(tables)
	s.mcpServer.RemoveTools(names...)
	s.logger.Info(fmt.Sprintf("disabled %d dynamic tools", len(names)))
}
