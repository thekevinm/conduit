package web

import (
	"encoding/json"
	"io/fs"
	"log/slog"
	"net/http"
	"strings"
	"time"
)

// Handler serves the embedded SPA frontend and the internal REST API.
type Handler struct {
	mux    *http.ServeMux
	api    *APIHandler
	spaFS  http.Handler
	logger *slog.Logger
}

// APIHandler provides the REST API backend for the web UI.
type APIHandler struct {
	startTime time.Time
	version   string
	logger    *slog.Logger
}

// NewHandler creates a new web handler with SPA serving and API routes.
func NewHandler(version string, logger *slog.Logger) *Handler {
	api := &APIHandler{
		startTime: time.Now(),
		version:   version,
		logger:    logger,
	}

	// Extract the frontend build from the embedded FS
	frontendBuild, err := fs.Sub(FrontendFS, "static")
	if err != nil {
		logger.Error("failed to access embedded frontend", "error", err)
	}

	mux := http.NewServeMux()
	h := &Handler{
		mux:    mux,
		api:    api,
		spaFS:  http.FileServer(http.FS(frontendBuild)),
		logger: logger,
	}

	// API routes
	mux.HandleFunc("GET /api/v1/health", api.handleHealth)
	mux.HandleFunc("GET /api/v1/sources", api.handleListSources)
	mux.HandleFunc("POST /api/v1/sources", api.handleAddSource)
	mux.HandleFunc("POST /api/v1/sources/test", api.handleTestConnection)
	mux.HandleFunc("GET /api/v1/stats", api.handleStats)
	mux.HandleFunc("GET /api/v1/config", api.handleGetConfig)
	mux.HandleFunc("GET /api/v1/audit/recent", api.handleAuditRecent)
	mux.HandleFunc("GET /api/v1/clients/{name}/snippet", api.handleClientSnippet)

	// Healthcheck (top-level, not behind /ui)
	mux.HandleFunc("GET /healthz", api.handleHealthz)

	// MCP server card
	mux.HandleFunc("GET /.well-known/mcp.json", api.handleMCPCard)

	// SPA fallback: serve frontend for /ui/* paths
	mux.HandleFunc("/ui/", h.handleSPA)
	mux.HandleFunc("/ui", h.handleSPA)

	return h
}

// ServeHTTP implements http.Handler.
func (h *Handler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	h.mux.ServeHTTP(w, r)
}

// handleSPA serves the SPA with fallback to index.html for client-side routing.
func (h *Handler) handleSPA(w http.ResponseWriter, r *http.Request) {
	// Strip /ui prefix for file serving
	path := strings.TrimPrefix(r.URL.Path, "/ui")
	if path == "" || path == "/" {
		path = "/index.html"
	}

	// Try to serve the static file
	r.URL.Path = path
	h.spaFS.ServeHTTP(w, r)
}

// --- API Handlers ---

func (a *APIHandler) handleHealth(w http.ResponseWriter, r *http.Request) {
	writeJSON(w, http.StatusOK, map[string]any{
		"status":  "ok",
		"version": a.version,
		"uptime":  time.Since(a.startTime).String(),
	})
}

func (a *APIHandler) handleHealthz(w http.ResponseWriter, r *http.Request) {
	writeJSON(w, http.StatusOK, map[string]any{
		"status":  "ok",
		"version": a.version,
		"uptime":  time.Since(a.startTime).String(),
	})
}

func (a *APIHandler) handleListSources(w http.ResponseWriter, r *http.Request) {
	// TODO: Wire to actual source registry
	writeJSON(w, http.StatusOK, map[string]any{
		"sources": []any{},
	})
}

func (a *APIHandler) handleAddSource(w http.ResponseWriter, r *http.Request) {
	var req map[string]any
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "invalid JSON"})
		return
	}
	// TODO: Wire to actual source management
	writeJSON(w, http.StatusCreated, map[string]any{"status": "created"})
}

func (a *APIHandler) handleTestConnection(w http.ResponseWriter, r *http.Request) {
	var req map[string]any
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "invalid JSON"})
		return
	}
	// TODO: Actually test the connection
	writeJSON(w, http.StatusOK, map[string]any{
		"success": true,
		"tables":  0,
		"message": "Connection test not yet implemented",
	})
}

func (a *APIHandler) handleStats(w http.ResponseWriter, r *http.Request) {
	writeJSON(w, http.StatusOK, map[string]any{
		"sources_connected": 0,
		"tables_exposed":    0,
		"queries_today":     0,
		"avg_latency_ms":    0,
		"uptime":            time.Since(a.startTime).String(),
	})
}

func (a *APIHandler) handleGetConfig(w http.ResponseWriter, r *http.Request) {
	writeJSON(w, http.StatusOK, map[string]any{
		"server": map[string]any{
			"version": a.version,
		},
	})
}

func (a *APIHandler) handleAuditRecent(w http.ResponseWriter, r *http.Request) {
	writeJSON(w, http.StatusOK, map[string]any{
		"events": []any{},
	})
}

func (a *APIHandler) handleClientSnippet(w http.ResponseWriter, r *http.Request) {
	clientName := r.PathValue("name")
	snippet := generateClientSnippet(clientName, "localhost", 8090)
	writeJSON(w, http.StatusOK, map[string]any{
		"client":  clientName,
		"snippet": snippet,
	})
}

func (a *APIHandler) handleMCPCard(w http.ResponseWriter, r *http.Request) {
	writeJSON(w, http.StatusOK, map[string]any{
		"name":        "conduit",
		"description": "Auto-generated MCP server for database access",
		"version":     a.version,
		"transport": map[string]string{
			"type": "streamable-http",
			"url":  "/mcp",
		},
		"capabilities": map[string]bool{
			"tools":     true,
			"resources": true,
		},
	})
}

// generateClientSnippet returns a config snippet for the given MCP client.
func generateClientSnippet(client, host string, port int) string {
	switch strings.ToLower(client) {
	case "claude-code":
		return `{
  "conduit": {
    "command": "conduit",
    "args": ["postgres://user:pass@localhost:5432/mydb"]
  }
}`
	case "claude-desktop":
		return `{
  "mcpServers": {
    "conduit": {
      "url": "http://` + host + `:` + strings.TrimRight(strings.TrimRight(json.Number(strings.Repeat("0", 0)).String(), "0"), ".") + `8090/mcp"
    }
  }
}`
	case "cursor":
		return `{
  "servers": {
    "conduit": {
      "type": "http",
      "url": "http://` + host + `:8090/mcp"
    }
  }
}`
	case "vscode":
		return `{
  "servers": {
    "conduit": {
      "type": "http",
      "url": "http://` + host + `:8090/mcp"
    }
  }
}`
	default:
		return `# Unknown client: ` + client + `
# Supported clients: claude-code, claude-desktop, cursor, vscode`
	}
}

func writeJSON(w http.ResponseWriter, status int, data any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	json.NewEncoder(w).Encode(data)
}
