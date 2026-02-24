package cli

import (
	"context"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"

	"github.com/conduitdb/conduit/internal/app"
	"github.com/conduitdb/conduit/internal/connector"
	_ "github.com/conduitdb/conduit/internal/connector/postgres"
	_ "github.com/conduitdb/conduit/internal/connector/sqlite"
	"github.com/conduitdb/conduit/internal/query"
	"github.com/conduitdb/conduit/internal/server"
	"github.com/conduitdb/conduit/internal/web"
	"github.com/modelcontextprotocol/go-sdk/mcp"
	"github.com/spf13/cobra"
)

type serveFlags struct {
	stdio       bool
	httpMode    bool
	port        int
	host        string
	allowWrites bool
	allowRawSQL bool
	maskPII     bool
	maxRows     int
	authToken   string
	configFile  string
}

func newServeCmd() *cobra.Command {
	var flags serveFlags

	cmd := &cobra.Command{
		Use:   "serve [DSN...]",
		Short: "Start the MCP server",
		Long:  `Start the Conduit MCP server. By default uses stdio transport. Use --http for Streamable HTTP with web dashboard.`,
		Args:  cobra.MinimumNArgs(0),
		RunE: func(cmd *cobra.Command, args []string) error {
			if len(args) == 0 && flags.configFile == "" {
				return fmt.Errorf("provide a DSN or use --config")
			}
			dsn := ""
			if len(args) > 0 {
				dsn = args[0]
			}
			return runServe(dsn, flags)
		},
	}

	cmd.Flags().BoolVar(&flags.stdio, "stdio", false, "Use stdio transport (default when no --http)")
	cmd.Flags().BoolVar(&flags.httpMode, "http", false, "Use HTTP transport with web dashboard")
	cmd.Flags().IntVarP(&flags.port, "port", "p", 8090, "HTTP server port")
	cmd.Flags().StringVar(&flags.host, "host", "localhost", "HTTP server host")
	cmd.Flags().BoolVar(&flags.allowWrites, "allow-writes", false, "Enable write operations (insert/update/delete)")
	cmd.Flags().BoolVar(&flags.allowRawSQL, "allow-raw-sql", false, "Enable raw SQL tool")
	cmd.Flags().BoolVar(&flags.maskPII, "mask-pii", false, "Mask PII columns in output")
	cmd.Flags().IntVar(&flags.maxRows, "max-rows", 1000, "Maximum rows per query")
	cmd.Flags().StringVar(&flags.authToken, "auth-token", "", "Bearer token for HTTP auth")
	cmd.Flags().StringVarP(&flags.configFile, "config", "c", "", "Path to config file")

	return cmd
}

func runServe(dsn string, flags serveFlags) error {
	logger := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{
		Level: slog.LevelInfo,
	}))

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Handle signals for graceful shutdown.
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
	go func() {
		<-sigCh
		logger.Info("shutting down...")
		cancel()
	}()

	// Build and start the application.
	application := app.New(app.Config{
		DSN: dsn,
		Connection: connector.ConnectionConfig{
			DSN: dsn,
		},
		QueryLimits: query.Limits{
			MaxRows:     flags.maxRows,
			AllowWrites: flags.allowWrites,
		},
		MaskPII: flags.maskPII,
		Logger:  logger,
	})

	if err := application.Start(ctx); err != nil {
		return fmt.Errorf("failed to start: %w", err)
	}
	defer application.Stop()

	// Build the MCP server.
	mcpSrv := server.New(application.Connector(), server.ServerConfig{
		Name:        "conduit",
		Version:     version,
		AllowWrites: flags.allowWrites,
		AllowRawSQL: flags.allowRawSQL,
		MaskPII:     flags.maskPII,
		MaxRows:     flags.maxRows,
		Instructions: fmt.Sprintf(
			"Connected to %s database. Use list_tables to see available tables, "+
				"describe_table for details, and query to read data. "+
				"Call enable_table_tools to load typed tools for specific tables.",
			application.Connector().DriverName()),
	}, logger)

	// Decide transport mode.
	useHTTP := flags.httpMode
	if !useHTTP && !flags.stdio {
		// Default: stdio if no --http flag.
		flags.stdio = true
	}

	if useHTTP {
		return runHTTP(ctx, mcpSrv, flags, logger)
	}
	return runStdio(ctx, mcpSrv, logger)
}

func runStdio(ctx context.Context, srv *server.Server, logger *slog.Logger) error {
	logger.Info("starting MCP server (stdio transport)")
	return srv.Run(ctx, &mcp.StdioTransport{})
}

func runHTTP(ctx context.Context, srv *server.Server, flags serveFlags, logger *slog.Logger) error {
	addr := fmt.Sprintf("%s:%d", flags.host, flags.port)
	logger.Info("starting HTTP server", "addr", addr)

	httpMux := http.NewServeMux()

	// MCP Streamable HTTP endpoint.
	mcpHandler := mcp.NewStreamableHTTPHandler(
		func(r *http.Request) *mcp.Server { return srv.MCPServer() },
		nil,
	)
	httpMux.Handle("/mcp", mcpHandler)

	// Web dashboard.
	webHandler := web.NewHandler(version, logger)
	httpMux.Handle("/ui/", webHandler)
	httpMux.Handle("/ui", webHandler)
	httpMux.Handle("/api/", webHandler)
	httpMux.Handle("/healthz", webHandler)
	httpMux.Handle("/.well-known/mcp.json", webHandler)

	// Root redirect to dashboard.
	httpMux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/" {
			http.Redirect(w, r, "/ui/", http.StatusTemporaryRedirect)
			return
		}
		http.NotFound(w, r)
	})

	httpSrv := &http.Server{
		Addr:    addr,
		Handler: httpMux,
	}

	// Print connection info.
	fmt.Fprintf(os.Stderr, "\n  Conduit is running!\n\n")
	fmt.Fprintf(os.Stderr, "  Dashboard:    http://%s/ui\n", addr)
	fmt.Fprintf(os.Stderr, "  MCP endpoint: http://%s/mcp\n", addr)
	fmt.Fprintf(os.Stderr, "  Health check: http://%s/healthz\n\n", addr)
	fmt.Fprintf(os.Stderr, "  Add to Claude Code:\n")
	fmt.Fprintf(os.Stderr, "    claude mcp add conduit --transport http http://%s/mcp\n\n", addr)

	go func() {
		<-ctx.Done()
		httpSrv.Close()
	}()

	if err := httpSrv.ListenAndServe(); err != http.ErrServerClosed {
		return fmt.Errorf("HTTP server error: %w", err)
	}
	return nil
}
