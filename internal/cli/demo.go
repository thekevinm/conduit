package cli

import (
	"context"
	"fmt"
	"log/slog"
	"os"

	"github.com/conduitdb/conduit/internal/demo"
	"github.com/spf13/cobra"
)

func newDemoCmd() *cobra.Command {
	var httpMode bool
	var port int

	cmd := &cobra.Command{
		Use:   "demo",
		Short: "Start with embedded sample data (no database needed)",
		Long: `Start Conduit in demo mode with an embedded SQLite database
containing sample e-commerce data (customers, products, orders, reviews).
Perfect for trying out Conduit without setting up a database.`,
		RunE: func(cmd *cobra.Command, args []string) error {
			return runDemo(httpMode, port)
		},
	}

	cmd.Flags().BoolVar(&httpMode, "http", false, "Use HTTP transport with web dashboard")
	cmd.Flags().IntVarP(&port, "port", "p", 8090, "HTTP server port")

	return cmd
}

func runDemo(httpMode bool, port int) error {
	logger := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{
		Level: slog.LevelInfo,
	}))

	ctx := context.Background()

	logger.Info("creating demo database with sample e-commerce data...")
	dsn, cleanup, err := demo.CreateDemoDB(ctx)
	if err != nil {
		return fmt.Errorf("failed to create demo database: %w", err)
	}
	defer cleanup()
	logger.Info("demo database ready", "dsn", dsn)

	fmt.Fprintf(os.Stderr, "\n  Demo mode: using embedded SQLite with sample data\n")
	fmt.Fprintf(os.Stderr, "  Tables: customers, products, orders, order_items, reviews\n\n")

	flags := serveFlags{
		stdio:       !httpMode,
		httpMode:    httpMode,
		port:        port,
		host:        "localhost",
		allowWrites: true,
		maxRows:     1000,
	}
	return runServe(dsn, flags)
}
