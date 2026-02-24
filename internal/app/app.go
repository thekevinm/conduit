// Package app provides the top-level application orchestrator that wires
// together all Conduit subsystems: connector, schema cache, query engine,
// and (eventually) the MCP server.
package app

import (
	"context"
	"fmt"
	"log/slog"

	"github.com/conduitdb/conduit/internal/connector"
	"github.com/conduitdb/conduit/internal/query"
	"github.com/conduitdb/conduit/internal/schema"
)

// Config holds all application-level configuration.
type Config struct {
	// DSN is the database connection string.
	DSN string

	// Connection settings.
	Connection connector.ConnectionConfig

	// Cache settings.
	Cache schema.CacheConfig

	// Query limits.
	QueryLimits query.Limits

	// MaskPII enables PII detection and masking.
	MaskPII bool

	// Logger is the structured logger. If nil, slog.Default() is used.
	Logger *slog.Logger
}

// App is the central orchestrator. It owns the lifecycle of all subsystems
// and provides the top-level API consumed by the MCP server layer and CLI.
type App struct {
	cfg       Config
	logger    *slog.Logger
	conn      connector.Connector
	cache     *schema.Cache
	engine    *query.Engine
	pii       *schema.PIIDetector
}

// New creates a new App from the given configuration. It does not open
// connections or start background tasks — call Start for that.
func New(cfg Config) *App {
	if cfg.Logger == nil {
		cfg.Logger = slog.Default()
	}
	return &App{
		cfg:    cfg,
		logger: cfg.Logger,
		pii:    schema.NewPIIDetector(),
	}
}

// Start initializes the connector, populates the schema cache, and starts
// background refresh. The provided context controls the background refresh
// goroutine's lifecycle.
func (a *App) Start(ctx context.Context) error {
	a.logger.Info("starting conduit",
		slog.String("dsn", connector.SanitizeDSN(a.cfg.DSN)))

	// Resolve driver from DSN.
	driver, err := connector.ParseDSN(a.cfg.DSN)
	if err != nil {
		return fmt.Errorf("failed to parse DSN: %w", err)
	}
	a.logger.Info("detected database driver", slog.String("driver", driver))

	// Create and open the connector.
	conn, err := connector.New(driver)
	if err != nil {
		return fmt.Errorf("failed to create connector: %w", err)
	}

	if err := conn.Open(ctx, a.cfg.Connection); err != nil {
		return fmt.Errorf("failed to open database connection: %w", err)
	}

	if err := conn.Ping(ctx); err != nil {
		return fmt.Errorf("failed to ping database: %w", err)
	}
	a.conn = conn
	a.logger.Info("database connection established")

	// Build the schema cache backed by the connector.
	a.cache = schema.NewCache(connectorSchemaAdapter{conn}, a.cfg.Cache, a.logger)
	a.cache.Start(ctx)

	// Perform initial schema load.
	if err := a.cache.Refresh(ctx); err != nil {
		a.logger.Warn("initial schema refresh failed (will retry in background)",
			slog.String("error", err.Error()))
	} else {
		stats := a.cache.Stats()
		a.logger.Info("schema loaded",
			slog.Int("tables", stats.TableCount),
			slog.Int("details", stats.DetailCount))
	}

	// Build the query engine.
	a.engine = query.NewEngine(conn, a.cache, query.EngineConfig{
		Limits:  a.cfg.QueryLimits,
		MaskPII: a.cfg.MaskPII,
	}, a.logger)

	a.logger.Info("conduit ready",
		slog.String("driver", driver),
		slog.Bool("mask_pii", a.cfg.MaskPII),
		slog.Bool("allow_writes", a.cfg.QueryLimits.AllowWrites),
		slog.Int("max_rows", a.cfg.QueryLimits.MaxRows))

	return nil
}

// Stop gracefully shuts down all subsystems.
func (a *App) Stop() error {
	a.logger.Info("stopping conduit")

	if a.cache != nil {
		a.cache.Stop()
	}
	if a.conn != nil {
		if err := a.conn.Close(); err != nil {
			a.logger.Warn("error closing database connection",
				slog.String("error", err.Error()))
			return err
		}
	}

	a.logger.Info("conduit stopped")
	return nil
}

// Connector returns the underlying database connector.
func (a *App) Connector() connector.Connector {
	return a.conn
}

// Cache returns the schema cache.
func (a *App) Cache() *schema.Cache {
	return a.cache
}

// Engine returns the query engine.
func (a *App) Engine() *query.Engine {
	return a.engine
}

// PIIDetector returns the PII detector.
func (a *App) PIIDetector() *schema.PIIDetector {
	return a.pii
}

// connectorSchemaAdapter wraps a connector.Connector to satisfy
// schema.SchemaProvider without importing cycles.
type connectorSchemaAdapter struct {
	conn connector.Connector
}

func (a connectorSchemaAdapter) ListTables(ctx context.Context) ([]schema.TableSummary, error) {
	return a.conn.ListTables(ctx)
}

func (a connectorSchemaAdapter) DescribeTable(ctx context.Context, tableName string) (*schema.TableDetail, error) {
	return a.conn.DescribeTable(ctx, tableName)
}
