// Package demo provides an embedded SQLite demo database with sample
// e-commerce data for trying Conduit without a real database.
package demo

import (
	"context"
	"database/sql"
	_ "embed"
	"fmt"
	"os"
	"path/filepath"

	_ "modernc.org/sqlite"
)

//go:embed seed.sql
var seedSQL string

// CreateDemoDB creates a temporary SQLite database seeded with demo data.
// Returns the DSN suitable for the SQLite connector. The caller is responsible
// for cleaning up the temp directory.
func CreateDemoDB(ctx context.Context) (dsn string, cleanup func(), err error) {
	tmpDir, err := os.MkdirTemp("", "conduit-demo-*")
	if err != nil {
		return "", nil, fmt.Errorf("failed to create temp directory: %w", err)
	}
	cleanup = func() { os.RemoveAll(tmpDir) }

	dbPath := filepath.Join(tmpDir, "demo.db")
	db, err := sql.Open("sqlite", dbPath)
	if err != nil {
		cleanup()
		return "", nil, fmt.Errorf("failed to open SQLite database: %w", err)
	}
	defer db.Close()

	// Enable WAL mode for better concurrent read performance.
	if _, err := db.ExecContext(ctx, "PRAGMA journal_mode=WAL"); err != nil {
		cleanup()
		return "", nil, fmt.Errorf("failed to set WAL mode: %w", err)
	}

	// Execute seed SQL.
	if _, err := db.ExecContext(ctx, seedSQL); err != nil {
		cleanup()
		return "", nil, fmt.Errorf("failed to seed demo database: %w", err)
	}

	return "sqlite://" + dbPath, cleanup, nil
}
