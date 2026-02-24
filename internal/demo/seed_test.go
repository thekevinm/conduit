package demo

import (
	"context"
	"database/sql"
	"testing"

	_ "modernc.org/sqlite"
)

func TestCreateDemoDB(t *testing.T) {
	ctx := context.Background()
	dsn, cleanup, err := CreateDemoDB(ctx)
	if err != nil {
		t.Fatalf("CreateDemoDB failed: %v", err)
	}
	defer cleanup()

	if dsn == "" {
		t.Fatal("DSN should not be empty")
	}

	// Open the database directly and verify tables/data.
	dbPath := dsn[len("sqlite://"):]
	db, err := sql.Open("sqlite", dbPath)
	if err != nil {
		t.Fatalf("failed to open demo db: %v", err)
	}
	defer db.Close()

	// Verify all 5 tables exist.
	tables := []string{"customers", "products", "orders", "order_items", "reviews"}
	for _, table := range tables {
		var count int
		err := db.QueryRowContext(ctx, "SELECT COUNT(*) FROM "+table).Scan(&count)
		if err != nil {
			t.Errorf("table %q: %v", table, err)
			continue
		}
		if count == 0 {
			t.Errorf("table %q: expected rows, got 0", table)
		}
	}

	// Verify specific counts.
	checks := map[string]int{
		"customers":   8,
		"products":    10,
		"orders":      10,
		"order_items": 19,
		"reviews":     8,
	}
	for table, expected := range checks {
		var count int
		db.QueryRowContext(ctx, "SELECT COUNT(*) FROM "+table).Scan(&count)
		if count != expected {
			t.Errorf("table %q: expected %d rows, got %d", table, expected, count)
		}
	}

	// Verify a join works (foreign keys are correct).
	var orderCount int
	err = db.QueryRowContext(ctx, `
		SELECT COUNT(*) FROM orders o
		JOIN customers c ON o.customer_id = c.id`).Scan(&orderCount)
	if err != nil {
		t.Fatalf("join query failed: %v", err)
	}
	if orderCount != 10 {
		t.Errorf("expected 10 orders with valid customers, got %d", orderCount)
	}
}
