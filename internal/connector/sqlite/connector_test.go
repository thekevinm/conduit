package sqlite

import (
	"context"
	"testing"

	"github.com/conduitdb/conduit/internal/connector"
	"github.com/conduitdb/conduit/internal/demo"
)

func setupTestDB(t *testing.T) (*Connector, func()) {
	t.Helper()
	ctx := context.Background()
	dsn, cleanup, err := demo.CreateDemoDB(ctx)
	if err != nil {
		t.Fatalf("failed to create demo db: %v", err)
	}

	c := &Connector{}
	if err := c.Open(ctx, connector.ConnectionConfig{DSN: dsn}); err != nil {
		cleanup()
		t.Fatalf("failed to open connector: %v", err)
	}

	return c, func() {
		c.Close()
		cleanup()
	}
}

func TestListTables(t *testing.T) {
	c, cleanup := setupTestDB(t)
	defer cleanup()

	tables, err := c.ListTables(context.Background())
	if err != nil {
		t.Fatalf("ListTables: %v", err)
	}

	if len(tables) != 5 {
		t.Errorf("expected 5 tables, got %d", len(tables))
	}

	tableNames := make(map[string]bool)
	for _, tbl := range tables {
		tableNames[tbl.Name] = true
		if tbl.RowCount <= 0 {
			t.Errorf("table %q: expected positive row count, got %d", tbl.Name, tbl.RowCount)
		}
	}

	for _, expected := range []string{"customers", "products", "orders", "order_items", "reviews"} {
		if !tableNames[expected] {
			t.Errorf("expected table %q not found", expected)
		}
	}
}

func TestDescribeTable(t *testing.T) {
	c, cleanup := setupTestDB(t)
	defer cleanup()

	detail, err := c.DescribeTable(context.Background(), "customers")
	if err != nil {
		t.Fatalf("DescribeTable: %v", err)
	}

	if detail.Name != "customers" {
		t.Errorf("expected name 'customers', got %q", detail.Name)
	}
	if len(detail.Columns) == 0 {
		t.Error("expected columns, got none")
	}

	// Check we have the expected columns.
	colNames := make(map[string]bool)
	for _, col := range detail.Columns {
		colNames[col.Name] = true
	}
	for _, expected := range []string{"id", "first_name", "last_name", "email"} {
		if !colNames[expected] {
			t.Errorf("expected column %q not found", expected)
		}
	}

	// Check primary key.
	if len(detail.PrimaryKey) == 0 {
		t.Error("expected primary key, got none")
	}
}

func TestSelect(t *testing.T) {
	c, cleanup := setupTestDB(t)
	defer cleanup()

	rs, err := c.Select(context.Background(), connector.SelectRequest{
		Table: "customers",
		Limit: 5,
	})
	if err != nil {
		t.Fatalf("Select: %v", err)
	}

	if len(rs.Rows) != 5 {
		t.Errorf("expected 5 rows, got %d", len(rs.Rows))
	}
	if len(rs.Columns) == 0 {
		t.Error("expected columns, got none")
	}
}

func TestSelectWithColumns(t *testing.T) {
	c, cleanup := setupTestDB(t)
	defer cleanup()

	rs, err := c.Select(context.Background(), connector.SelectRequest{
		Table:   "products",
		Columns: []string{"name", "price"},
		Limit:   3,
	})
	if err != nil {
		t.Fatalf("Select: %v", err)
	}

	if len(rs.Columns) != 2 {
		t.Errorf("expected 2 columns, got %d", len(rs.Columns))
	}
	if len(rs.Rows) != 3 {
		t.Errorf("expected 3 rows, got %d", len(rs.Rows))
	}
}

func TestSelectWithOrderBy(t *testing.T) {
	c, cleanup := setupTestDB(t)
	defer cleanup()

	rs, err := c.Select(context.Background(), connector.SelectRequest{
		Table:   "products",
		Columns: []string{"name", "price"},
		OrderBy: "price DESC",
		Limit:   3,
	})
	if err != nil {
		t.Fatalf("Select: %v", err)
	}

	if len(rs.Rows) != 3 {
		t.Errorf("expected 3 rows, got %d", len(rs.Rows))
	}
}

func TestInsertUpdateDelete(t *testing.T) {
	c, cleanup := setupTestDB(t)
	defer cleanup()

	ctx := context.Background()

	// Insert.
	insertResult, err := c.Insert(ctx, connector.InsertRequest{
		Table: "customers",
		Rows: []map[string]any{
			{
				"first_name": "Test",
				"last_name":  "User",
				"email":      "test@example.com",
			},
		},
	})
	if err != nil {
		t.Fatalf("Insert: %v", err)
	}
	if insertResult.RowsAffected != 1 {
		t.Errorf("expected 1 row affected, got %d", insertResult.RowsAffected)
	}

	// Update.
	updateResult, err := c.Update(ctx, connector.UpdateRequest{
		Table:  "customers",
		Filter: "email = 'test@example.com'",
		Set:    map[string]any{"first_name": "Updated"},
	})
	if err != nil {
		t.Fatalf("Update: %v", err)
	}
	if updateResult.RowsAffected != 1 {
		t.Errorf("expected 1 row affected, got %d", updateResult.RowsAffected)
	}

	// Delete.
	deleteResult, err := c.Delete(ctx, connector.DeleteRequest{
		Table:  "customers",
		Filter: "email = 'test@example.com'",
	})
	if err != nil {
		t.Fatalf("Delete: %v", err)
	}
	if deleteResult.RowsAffected != 1 {
		t.Errorf("expected 1 row affected, got %d", deleteResult.RowsAffected)
	}
}

func TestDriverName(t *testing.T) {
	c := &Connector{}
	if c.DriverName() != "sqlite" {
		t.Errorf("expected 'sqlite', got %q", c.DriverName())
	}
}

func TestQuoteIdentifier(t *testing.T) {
	c := &Connector{}
	tests := []struct {
		input    string
		expected string
	}{
		{"users", `"users"`},
		{`has"quote`, `"has""quote"`},
		{"order_items", `"order_items"`},
	}
	for _, tt := range tests {
		got := c.QuoteIdentifier(tt.input)
		if got != tt.expected {
			t.Errorf("QuoteIdentifier(%q) = %q, want %q", tt.input, got, tt.expected)
		}
	}
}

func TestMapSQLiteType(t *testing.T) {
	tests := []struct {
		input    string
		expected string
	}{
		{"INTEGER", "integer"},
		{"INT", "integer"},
		{"REAL", "decimal"},
		{"FLOAT", "decimal"},
		{"TEXT", "string"},
		{"BOOLEAN", "boolean"},
		{"DATETIME", "datetime"},
		{"BLOB", "binary"},
		{"JSON", "json"},
		{"VARCHAR(255)", "string"},
	}
	for _, tt := range tests {
		got := mapSQLiteType(tt.input)
		if got != tt.expected {
			t.Errorf("mapSQLiteType(%q) = %q, want %q", tt.input, got, tt.expected)
		}
	}
}
