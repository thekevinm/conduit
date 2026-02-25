package snowflake

import (
	"testing"

	"github.com/conduitdb/conduit/internal/connector"
)

func TestQuoteIdentifier(t *testing.T) {
	qb := &QueryBuilder{}

	tests := []struct {
		input string
		want  string
	}{
		{"users", `"users"`},
		{"PUBLIC.USERS", `"PUBLIC"."USERS"`},
		{"MY_DB.MY_SCHEMA.MY_TABLE", `"MY_DB"."MY_SCHEMA"."MY_TABLE"`},
		{`has"quote`, `"has""quote"`},
		{`sch"ema.tab"le`, `"sch""ema"."tab""le"`},
		{"simple", `"simple"`},
	}

	for _, tt := range tests {
		t.Run(tt.input, func(t *testing.T) {
			got := qb.QuoteIdentifier(tt.input)
			if got != tt.want {
				t.Errorf("QuoteIdentifier(%q) = %q, want %q", tt.input, got, tt.want)
			}
		})
	}
}

func TestQuoteIdentifier_ThreeLevel(t *testing.T) {
	qb := &QueryBuilder{}

	// Three-level database.schema.table should produce three quoted parts.
	got := qb.QuoteIdentifier("MYDB.PUBLIC.ORDERS")
	want := `"MYDB"."PUBLIC"."ORDERS"`
	if got != want {
		t.Errorf("QuoteIdentifier three-level = %q, want %q", got, want)
	}
}

func TestQuoteIdentifier_MoreThanThreeParts(t *testing.T) {
	qb := &QueryBuilder{}

	// More than 3 parts should be truncated to 3.
	got := qb.QuoteIdentifier("a.b.c.d")
	want := `"a"."b"."c"`
	if got != want {
		t.Errorf("QuoteIdentifier 4-part = %q, want %q", got, want)
	}
}

func TestParameterPlaceholder(t *testing.T) {
	qb := &QueryBuilder{}

	// Snowflake always uses ? regardless of index.
	tests := []struct {
		index int
		want  string
	}{
		{1, "?"},
		{2, "?"},
		{10, "?"},
		{100, "?"},
	}

	for _, tt := range tests {
		got := qb.ParameterPlaceholder(tt.index)
		if got != tt.want {
			t.Errorf("ParameterPlaceholder(%d) = %q, want %q", tt.index, got, tt.want)
		}
	}
}

func TestBuildSelect(t *testing.T) {
	qb := &QueryBuilder{}

	t.Run("simple select all", func(t *testing.T) {
		req := connector.SelectRequest{
			Table: "USERS",
		}
		query, args := qb.BuildSelect(req)
		wantQuery := `SELECT * FROM "USERS"`
		if query != wantQuery {
			t.Errorf("got query %q, want %q", query, wantQuery)
		}
		if len(args) != 0 {
			t.Errorf("got %d args, want 0", len(args))
		}
	})

	t.Run("select with columns", func(t *testing.T) {
		req := connector.SelectRequest{
			Table:   "PUBLIC.USERS",
			Columns: []string{"ID", "NAME", "EMAIL"},
		}
		query, args := qb.BuildSelect(req)
		wantQuery := `SELECT "ID", "NAME", "EMAIL" FROM "PUBLIC"."USERS"`
		if query != wantQuery {
			t.Errorf("got query %q, want %q", query, wantQuery)
		}
		if len(args) != 0 {
			t.Errorf("got %d args, want 0", len(args))
		}
	})

	t.Run("select with filter", func(t *testing.T) {
		req := connector.SelectRequest{
			Table:  "ORDERS",
			Filter: "STATUS = 'active'",
		}
		query, args := qb.BuildSelect(req)
		wantQuery := `SELECT * FROM "ORDERS" WHERE STATUS = 'active'`
		if query != wantQuery {
			t.Errorf("got query %q, want %q", query, wantQuery)
		}
		if len(args) != 0 {
			t.Errorf("got %d args, want 0", len(args))
		}
	})

	t.Run("select with order", func(t *testing.T) {
		req := connector.SelectRequest{
			Table:   "EVENTS",
			OrderBy: "CREATED_AT DESC",
		}
		query, args := qb.BuildSelect(req)
		wantQuery := `SELECT * FROM "EVENTS" ORDER BY CREATED_AT DESC`
		if query != wantQuery {
			t.Errorf("got query %q, want %q", query, wantQuery)
		}
		if len(args) != 0 {
			t.Errorf("got %d args, want 0", len(args))
		}
	})

	t.Run("select with limit and offset", func(t *testing.T) {
		req := connector.SelectRequest{
			Table:   "ORDERS",
			Filter:  "STATUS = 'active'",
			OrderBy: "CREATED_AT DESC",
			Limit:   50,
			Offset:  10,
		}
		query, args := qb.BuildSelect(req)
		wantQuery := `SELECT * FROM "ORDERS" WHERE STATUS = 'active' ORDER BY CREATED_AT DESC LIMIT ? OFFSET ?`
		if query != wantQuery {
			t.Errorf("got query:\n  %q\nwant:\n  %q", query, wantQuery)
		}
		if len(args) != 2 {
			t.Fatalf("got %d args, want 2", len(args))
		}
		if args[0] != 50 {
			t.Errorf("args[0] = %v, want 50", args[0])
		}
		if args[1] != 10 {
			t.Errorf("args[1] = %v, want 10", args[1])
		}
	})

	t.Run("select with limit only", func(t *testing.T) {
		req := connector.SelectRequest{
			Table: "USERS",
			Limit: 100,
		}
		query, args := qb.BuildSelect(req)
		wantQuery := `SELECT * FROM "USERS" LIMIT ?`
		if query != wantQuery {
			t.Errorf("got query %q, want %q", query, wantQuery)
		}
		if len(args) != 1 {
			t.Fatalf("got %d args, want 1", len(args))
		}
		if args[0] != 100 {
			t.Errorf("args[0] = %v, want 100", args[0])
		}
	})
}

func TestBuildInsert(t *testing.T) {
	qb := &QueryBuilder{}

	t.Run("single row", func(t *testing.T) {
		req := connector.InsertRequest{
			Table: "USERS",
			Rows: []map[string]any{
				{"EMAIL": "a@b.com", "NAME": "Alice"},
			},
		}
		query, args := qb.BuildInsert(req)
		// Columns are sorted: EMAIL, NAME
		wantQuery := `INSERT INTO "USERS" ("EMAIL", "NAME") VALUES (?, ?)`
		if query != wantQuery {
			t.Errorf("got query:\n  %q\nwant:\n  %q", query, wantQuery)
		}
		if len(args) != 2 {
			t.Fatalf("got %d args, want 2", len(args))
		}
		if args[0] != "a@b.com" {
			t.Errorf("args[0] = %v, want %q", args[0], "a@b.com")
		}
		if args[1] != "Alice" {
			t.Errorf("args[1] = %v, want %q", args[1], "Alice")
		}
	})

	t.Run("multi row", func(t *testing.T) {
		req := connector.InsertRequest{
			Table: "USERS",
			Rows: []map[string]any{
				{"EMAIL": "a@b.com", "NAME": "Alice"},
				{"EMAIL": "c@d.com", "NAME": "Bob"},
			},
		}
		query, args := qb.BuildInsert(req)
		wantQuery := `INSERT INTO "USERS" ("EMAIL", "NAME") VALUES (?, ?), (?, ?)`
		if query != wantQuery {
			t.Errorf("got query:\n  %q\nwant:\n  %q", query, wantQuery)
		}
		if len(args) != 4 {
			t.Fatalf("got %d args, want 4", len(args))
		}
	})

	t.Run("deterministic column order", func(t *testing.T) {
		// Columns should be sorted alphabetically regardless of insertion order.
		req := connector.InsertRequest{
			Table: "USERS",
			Rows: []map[string]any{
				{"ZEBRA": "z", "ALPHA": "a", "MIDDLE": "m"},
			},
		}
		query, args := qb.BuildInsert(req)
		wantQuery := `INSERT INTO "USERS" ("ALPHA", "MIDDLE", "ZEBRA") VALUES (?, ?, ?)`
		if query != wantQuery {
			t.Errorf("got query:\n  %q\nwant:\n  %q", query, wantQuery)
		}
		if len(args) != 3 {
			t.Fatalf("got %d args, want 3", len(args))
		}
		if args[0] != "a" {
			t.Errorf("args[0] = %v, want %q", args[0], "a")
		}
		if args[1] != "m" {
			t.Errorf("args[1] = %v, want %q", args[1], "m")
		}
		if args[2] != "z" {
			t.Errorf("args[2] = %v, want %q", args[2], "z")
		}
	})

	t.Run("sparse rows use DEFAULT", func(t *testing.T) {
		req := connector.InsertRequest{
			Table: "USERS",
			Rows: []map[string]any{
				{"EMAIL": "a@b.com", "NAME": "Alice"},
				{"EMAIL": "c@d.com"},
			},
		}
		query, args := qb.BuildInsert(req)
		// Second row has no "NAME" so it should use DEFAULT
		wantQuery := `INSERT INTO "USERS" ("EMAIL", "NAME") VALUES (?, ?), (?, DEFAULT)`
		if query != wantQuery {
			t.Errorf("got query:\n  %q\nwant:\n  %q", query, wantQuery)
		}
		if len(args) != 3 {
			t.Fatalf("got %d args, want 3", len(args))
		}
	})

	t.Run("empty rows", func(t *testing.T) {
		req := connector.InsertRequest{
			Table: "USERS",
			Rows:  []map[string]any{},
		}
		query, args := qb.BuildInsert(req)
		if query != "" {
			t.Errorf("got query %q, want empty string", query)
		}
		if args != nil {
			t.Errorf("got args %v, want nil", args)
		}
	})
}

func TestBuildUpdate(t *testing.T) {
	qb := &QueryBuilder{}

	t.Run("basic update", func(t *testing.T) {
		req := connector.UpdateRequest{
			Table:  "USERS",
			Set:    map[string]any{"EMAIL": "new@b.com", "NAME": "Bob"},
			Filter: "ID = 1",
		}
		query, args := qb.BuildUpdate(req)
		// SET columns are sorted: EMAIL, NAME
		wantQuery := `UPDATE "USERS" SET "EMAIL" = ?, "NAME" = ? WHERE ID = 1`
		if query != wantQuery {
			t.Errorf("got query:\n  %q\nwant:\n  %q", query, wantQuery)
		}
		if len(args) != 2 {
			t.Fatalf("got %d args, want 2", len(args))
		}
		if args[0] != "new@b.com" {
			t.Errorf("args[0] = %v, want %q", args[0], "new@b.com")
		}
		if args[1] != "Bob" {
			t.Errorf("args[1] = %v, want %q", args[1], "Bob")
		}
	})

	t.Run("multiple columns sorted", func(t *testing.T) {
		req := connector.UpdateRequest{
			Table:  "PRODUCTS",
			Set:    map[string]any{"PRICE": 9.99, "CATEGORY": "electronics", "ACTIVE": true},
			Filter: "ID = 42",
		}
		query, args := qb.BuildUpdate(req)
		// SET columns sorted: ACTIVE, CATEGORY, PRICE
		wantQuery := `UPDATE "PRODUCTS" SET "ACTIVE" = ?, "CATEGORY" = ?, "PRICE" = ? WHERE ID = 42`
		if query != wantQuery {
			t.Errorf("got query:\n  %q\nwant:\n  %q", query, wantQuery)
		}
		if len(args) != 3 {
			t.Fatalf("got %d args, want 3", len(args))
		}
		if args[0] != true {
			t.Errorf("args[0] = %v, want true", args[0])
		}
		if args[1] != "electronics" {
			t.Errorf("args[1] = %v, want %q", args[1], "electronics")
		}
		if args[2] != 9.99 {
			t.Errorf("args[2] = %v, want 9.99", args[2])
		}
	})

	t.Run("update without filter", func(t *testing.T) {
		req := connector.UpdateRequest{
			Table: "USERS",
			Set:   map[string]any{"ACTIVE": false},
		}
		query, args := qb.BuildUpdate(req)
		wantQuery := `UPDATE "USERS" SET "ACTIVE" = ?`
		if query != wantQuery {
			t.Errorf("got query %q, want %q", query, wantQuery)
		}
		if len(args) != 1 {
			t.Fatalf("got %d args, want 1", len(args))
		}
	})
}

func TestBuildDelete(t *testing.T) {
	qb := &QueryBuilder{}

	t.Run("with filter", func(t *testing.T) {
		req := connector.DeleteRequest{
			Table:  "USERS",
			Filter: "ID = 1",
		}
		query, args := qb.BuildDelete(req)
		wantQuery := `DELETE FROM "USERS" WHERE ID = 1`
		if query != wantQuery {
			t.Errorf("got query %q, want %q", query, wantQuery)
		}
		if len(args) != 0 {
			t.Errorf("got %d args, want 0", len(args))
		}
	})

	t.Run("without filter", func(t *testing.T) {
		req := connector.DeleteRequest{
			Table: "USERS",
		}
		query, _ := qb.BuildDelete(req)
		wantQuery := `DELETE FROM "USERS"`
		if query != wantQuery {
			t.Errorf("got query %q, want %q", query, wantQuery)
		}
	})

	t.Run("schema-qualified table", func(t *testing.T) {
		req := connector.DeleteRequest{
			Table:  "PUBLIC.ORDERS",
			Filter: "STATUS = 'cancelled'",
		}
		query, args := qb.BuildDelete(req)
		wantQuery := `DELETE FROM "PUBLIC"."ORDERS" WHERE STATUS = 'cancelled'`
		if query != wantQuery {
			t.Errorf("got query %q, want %q", query, wantQuery)
		}
		if len(args) != 0 {
			t.Errorf("got %d args, want 0", len(args))
		}
	})
}

func TestBuildProcedureCall(t *testing.T) {
	qb := &QueryBuilder{}

	t.Run("no params", func(t *testing.T) {
		req := connector.ProcedureCallRequest{
			Name:   "PUBLIC.MY_PROC",
			Params: map[string]any{},
		}
		query, args := qb.BuildProcedureCall(req)
		wantQuery := `CALL "PUBLIC"."MY_PROC"()`
		if query != wantQuery {
			t.Errorf("got query %q, want %q", query, wantQuery)
		}
		if len(args) != 0 {
			t.Errorf("got %d args, want 0", len(args))
		}
	})

	t.Run("with params", func(t *testing.T) {
		req := connector.ProcedureCallRequest{
			Name: "GET_USER",
			Params: map[string]any{
				"user_id": 42,
			},
		}
		query, args := qb.BuildProcedureCall(req)
		wantQuery := `CALL "GET_USER"(?)` // Snowflake uses ? placeholders
		if query != wantQuery {
			t.Errorf("got query %q, want %q", query, wantQuery)
		}
		if len(args) != 1 {
			t.Fatalf("got %d args, want 1", len(args))
		}
		if args[0] != 42 {
			t.Errorf("args[0] = %v, want 42", args[0])
		}
	})

	t.Run("multiple params sorted", func(t *testing.T) {
		req := connector.ProcedureCallRequest{
			Name: "UPDATE_USER",
			Params: map[string]any{
				"name":    "Alice",
				"age":     30,
				"user_id": 1,
			},
		}
		query, args := qb.BuildProcedureCall(req)
		// Params sorted alphabetically: age, name, user_id
		wantQuery := `CALL "UPDATE_USER"(?, ?, ?)`
		if query != wantQuery {
			t.Errorf("got query %q, want %q", query, wantQuery)
		}
		if len(args) != 3 {
			t.Fatalf("got %d args, want 3", len(args))
		}
		if args[0] != 30 {
			t.Errorf("args[0] = %v, want 30 (age)", args[0])
		}
		if args[1] != "Alice" {
			t.Errorf("args[1] = %v, want %q (name)", args[1], "Alice")
		}
		if args[2] != 1 {
			t.Errorf("args[2] = %v, want 1 (user_id)", args[2])
		}
	})
}
