package postgres

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
		{"public.users", `"public"."users"`},
		{"my schema.my table", `"my schema"."my table"`},
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

func TestParameterPlaceholder(t *testing.T) {
	qb := &QueryBuilder{}

	tests := []struct {
		index int
		want  string
	}{
		{1, "$1"},
		{2, "$2"},
		{10, "$10"},
		{100, "$100"},
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
			Table: "users",
		}
		query, args := qb.BuildSelect(req)
		wantQuery := `SELECT * FROM "users"`
		if query != wantQuery {
			t.Errorf("got query %q, want %q", query, wantQuery)
		}
		if len(args) != 0 {
			t.Errorf("got %d args, want 0", len(args))
		}
	})

	t.Run("select with columns", func(t *testing.T) {
		req := connector.SelectRequest{
			Table:   "public.users",
			Columns: []string{"id", "name", "email"},
		}
		query, args := qb.BuildSelect(req)
		wantQuery := `SELECT "id", "name", "email" FROM "public"."users"`
		if query != wantQuery {
			t.Errorf("got query %q, want %q", query, wantQuery)
		}
		if len(args) != 0 {
			t.Errorf("got %d args, want 0", len(args))
		}
	})

	t.Run("select with filter and limit", func(t *testing.T) {
		req := connector.SelectRequest{
			Table:   "orders",
			Filter:  "status = 'active'",
			OrderBy: "created_at DESC",
			Limit:   50,
			Offset:  10,
		}
		query, args := qb.BuildSelect(req)
		wantQuery := `SELECT * FROM "orders" WHERE status = 'active' ORDER BY created_at DESC LIMIT $1 OFFSET $2`
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
}

func TestBuildInsert(t *testing.T) {
	qb := &QueryBuilder{}

	t.Run("single row", func(t *testing.T) {
		req := connector.InsertRequest{
			Table: "users",
			Rows: []map[string]any{
				{"email": "a@b.com", "name": "Alice"},
			},
		}
		query, args := qb.BuildInsert(req)
		// Columns are sorted: email, name
		wantQuery := `INSERT INTO "users" ("email", "name") VALUES ($1, $2)`
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
			Table: "users",
			Rows: []map[string]any{
				{"email": "a@b.com", "name": "Alice"},
				{"email": "c@d.com", "name": "Bob"},
			},
		}
		query, args := qb.BuildInsert(req)
		wantQuery := `INSERT INTO "users" ("email", "name") VALUES ($1, $2), ($3, $4)`
		if query != wantQuery {
			t.Errorf("got query:\n  %q\nwant:\n  %q", query, wantQuery)
		}
		if len(args) != 4 {
			t.Fatalf("got %d args, want 4", len(args))
		}
	})

	t.Run("sparse rows use DEFAULT", func(t *testing.T) {
		req := connector.InsertRequest{
			Table: "users",
			Rows: []map[string]any{
				{"email": "a@b.com", "name": "Alice"},
				{"email": "c@d.com"},
			},
		}
		query, args := qb.BuildInsert(req)
		// Second row has no "name" so it should use DEFAULT
		wantQuery := `INSERT INTO "users" ("email", "name") VALUES ($1, $2), ($3, DEFAULT)`
		if query != wantQuery {
			t.Errorf("got query:\n  %q\nwant:\n  %q", query, wantQuery)
		}
		if len(args) != 3 {
			t.Fatalf("got %d args, want 3", len(args))
		}
	})

	t.Run("empty rows", func(t *testing.T) {
		req := connector.InsertRequest{
			Table: "users",
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
			Table:  "users",
			Set:    map[string]any{"email": "new@b.com", "name": "Bob"},
			Filter: "id = 1",
		}
		query, args := qb.BuildUpdate(req)
		// SET columns are sorted: email, name
		wantQuery := `UPDATE "users" SET "email" = $1, "name" = $2 WHERE id = 1`
		if query != wantQuery {
			t.Errorf("got query:\n  %q\nwant:\n  %q", query, wantQuery)
		}
		if len(args) != 2 {
			t.Fatalf("got %d args, want 2", len(args))
		}
		if args[0] != "new@b.com" {
			t.Errorf("args[0] = %v, want %q", args[0], "new@b.com")
		}
	})
}

func TestBuildDelete(t *testing.T) {
	qb := &QueryBuilder{}

	t.Run("basic delete", func(t *testing.T) {
		req := connector.DeleteRequest{
			Table:  "users",
			Filter: "id = 1",
		}
		query, args := qb.BuildDelete(req)
		wantQuery := `DELETE FROM "users" WHERE id = 1`
		if query != wantQuery {
			t.Errorf("got query %q, want %q", query, wantQuery)
		}
		if len(args) != 0 {
			t.Errorf("got %d args, want 0", len(args))
		}
	})

	t.Run("delete without filter", func(t *testing.T) {
		req := connector.DeleteRequest{
			Table: "users",
		}
		query, _ := qb.BuildDelete(req)
		wantQuery := `DELETE FROM "users"`
		if query != wantQuery {
			t.Errorf("got query %q, want %q", query, wantQuery)
		}
	})
}

func TestBuildProcedureCall(t *testing.T) {
	qb := &QueryBuilder{}

	t.Run("no params", func(t *testing.T) {
		req := connector.ProcedureCallRequest{
			Name:   "public.my_func",
			Params: map[string]any{},
		}
		query, args := qb.BuildProcedureCall(req)
		wantQuery := `SELECT * FROM "public"."my_func"()`
		if query != wantQuery {
			t.Errorf("got query %q, want %q", query, wantQuery)
		}
		if len(args) != 0 {
			t.Errorf("got %d args, want 0", len(args))
		}
	})

	t.Run("with params", func(t *testing.T) {
		req := connector.ProcedureCallRequest{
			Name: "get_user",
			Params: map[string]any{
				"user_id": 42,
			},
		}
		query, args := qb.BuildProcedureCall(req)
		wantQuery := `SELECT * FROM "get_user"("user_id" := $1)`
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
}

func TestSplitTableName(t *testing.T) {
	tests := []struct {
		input      string
		wantSchema string
		wantName   string
	}{
		{"users", "public", "users"},
		{"public.users", "public", "users"},
		{"myschema.orders", "myschema", "orders"},
		{"a.b.c", "a", "b.c"}, // Only first dot splits
	}

	for _, tt := range tests {
		t.Run(tt.input, func(t *testing.T) {
			schema, name := splitTableName(tt.input)
			if schema != tt.wantSchema {
				t.Errorf("splitTableName(%q) schema = %q, want %q", tt.input, schema, tt.wantSchema)
			}
			if name != tt.wantName {
				t.Errorf("splitTableName(%q) name = %q, want %q", tt.input, name, tt.wantName)
			}
		})
	}
}
