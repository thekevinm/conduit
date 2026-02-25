package mssql

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
		{"users", "[users]"},
		{"dbo.users", "[dbo].[users]"},
		{"my schema.my table", "[my schema].[my table]"},
		{"has]bracket", "[has]]bracket]"},
		{"sch]ema.tab]le", "[sch]]ema].[tab]]le]"},
		{"simple", "[simple]"},
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
		{1, "@p1"},
		{2, "@p2"},
		{10, "@p10"},
		{100, "@p100"},
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
		wantQuery := `SELECT * FROM [users]`
		if query != wantQuery {
			t.Errorf("got query %q, want %q", query, wantQuery)
		}
		if len(args) != 0 {
			t.Errorf("got %d args, want 0", len(args))
		}
	})

	t.Run("select with columns", func(t *testing.T) {
		req := connector.SelectRequest{
			Table:   "dbo.users",
			Columns: []string{"id", "name", "email"},
		}
		query, args := qb.BuildSelect(req)
		wantQuery := `SELECT [id], [name], [email] FROM [dbo].[users]`
		if query != wantQuery {
			t.Errorf("got query %q, want %q", query, wantQuery)
		}
		if len(args) != 0 {
			t.Errorf("got %d args, want 0", len(args))
		}
	})

	t.Run("select with filter", func(t *testing.T) {
		req := connector.SelectRequest{
			Table:  "orders",
			Filter: "status = 'active'",
		}
		query, args := qb.BuildSelect(req)
		wantQuery := `SELECT * FROM [orders] WHERE status = 'active'`
		if query != wantQuery {
			t.Errorf("got query %q, want %q", query, wantQuery)
		}
		if len(args) != 0 {
			t.Errorf("got %d args, want 0", len(args))
		}
	})

	t.Run("select with order by", func(t *testing.T) {
		req := connector.SelectRequest{
			Table:   "orders",
			OrderBy: "created_at DESC",
		}
		query, args := qb.BuildSelect(req)
		wantQuery := `SELECT * FROM [orders] ORDER BY created_at DESC`
		if query != wantQuery {
			t.Errorf("got query %q, want %q", query, wantQuery)
		}
		if len(args) != 0 {
			t.Errorf("got %d args, want 0", len(args))
		}
	})

	t.Run("select with limit and offset uses OFFSET FETCH NEXT", func(t *testing.T) {
		req := connector.SelectRequest{
			Table:   "orders",
			Filter:  "status = 'active'",
			OrderBy: "created_at DESC",
			Limit:   50,
			Offset:  10,
		}
		query, args := qb.BuildSelect(req)
		wantQuery := `SELECT * FROM [orders] WHERE status = 'active' ORDER BY created_at DESC OFFSET @p1 ROWS FETCH NEXT @p2 ROWS ONLY`
		if query != wantQuery {
			t.Errorf("got query:\n  %q\nwant:\n  %q", query, wantQuery)
		}
		if len(args) != 2 {
			t.Fatalf("got %d args, want 2", len(args))
		}
		if args[0] != 10 {
			t.Errorf("args[0] = %v, want 10 (offset)", args[0])
		}
		if args[1] != 50 {
			t.Errorf("args[1] = %v, want 50 (limit)", args[1])
		}
	})

	t.Run("pagination without ORDER BY adds ORDER BY SELECT NULL", func(t *testing.T) {
		req := connector.SelectRequest{
			Table:  "users",
			Limit:  20,
			Offset: 0,
		}
		query, args := qb.BuildSelect(req)
		wantQuery := `SELECT * FROM [users] ORDER BY (SELECT NULL) OFFSET @p1 ROWS FETCH NEXT @p2 ROWS ONLY`
		if query != wantQuery {
			t.Errorf("got query:\n  %q\nwant:\n  %q", query, wantQuery)
		}
		if len(args) != 2 {
			t.Fatalf("got %d args, want 2", len(args))
		}
		if args[0] != 0 {
			t.Errorf("args[0] = %v, want 0 (offset)", args[0])
		}
		if args[1] != 20 {
			t.Errorf("args[1] = %v, want 20 (limit)", args[1])
		}
	})

	t.Run("offset only without ORDER BY adds ORDER BY SELECT NULL", func(t *testing.T) {
		req := connector.SelectRequest{
			Table:  "users",
			Offset: 5,
		}
		query, args := qb.BuildSelect(req)
		wantQuery := `SELECT * FROM [users] ORDER BY (SELECT NULL) OFFSET @p1 ROWS`
		if query != wantQuery {
			t.Errorf("got query:\n  %q\nwant:\n  %q", query, wantQuery)
		}
		if len(args) != 1 {
			t.Fatalf("got %d args, want 1", len(args))
		}
		if args[0] != 5 {
			t.Errorf("args[0] = %v, want 5 (offset)", args[0])
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
		wantQuery := `INSERT INTO [users] ([email], [name]) VALUES (@p1, @p2)`
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
		wantQuery := `INSERT INTO [users] ([email], [name]) VALUES (@p1, @p2), (@p3, @p4)`
		if query != wantQuery {
			t.Errorf("got query:\n  %q\nwant:\n  %q", query, wantQuery)
		}
		if len(args) != 4 {
			t.Fatalf("got %d args, want 4", len(args))
		}
	})

	t.Run("deterministic column order", func(t *testing.T) {
		req := connector.InsertRequest{
			Table: "users",
			Rows: []map[string]any{
				{"zebra": 1, "alpha": 2, "middle": 3},
			},
		}
		query, args := qb.BuildInsert(req)
		// Columns sorted: alpha, middle, zebra
		wantQuery := `INSERT INTO [users] ([alpha], [middle], [zebra]) VALUES (@p1, @p2, @p3)`
		if query != wantQuery {
			t.Errorf("got query:\n  %q\nwant:\n  %q", query, wantQuery)
		}
		if len(args) != 3 {
			t.Fatalf("got %d args, want 3", len(args))
		}
		if args[0] != 2 {
			t.Errorf("args[0] = %v, want 2 (alpha)", args[0])
		}
		if args[1] != 3 {
			t.Errorf("args[1] = %v, want 3 (middle)", args[1])
		}
		if args[2] != 1 {
			t.Errorf("args[2] = %v, want 1 (zebra)", args[2])
		}
	})

	t.Run("sparse rows use DEFAULT for missing columns", func(t *testing.T) {
		req := connector.InsertRequest{
			Table: "users",
			Rows: []map[string]any{
				{"email": "a@b.com", "name": "Alice"},
				{"email": "c@d.com"},
			},
		}
		query, args := qb.BuildInsert(req)
		// Second row has no "name" so it should use DEFAULT
		wantQuery := `INSERT INTO [users] ([email], [name]) VALUES (@p1, @p2), (@p3, DEFAULT)`
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
		wantQuery := `UPDATE [users] SET [email] = @p1, [name] = @p2 WHERE id = 1`
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

	t.Run("multiple columns sorted deterministically", func(t *testing.T) {
		req := connector.UpdateRequest{
			Table:  "products",
			Set:    map[string]any{"zebra": "z", "alpha": "a", "middle": "m"},
			Filter: "id = 5",
		}
		query, args := qb.BuildUpdate(req)
		// SET columns sorted: alpha, middle, zebra
		wantQuery := `UPDATE [products] SET [alpha] = @p1, [middle] = @p2, [zebra] = @p3 WHERE id = 5`
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

	t.Run("update without filter", func(t *testing.T) {
		req := connector.UpdateRequest{
			Table: "users",
			Set:   map[string]any{"active": false},
		}
		query, args := qb.BuildUpdate(req)
		wantQuery := `UPDATE [users] SET [active] = @p1`
		if query != wantQuery {
			t.Errorf("got query %q, want %q", query, wantQuery)
		}
		if len(args) != 1 {
			t.Fatalf("got %d args, want 1", len(args))
		}
		if args[0] != false {
			t.Errorf("args[0] = %v, want false", args[0])
		}
	})
}

func TestBuildDelete(t *testing.T) {
	qb := &QueryBuilder{}

	t.Run("delete with filter", func(t *testing.T) {
		req := connector.DeleteRequest{
			Table:  "users",
			Filter: "id = 1",
		}
		query, args := qb.BuildDelete(req)
		wantQuery := `DELETE FROM [users] WHERE id = 1`
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
		wantQuery := `DELETE FROM [users]`
		if query != wantQuery {
			t.Errorf("got query %q, want %q", query, wantQuery)
		}
	})
}

func TestBuildProcedureCall(t *testing.T) {
	qb := &QueryBuilder{}

	t.Run("no params", func(t *testing.T) {
		req := connector.ProcedureCallRequest{
			Name:   "dbo.my_proc",
			Params: map[string]any{},
		}
		query, args := qb.BuildProcedureCall(req)
		wantQuery := `EXEC [dbo].[my_proc]`
		if query != wantQuery {
			t.Errorf("got query %q, want %q", query, wantQuery)
		}
		if len(args) != 0 {
			t.Errorf("got %d args, want 0", len(args))
		}
	})

	t.Run("with single param", func(t *testing.T) {
		req := connector.ProcedureCallRequest{
			Name: "get_user",
			Params: map[string]any{
				"user_id": 42,
			},
		}
		query, args := qb.BuildProcedureCall(req)
		wantQuery := `EXEC [get_user] @user_id = @p1`
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

	t.Run("with multiple params sorted", func(t *testing.T) {
		req := connector.ProcedureCallRequest{
			Name: "dbo.update_user",
			Params: map[string]any{
				"name":    "Bob",
				"email":   "bob@test.com",
				"user_id": 7,
			},
		}
		query, args := qb.BuildProcedureCall(req)
		// Params sorted: email, name, user_id
		wantQuery := `EXEC [dbo].[update_user] @email = @p1, @name = @p2, @user_id = @p3`
		if query != wantQuery {
			t.Errorf("got query:\n  %q\nwant:\n  %q", query, wantQuery)
		}
		if len(args) != 3 {
			t.Fatalf("got %d args, want 3", len(args))
		}
		if args[0] != "bob@test.com" {
			t.Errorf("args[0] = %v, want %q", args[0], "bob@test.com")
		}
		if args[1] != "Bob" {
			t.Errorf("args[1] = %v, want %q", args[1], "Bob")
		}
		if args[2] != 7 {
			t.Errorf("args[2] = %v, want 7", args[2])
		}
	})
}

func TestSplitTableName(t *testing.T) {
	tests := []struct {
		input      string
		wantSchema string
		wantName   string
	}{
		{"users", "dbo", "users"},
		{"dbo.users", "dbo", "users"},
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
