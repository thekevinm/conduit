package mysql

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
		{"users", "`users`"},
		{"mydb.users", "`mydb`.`users`"},
		{"my schema.my table", "`my schema`.`my table`"},
		{"has`backtick", "`has``backtick`"},
		{"sch`ema.tab`le", "`sch``ema`.`tab``le`"},
		{"simple", "`simple`"},
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
			Table: "users",
		}
		query, args := qb.BuildSelect(req)
		wantQuery := "SELECT * FROM `users`"
		if query != wantQuery {
			t.Errorf("got query %q, want %q", query, wantQuery)
		}
		if len(args) != 0 {
			t.Errorf("got %d args, want 0", len(args))
		}
	})

	t.Run("select with columns", func(t *testing.T) {
		req := connector.SelectRequest{
			Table:   "mydb.users",
			Columns: []string{"id", "name", "email"},
		}
		query, args := qb.BuildSelect(req)
		wantQuery := "SELECT `id`, `name`, `email` FROM `mydb`.`users`"
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
		wantQuery := "SELECT * FROM `orders` WHERE status = 'active'"
		if query != wantQuery {
			t.Errorf("got query %q, want %q", query, wantQuery)
		}
		if len(args) != 0 {
			t.Errorf("got %d args, want 0", len(args))
		}
	})

	t.Run("select with order", func(t *testing.T) {
		req := connector.SelectRequest{
			Table:   "products",
			OrderBy: "price DESC",
		}
		query, args := qb.BuildSelect(req)
		wantQuery := "SELECT * FROM `products` ORDER BY price DESC"
		if query != wantQuery {
			t.Errorf("got query %q, want %q", query, wantQuery)
		}
		if len(args) != 0 {
			t.Errorf("got %d args, want 0", len(args))
		}
	})

	t.Run("select with limit and offset", func(t *testing.T) {
		req := connector.SelectRequest{
			Table:  "items",
			Limit:  25,
			Offset: 50,
		}
		query, args := qb.BuildSelect(req)
		wantQuery := "SELECT * FROM `items` LIMIT ? OFFSET ?"
		if query != wantQuery {
			t.Errorf("got query %q, want %q", query, wantQuery)
		}
		if len(args) != 2 {
			t.Fatalf("got %d args, want 2", len(args))
		}
		if args[0] != 25 {
			t.Errorf("args[0] = %v, want 25", args[0])
		}
		if args[1] != 50 {
			t.Errorf("args[1] = %v, want 50", args[1])
		}
	})

	t.Run("full query", func(t *testing.T) {
		req := connector.SelectRequest{
			Table:   "orders",
			Columns: []string{"id", "total"},
			Filter:  "status = 'active'",
			OrderBy: "created_at DESC",
			Limit:   50,
			Offset:  10,
		}
		query, args := qb.BuildSelect(req)
		wantQuery := "SELECT `id`, `total` FROM `orders` WHERE status = 'active' ORDER BY created_at DESC LIMIT ? OFFSET ?"
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
		wantQuery := "INSERT INTO `users` (`email`, `name`) VALUES (?, ?)"
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
		wantQuery := "INSERT INTO `users` (`email`, `name`) VALUES (?, ?), (?, ?)"
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
				{"zebra": "z", "alpha": "a", "middle": "m"},
			},
		}
		query, args := qb.BuildInsert(req)
		// Columns must be sorted alphabetically: alpha, middle, zebra
		wantQuery := "INSERT INTO `users` (`alpha`, `middle`, `zebra`) VALUES (?, ?, ?)"
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
			Table: "users",
			Rows: []map[string]any{
				{"email": "a@b.com", "name": "Alice"},
				{"email": "c@d.com"},
			},
		}
		query, args := qb.BuildInsert(req)
		// Second row has no "name" so it should use DEFAULT
		wantQuery := "INSERT INTO `users` (`email`, `name`) VALUES (?, ?), (?, DEFAULT)"
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
		wantQuery := "UPDATE `users` SET `email` = ?, `name` = ? WHERE id = 1"
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

	t.Run("multiple columns sorted", func(t *testing.T) {
		req := connector.UpdateRequest{
			Table:  "products",
			Set:    map[string]any{"stock": 100, "name": "Widget", "active": true},
			Filter: "id = 5",
		}
		query, args := qb.BuildUpdate(req)
		// SET columns sorted: active, name, stock
		wantQuery := "UPDATE `products` SET `active` = ?, `name` = ?, `stock` = ? WHERE id = 5"
		if query != wantQuery {
			t.Errorf("got query:\n  %q\nwant:\n  %q", query, wantQuery)
		}
		if len(args) != 3 {
			t.Fatalf("got %d args, want 3", len(args))
		}
		if args[0] != true {
			t.Errorf("args[0] = %v, want true", args[0])
		}
		if args[1] != "Widget" {
			t.Errorf("args[1] = %v, want %q", args[1], "Widget")
		}
		if args[2] != 100 {
			t.Errorf("args[2] = %v, want 100", args[2])
		}
	})
}

func TestBuildDelete(t *testing.T) {
	qb := &QueryBuilder{}

	t.Run("with filter", func(t *testing.T) {
		req := connector.DeleteRequest{
			Table:  "users",
			Filter: "id = 1",
		}
		query, args := qb.BuildDelete(req)
		wantQuery := "DELETE FROM `users` WHERE id = 1"
		if query != wantQuery {
			t.Errorf("got query %q, want %q", query, wantQuery)
		}
		if len(args) != 0 {
			t.Errorf("got %d args, want 0", len(args))
		}
	})

	t.Run("without filter", func(t *testing.T) {
		req := connector.DeleteRequest{
			Table: "users",
		}
		query, _ := qb.BuildDelete(req)
		wantQuery := "DELETE FROM `users`"
		if query != wantQuery {
			t.Errorf("got query %q, want %q", query, wantQuery)
		}
	})
}

func TestBuildProcedureCall(t *testing.T) {
	qb := &QueryBuilder{}

	t.Run("no params", func(t *testing.T) {
		req := connector.ProcedureCallRequest{
			Name:   "my_proc",
			Params: map[string]any{},
		}
		query, args := qb.BuildProcedureCall(req)
		wantQuery := "CALL `my_proc`()"
		if query != wantQuery {
			t.Errorf("got query %q, want %q", query, wantQuery)
		}
		if len(args) != 0 {
			t.Errorf("got %d args, want 0", len(args))
		}
	})

	t.Run("with params sorted", func(t *testing.T) {
		req := connector.ProcedureCallRequest{
			Name: "get_user",
			Params: map[string]any{
				"user_id": 42,
				"active":  true,
			},
		}
		query, args := qb.BuildProcedureCall(req)
		// Params sorted: active, user_id
		wantQuery := "CALL `get_user`(?, ?)"
		if query != wantQuery {
			t.Errorf("got query %q, want %q", query, wantQuery)
		}
		if len(args) != 2 {
			t.Fatalf("got %d args, want 2", len(args))
		}
		if args[0] != true {
			t.Errorf("args[0] = %v, want true", args[0])
		}
		if args[1] != 42 {
			t.Errorf("args[1] = %v, want 42", args[1])
		}
	})

	t.Run("schema-qualified name", func(t *testing.T) {
		req := connector.ProcedureCallRequest{
			Name:   "mydb.my_func",
			Params: map[string]any{},
		}
		query, args := qb.BuildProcedureCall(req)
		wantQuery := "CALL `mydb`.`my_func`()"
		if query != wantQuery {
			t.Errorf("got query %q, want %q", query, wantQuery)
		}
		if len(args) != 0 {
			t.Errorf("got %d args, want 0", len(args))
		}
	})
}
