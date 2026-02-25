package oracle

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
		{"EMPLOYEES", `"EMPLOYEES"`},
		{"HR.EMPLOYEES", `"HR"."EMPLOYEES"`},
		{`has"quote`, `"has""quote"`},
		{`OWN"ER.TAB"LE`, `"OWN""ER"."TAB""LE"`},
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
		{1, ":1"},
		{2, ":2"},
		{10, ":10"},
		{100, ":100"},
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
			Table: "EMPLOYEES",
		}
		query, args := qb.BuildSelect(req)
		wantQuery := `SELECT * FROM "EMPLOYEES"`
		if query != wantQuery {
			t.Errorf("got query %q, want %q", query, wantQuery)
		}
		if len(args) != 0 {
			t.Errorf("got %d args, want 0", len(args))
		}
	})

	t.Run("select with columns", func(t *testing.T) {
		req := connector.SelectRequest{
			Table:   "HR.EMPLOYEES",
			Columns: []string{"EMPLOYEE_ID", "FIRST_NAME", "EMAIL"},
		}
		query, args := qb.BuildSelect(req)
		wantQuery := `SELECT "EMPLOYEE_ID", "FIRST_NAME", "EMAIL" FROM "HR"."EMPLOYEES"`
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
			Filter: "STATUS = 'ACTIVE'",
		}
		query, args := qb.BuildSelect(req)
		wantQuery := `SELECT * FROM "ORDERS" WHERE STATUS = 'ACTIVE'`
		if query != wantQuery {
			t.Errorf("got query %q, want %q", query, wantQuery)
		}
		if len(args) != 0 {
			t.Errorf("got %d args, want 0", len(args))
		}
	})

	t.Run("select with order", func(t *testing.T) {
		req := connector.SelectRequest{
			Table:   "EMPLOYEES",
			OrderBy: "HIRE_DATE DESC",
		}
		query, args := qb.BuildSelect(req)
		wantQuery := `SELECT * FROM "EMPLOYEES" ORDER BY HIRE_DATE DESC`
		if query != wantQuery {
			t.Errorf("got query %q, want %q", query, wantQuery)
		}
		if len(args) != 0 {
			t.Errorf("got %d args, want 0", len(args))
		}
	})

	t.Run("select with limit only", func(t *testing.T) {
		req := connector.SelectRequest{
			Table: "EMPLOYEES",
			Limit: 25,
		}
		query, args := qb.BuildSelect(req)
		wantQuery := `SELECT * FROM "EMPLOYEES" FETCH FIRST :1 ROWS ONLY`
		if query != wantQuery {
			t.Errorf("got query:\n  %q\nwant:\n  %q", query, wantQuery)
		}
		if len(args) != 1 {
			t.Fatalf("got %d args, want 1", len(args))
		}
		if args[0] != 25 {
			t.Errorf("args[0] = %v, want 25", args[0])
		}
	})

	t.Run("select with offset and limit", func(t *testing.T) {
		req := connector.SelectRequest{
			Table:   "ORDERS",
			Filter:  "STATUS = 'ACTIVE'",
			OrderBy: "CREATED_AT DESC",
			Limit:   50,
			Offset:  10,
		}
		query, args := qb.BuildSelect(req)
		wantQuery := `SELECT * FROM "ORDERS" WHERE STATUS = 'ACTIVE' ORDER BY CREATED_AT DESC OFFSET :1 ROWS FETCH FIRST :2 ROWS ONLY`
		if query != wantQuery {
			t.Errorf("got query:\n  %q\nwant:\n  %q", query, wantQuery)
		}
		if len(args) != 2 {
			t.Fatalf("got %d args, want 2", len(args))
		}
		if args[0] != 10 {
			t.Errorf("args[0] = %v, want 10", args[0])
		}
		if args[1] != 50 {
			t.Errorf("args[1] = %v, want 50", args[1])
		}
	})

	t.Run("select with offset only", func(t *testing.T) {
		req := connector.SelectRequest{
			Table:  "EMPLOYEES",
			Offset: 5,
		}
		query, args := qb.BuildSelect(req)
		wantQuery := `SELECT * FROM "EMPLOYEES" OFFSET :1 ROWS`
		if query != wantQuery {
			t.Errorf("got query:\n  %q\nwant:\n  %q", query, wantQuery)
		}
		if len(args) != 1 {
			t.Fatalf("got %d args, want 1", len(args))
		}
		if args[0] != 5 {
			t.Errorf("args[0] = %v, want 5", args[0])
		}
	})
}

func TestBuildInsert(t *testing.T) {
	qb := &QueryBuilder{}

	t.Run("single row", func(t *testing.T) {
		req := connector.InsertRequest{
			Table: "EMPLOYEES",
			Rows: []map[string]any{
				{"EMAIL": "alice@corp.com", "FIRST_NAME": "Alice"},
			},
		}
		query, args := qb.BuildInsert(req)
		// Columns are sorted: EMAIL, FIRST_NAME
		wantQuery := `INSERT INTO "EMPLOYEES" ("EMAIL", "FIRST_NAME") VALUES (:1, :2)`
		if query != wantQuery {
			t.Errorf("got query:\n  %q\nwant:\n  %q", query, wantQuery)
		}
		if len(args) != 2 {
			t.Fatalf("got %d args, want 2", len(args))
		}
		if args[0] != "alice@corp.com" {
			t.Errorf("args[0] = %v, want %q", args[0], "alice@corp.com")
		}
		if args[1] != "Alice" {
			t.Errorf("args[1] = %v, want %q", args[1], "Alice")
		}
	})

	t.Run("multi row uses INSERT ALL", func(t *testing.T) {
		req := connector.InsertRequest{
			Table: "EMPLOYEES",
			Rows: []map[string]any{
				{"EMAIL": "alice@corp.com", "FIRST_NAME": "Alice"},
				{"EMAIL": "bob@corp.com", "FIRST_NAME": "Bob"},
			},
		}
		query, args := qb.BuildInsert(req)
		wantQuery := `INSERT ALL INTO "EMPLOYEES" ("EMAIL", "FIRST_NAME") VALUES (:1, :2) INTO "EMPLOYEES" ("EMAIL", "FIRST_NAME") VALUES (:3, :4) SELECT 1 FROM DUAL`
		if query != wantQuery {
			t.Errorf("got query:\n  %q\nwant:\n  %q", query, wantQuery)
		}
		if len(args) != 4 {
			t.Fatalf("got %d args, want 4", len(args))
		}
		if args[0] != "alice@corp.com" {
			t.Errorf("args[0] = %v, want %q", args[0], "alice@corp.com")
		}
		if args[1] != "Alice" {
			t.Errorf("args[1] = %v, want %q", args[1], "Alice")
		}
		if args[2] != "bob@corp.com" {
			t.Errorf("args[2] = %v, want %q", args[2], "bob@corp.com")
		}
		if args[3] != "Bob" {
			t.Errorf("args[3] = %v, want %q", args[3], "Bob")
		}
	})

	t.Run("deterministic column order", func(t *testing.T) {
		req := connector.InsertRequest{
			Table: "EMPLOYEES",
			Rows: []map[string]any{
				{"ZEBRA": "z", "ALPHA": "a", "MIDDLE": "m"},
			},
		}
		query, args := qb.BuildInsert(req)
		// Columns must be sorted alphabetically: ALPHA, MIDDLE, ZEBRA
		wantQuery := `INSERT INTO "EMPLOYEES" ("ALPHA", "MIDDLE", "ZEBRA") VALUES (:1, :2, :3)`
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
			Table: "EMPLOYEES",
			Rows: []map[string]any{
				{"EMAIL": "alice@corp.com", "FIRST_NAME": "Alice"},
				{"EMAIL": "bob@corp.com"},
			},
		}
		query, args := qb.BuildInsert(req)
		// Second row has no FIRST_NAME so it should use DEFAULT
		wantQuery := `INSERT ALL INTO "EMPLOYEES" ("EMAIL", "FIRST_NAME") VALUES (:1, :2) INTO "EMPLOYEES" ("EMAIL", "FIRST_NAME") VALUES (:3, DEFAULT) SELECT 1 FROM DUAL`
		if query != wantQuery {
			t.Errorf("got query:\n  %q\nwant:\n  %q", query, wantQuery)
		}
		if len(args) != 3 {
			t.Fatalf("got %d args, want 3", len(args))
		}
	})

	t.Run("empty rows", func(t *testing.T) {
		req := connector.InsertRequest{
			Table: "EMPLOYEES",
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
			Table:  "EMPLOYEES",
			Set:    map[string]any{"EMAIL": "new@corp.com", "FIRST_NAME": "Bob"},
			Filter: "EMPLOYEE_ID = 1",
		}
		query, args := qb.BuildUpdate(req)
		// SET columns are sorted: EMAIL, FIRST_NAME
		wantQuery := `UPDATE "EMPLOYEES" SET "EMAIL" = :1, "FIRST_NAME" = :2 WHERE EMPLOYEE_ID = 1`
		if query != wantQuery {
			t.Errorf("got query:\n  %q\nwant:\n  %q", query, wantQuery)
		}
		if len(args) != 2 {
			t.Fatalf("got %d args, want 2", len(args))
		}
		if args[0] != "new@corp.com" {
			t.Errorf("args[0] = %v, want %q", args[0], "new@corp.com")
		}
		if args[1] != "Bob" {
			t.Errorf("args[1] = %v, want %q", args[1], "Bob")
		}
	})

	t.Run("multiple columns sorted", func(t *testing.T) {
		req := connector.UpdateRequest{
			Table:  "EMPLOYEES",
			Set:    map[string]any{"SALARY": 50000, "DEPARTMENT_ID": 10, "COMMISSION": 0.1},
			Filter: "EMPLOYEE_ID = 42",
		}
		query, args := qb.BuildUpdate(req)
		// Sorted: COMMISSION, DEPARTMENT_ID, SALARY
		wantQuery := `UPDATE "EMPLOYEES" SET "COMMISSION" = :1, "DEPARTMENT_ID" = :2, "SALARY" = :3 WHERE EMPLOYEE_ID = 42`
		if query != wantQuery {
			t.Errorf("got query:\n  %q\nwant:\n  %q", query, wantQuery)
		}
		if len(args) != 3 {
			t.Fatalf("got %d args, want 3", len(args))
		}
		if args[0] != 0.1 {
			t.Errorf("args[0] = %v, want 0.1", args[0])
		}
		if args[1] != 10 {
			t.Errorf("args[1] = %v, want 10", args[1])
		}
		if args[2] != 50000 {
			t.Errorf("args[2] = %v, want 50000", args[2])
		}
	})

	t.Run("update without filter", func(t *testing.T) {
		req := connector.UpdateRequest{
			Table: "EMPLOYEES",
			Set:   map[string]any{"STATUS": "INACTIVE"},
		}
		query, _ := qb.BuildUpdate(req)
		wantQuery := `UPDATE "EMPLOYEES" SET "STATUS" = :1`
		if query != wantQuery {
			t.Errorf("got query %q, want %q", query, wantQuery)
		}
	})
}

func TestBuildDelete(t *testing.T) {
	qb := &QueryBuilder{}

	t.Run("delete with filter", func(t *testing.T) {
		req := connector.DeleteRequest{
			Table:  "EMPLOYEES",
			Filter: "EMPLOYEE_ID = 1",
		}
		query, args := qb.BuildDelete(req)
		wantQuery := `DELETE FROM "EMPLOYEES" WHERE EMPLOYEE_ID = 1`
		if query != wantQuery {
			t.Errorf("got query %q, want %q", query, wantQuery)
		}
		if len(args) != 0 {
			t.Errorf("got %d args, want 0", len(args))
		}
	})

	t.Run("delete without filter", func(t *testing.T) {
		req := connector.DeleteRequest{
			Table: "EMPLOYEES",
		}
		query, _ := qb.BuildDelete(req)
		wantQuery := `DELETE FROM "EMPLOYEES"`
		if query != wantQuery {
			t.Errorf("got query %q, want %q", query, wantQuery)
		}
	})
}

func TestBuildProcedureCall(t *testing.T) {
	qb := &QueryBuilder{}

	t.Run("no params", func(t *testing.T) {
		req := connector.ProcedureCallRequest{
			Name:   "HR.MY_PROC",
			Params: map[string]any{},
		}
		query, args := qb.BuildProcedureCall(req)
		wantQuery := `BEGIN "HR"."MY_PROC"(); END;`
		if query != wantQuery {
			t.Errorf("got query %q, want %q", query, wantQuery)
		}
		if len(args) != 0 {
			t.Errorf("got %d args, want 0", len(args))
		}
	})

	t.Run("with params uses => syntax", func(t *testing.T) {
		req := connector.ProcedureCallRequest{
			Name: "GET_EMPLOYEE",
			Params: map[string]any{
				"P_EMP_ID": 42,
			},
		}
		query, args := qb.BuildProcedureCall(req)
		wantQuery := `BEGIN "GET_EMPLOYEE"("P_EMP_ID" => :1); END;`
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
			Name: "UPDATE_SALARY",
			Params: map[string]any{
				"P_SALARY": 75000,
				"P_EMP_ID": 10,
			},
		}
		query, args := qb.BuildProcedureCall(req)
		// Params sorted: P_EMP_ID, P_SALARY
		wantQuery := `BEGIN "UPDATE_SALARY"("P_EMP_ID" => :1, "P_SALARY" => :2); END;`
		if query != wantQuery {
			t.Errorf("got query %q, want %q", query, wantQuery)
		}
		if len(args) != 2 {
			t.Fatalf("got %d args, want 2", len(args))
		}
		if args[0] != 10 {
			t.Errorf("args[0] = %v, want 10", args[0])
		}
		if args[1] != 75000 {
			t.Errorf("args[1] = %v, want 75000", args[1])
		}
	})
}

func TestSplitTableName(t *testing.T) {
	tests := []struct {
		input        string
		defaultOwner string
		wantOwner    string
		wantName     string
	}{
		{"EMPLOYEES", "HR", "HR", "EMPLOYEES"},
		{"HR.EMPLOYEES", "SYSTEM", "HR", "EMPLOYEES"},
		{"sales.orders", "HR", "SALES", "ORDERS"},     // Uppercased
		{"a.b.c", "HR", "A", "B.C"},                   // Only first dot splits
		{"lowercase", "ADMIN", "ADMIN", "LOWERCASE"},   // Uppercased
	}

	for _, tt := range tests {
		t.Run(tt.input, func(t *testing.T) {
			owner, name := splitTableName(tt.input, tt.defaultOwner)
			if owner != tt.wantOwner {
				t.Errorf("splitTableName(%q, %q) owner = %q, want %q", tt.input, tt.defaultOwner, owner, tt.wantOwner)
			}
			if name != tt.wantName {
				t.Errorf("splitTableName(%q, %q) name = %q, want %q", tt.input, tt.defaultOwner, name, tt.wantName)
			}
		})
	}
}
