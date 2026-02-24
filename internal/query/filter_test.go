package query

import (
	"fmt"
	"strings"
	"testing"
)

// testQuoter wraps identifiers in double quotes.
func testQuoter(name string) string {
	return fmt.Sprintf(`"%s"`, name)
}

func TestParseFilter_EmptyInput(t *testing.T) {
	result, err := ParseFilter("", testQuoter, PostgresPlaceholder)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if result.WhereClause != "" {
		t.Errorf("expected empty where clause, got %q", result.WhereClause)
	}
	if len(result.Params) != 0 {
		t.Errorf("expected no params, got %d", len(result.Params))
	}
}

func TestParseFilter_SimpleEquality(t *testing.T) {
	result, err := ParseFilter("name = 'Alice'", testQuoter, PostgresPlaceholder)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if result.WhereClause != `"name" = $1` {
		t.Errorf("unexpected where clause: %q", result.WhereClause)
	}
	if len(result.Params) != 1 || result.Params[0] != "Alice" {
		t.Errorf("unexpected params: %v", result.Params)
	}
}

func TestParseFilter_NumericComparison(t *testing.T) {
	result, err := ParseFilter("age > 21", testQuoter, PostgresPlaceholder)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if result.WhereClause != `"age" > $1` {
		t.Errorf("unexpected where clause: %q", result.WhereClause)
	}
	if len(result.Params) != 1 || result.Params[0] != int64(21) {
		t.Errorf("unexpected params: %v", result.Params)
	}
}

func TestParseFilter_FloatValue(t *testing.T) {
	result, err := ParseFilter("price >= 19.99", testQuoter, PostgresPlaceholder)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if result.WhereClause != `"price" >= $1` {
		t.Errorf("unexpected where clause: %q", result.WhereClause)
	}
	if len(result.Params) != 1 || result.Params[0] != 19.99 {
		t.Errorf("unexpected params: %v", result.Params)
	}
}

func TestParseFilter_NegativeNumber(t *testing.T) {
	result, err := ParseFilter("temp < -10", testQuoter, PostgresPlaceholder)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if result.WhereClause != `"temp" < $1` {
		t.Errorf("unexpected where clause: %q", result.WhereClause)
	}
	if len(result.Params) != 1 || result.Params[0] != int64(-10) {
		t.Errorf("unexpected params: %v", result.Params)
	}
}

func TestParseFilter_BooleanValue(t *testing.T) {
	result, err := ParseFilter("active = true", testQuoter, PostgresPlaceholder)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if result.WhereClause != `"active" = $1` {
		t.Errorf("unexpected where clause: %q", result.WhereClause)
	}
	if len(result.Params) != 1 || result.Params[0] != true {
		t.Errorf("unexpected params: %v", result.Params)
	}
}

func TestParseFilter_NotEqual(t *testing.T) {
	result, err := ParseFilter("status != 'deleted'", testQuoter, PostgresPlaceholder)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if result.WhereClause != `"status" != $1` {
		t.Errorf("unexpected where clause: %q", result.WhereClause)
	}
	if len(result.Params) != 1 || result.Params[0] != "deleted" {
		t.Errorf("unexpected params: %v", result.Params)
	}
}

func TestParseFilter_NotEqualDiamond(t *testing.T) {
	result, err := ParseFilter("status <> 'deleted'", testQuoter, PostgresPlaceholder)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if result.WhereClause != `"status" <> $1` {
		t.Errorf("unexpected where clause: %q", result.WhereClause)
	}
}

func TestParseFilter_IsNull(t *testing.T) {
	result, err := ParseFilter("email IS NULL", testQuoter, PostgresPlaceholder)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if result.WhereClause != `"email" IS NULL` {
		t.Errorf("unexpected where clause: %q", result.WhereClause)
	}
	if len(result.Params) != 0 {
		t.Errorf("IS NULL should have no params, got %d", len(result.Params))
	}
}

func TestParseFilter_IsNotNull(t *testing.T) {
	result, err := ParseFilter("email IS NOT NULL", testQuoter, PostgresPlaceholder)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if result.WhereClause != `"email" IS NOT NULL` {
		t.Errorf("unexpected where clause: %q", result.WhereClause)
	}
	if len(result.Params) != 0 {
		t.Errorf("IS NOT NULL should have no params, got %d", len(result.Params))
	}
}

func TestParseFilter_NullEquality(t *testing.T) {
	// "column = NULL" should be rewritten to "column IS NULL"
	result, err := ParseFilter("email = NULL", testQuoter, PostgresPlaceholder)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if result.WhereClause != `"email" IS NULL` {
		t.Errorf("expected IS NULL rewrite, got %q", result.WhereClause)
	}
}

func TestParseFilter_NullInequality(t *testing.T) {
	result, err := ParseFilter("email != NULL", testQuoter, PostgresPlaceholder)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if result.WhereClause != `"email" IS NOT NULL` {
		t.Errorf("expected IS NOT NULL rewrite, got %q", result.WhereClause)
	}
}

func TestParseFilter_InList(t *testing.T) {
	result, err := ParseFilter("status IN ('active', 'pending')", testQuoter, PostgresPlaceholder)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if result.WhereClause != `"status" IN ($1, $2)` {
		t.Errorf("unexpected where clause: %q", result.WhereClause)
	}
	if len(result.Params) != 2 || result.Params[0] != "active" || result.Params[1] != "pending" {
		t.Errorf("unexpected params: %v", result.Params)
	}
}

func TestParseFilter_NotInList(t *testing.T) {
	result, err := ParseFilter("id NOT IN (1, 2, 3)", testQuoter, PostgresPlaceholder)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if result.WhereClause != `"id" NOT IN ($1, $2, $3)` {
		t.Errorf("unexpected where clause: %q", result.WhereClause)
	}
	if len(result.Params) != 3 {
		t.Errorf("expected 3 params, got %d", len(result.Params))
	}
}

func TestParseFilter_Like(t *testing.T) {
	result, err := ParseFilter("name LIKE '%smith%'", testQuoter, PostgresPlaceholder)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if result.WhereClause != `"name" LIKE $1` {
		t.Errorf("unexpected where clause: %q", result.WhereClause)
	}
	if len(result.Params) != 1 || result.Params[0] != "%smith%" {
		t.Errorf("unexpected params: %v", result.Params)
	}
}

func TestParseFilter_Between(t *testing.T) {
	result, err := ParseFilter("age BETWEEN 18 AND 65", testQuoter, PostgresPlaceholder)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if result.WhereClause != `"age" BETWEEN $1 AND $2` {
		t.Errorf("unexpected where clause: %q", result.WhereClause)
	}
	if len(result.Params) != 2 {
		t.Errorf("expected 2 params, got %d", len(result.Params))
	}
	if result.Params[0] != int64(18) || result.Params[1] != int64(65) {
		t.Errorf("unexpected params: %v", result.Params)
	}
}

func TestParseFilter_ANDCombination(t *testing.T) {
	result, err := ParseFilter("age > 21 AND state = 'CA'", testQuoter, PostgresPlaceholder)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if result.WhereClause != `"age" > $1 AND "state" = $2` {
		t.Errorf("unexpected where clause: %q", result.WhereClause)
	}
	if len(result.Params) != 2 {
		t.Errorf("expected 2 params, got %d", len(result.Params))
	}
	if result.Params[0] != int64(21) || result.Params[1] != "CA" {
		t.Errorf("unexpected params: %v", result.Params)
	}
}

func TestParseFilter_ORCombination(t *testing.T) {
	result, err := ParseFilter("status = 'active' OR status = 'pending'", testQuoter, PostgresPlaceholder)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if result.WhereClause != `"status" = $1 OR "status" = $2` {
		t.Errorf("unexpected where clause: %q", result.WhereClause)
	}
	if len(result.Params) != 2 {
		t.Errorf("expected 2 params, got %d", len(result.Params))
	}
}

func TestParseFilter_MultipleAND(t *testing.T) {
	result, err := ParseFilter("age > 21 AND state = 'CA' AND active = true", testQuoter, PostgresPlaceholder)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if result.WhereClause != `"age" > $1 AND "state" = $2 AND "active" = $3` {
		t.Errorf("unexpected where clause: %q", result.WhereClause)
	}
	if len(result.Params) != 3 {
		t.Errorf("expected 3 params, got %d", len(result.Params))
	}
}

func TestParseFilter_EscapedQuotes(t *testing.T) {
	result, err := ParseFilter("name = 'O''Brien'", testQuoter, PostgresPlaceholder)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if result.WhereClause != `"name" = $1` {
		t.Errorf("unexpected where clause: %q", result.WhereClause)
	}
	if len(result.Params) != 1 || result.Params[0] != "O'Brien" {
		t.Errorf("unexpected params: %v", result.Params)
	}
}

func TestParseFilter_MysqlPlaceholders(t *testing.T) {
	result, err := ParseFilter("age > 21 AND name = 'Alice'", testQuoter, QuestionPlaceholder)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if result.WhereClause != `"age" > ? AND "name" = ?` {
		t.Errorf("unexpected where clause: %q", result.WhereClause)
	}
}

func TestParseFilter_MSSQLPlaceholders(t *testing.T) {
	result, err := ParseFilter("age > 21 AND name = 'Alice'", testQuoter, MSSQLPlaceholder)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if result.WhereClause != `"age" > @p1 AND "name" = @p2` {
		t.Errorf("unexpected where clause: %q", result.WhereClause)
	}
}

// --- JSON filter tests ---

func TestParseFilter_JSONEquality(t *testing.T) {
	result, err := ParseFilter(`{"name": "Alice"}`, testQuoter, PostgresPlaceholder)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if result.WhereClause != `"name" = $1` {
		t.Errorf("unexpected where clause: %q", result.WhereClause)
	}
	if len(result.Params) != 1 || result.Params[0] != "Alice" {
		t.Errorf("unexpected params: %v", result.Params)
	}
}

func TestParseFilter_JSONOperator(t *testing.T) {
	result, err := ParseFilter(`{"age": {"$gt": 21}}`, testQuoter, PostgresPlaceholder)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if result.WhereClause != `"age" > $1` {
		t.Errorf("unexpected where clause: %q", result.WhereClause)
	}
	if len(result.Params) != 1 {
		t.Errorf("expected 1 param, got %d", len(result.Params))
	}
}

func TestParseFilter_JSONNull(t *testing.T) {
	result, err := ParseFilter(`{"email": null}`, testQuoter, PostgresPlaceholder)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if result.WhereClause != `"email" IS NULL` {
		t.Errorf("unexpected where clause: %q", result.WhereClause)
	}
	if len(result.Params) != 0 {
		t.Errorf("expected 0 params for NULL, got %d", len(result.Params))
	}
}

func TestParseFilter_JSONMultipleKeys(t *testing.T) {
	result, err := ParseFilter(`{"age": {"$gte": 18}, "status": "active"}`, testQuoter, PostgresPlaceholder)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	// Keys are sorted alphabetically: age before status.
	if result.WhereClause != `"age" >= $1 AND "status" = $2` {
		t.Errorf("unexpected where clause: %q", result.WhereClause)
	}
	if len(result.Params) != 2 {
		t.Errorf("expected 2 params, got %d", len(result.Params))
	}
}

// --- SQL injection rejection tests ---

func TestParseFilter_RejectSemicolon(t *testing.T) {
	_, err := ParseFilter("name = 'x'; DROP TABLE users", testQuoter, PostgresPlaceholder)
	if err == nil {
		t.Fatal("expected error for semicolon injection")
	}
	assertInjectionError(t, err, "semicolon")
}

func TestParseFilter_RejectLineComment(t *testing.T) {
	_, err := ParseFilter("name = 'x' -- comment", testQuoter, PostgresPlaceholder)
	if err == nil {
		t.Fatal("expected error for line comment injection")
	}
	assertInjectionError(t, err, "comment")
}

func TestParseFilter_RejectBlockComment(t *testing.T) {
	_, err := ParseFilter("name = 'x' /* comment */", testQuoter, PostgresPlaceholder)
	if err == nil {
		t.Fatal("expected error for block comment injection")
	}
	assertInjectionError(t, err, "comment")
}

func TestParseFilter_RejectSELECT(t *testing.T) {
	_, err := ParseFilter("id IN (SELECT id FROM admin)", testQuoter, PostgresPlaceholder)
	if err == nil {
		t.Fatal("expected error for SELECT subquery injection")
	}
	assertInjectionError(t, err, "SELECT")
}

func TestParseFilter_RejectUNION(t *testing.T) {
	_, err := ParseFilter("id = 1 UNION ALL", testQuoter, PostgresPlaceholder)
	if err == nil {
		t.Fatal("expected error for UNION injection")
	}
	assertInjectionError(t, err, "UNION")
}

func TestParseFilter_RejectDROP(t *testing.T) {
	_, err := ParseFilter("name = 'x' OR 1=1 DROP TABLE users", testQuoter, PostgresPlaceholder)
	if err == nil {
		t.Fatal("expected error for DROP injection")
	}
	assertInjectionError(t, err, "DROP")
}

func TestParseFilter_RejectINSERT(t *testing.T) {
	_, err := ParseFilter("id = 1 INSERT INTO admin", testQuoter, PostgresPlaceholder)
	if err == nil {
		t.Fatal("expected error for INSERT injection")
	}
	assertInjectionError(t, err, "INSERT")
}

func TestParseFilter_RejectUPDATE(t *testing.T) {
	_, err := ParseFilter("id = 1 UPDATE users SET admin=true", testQuoter, PostgresPlaceholder)
	if err == nil {
		t.Fatal("expected error for UPDATE injection")
	}
	assertInjectionError(t, err, "UPDATE")
}

func TestParseFilter_RejectDELETE(t *testing.T) {
	_, err := ParseFilter("id = 1 DELETE FROM users", testQuoter, PostgresPlaceholder)
	if err == nil {
		t.Fatal("expected error for DELETE injection")
	}
	// May be caught by FROM or DELETE pattern — either is correct.
	if !strings.Contains(err.Error(), "injection") {
		t.Errorf("expected injection error, got: %v", err)
	}
}

func TestParseFilter_RejectDELETEAlone(t *testing.T) {
	_, err := ParseFilter("id = 1 DELETE users", testQuoter, PostgresPlaceholder)
	if err == nil {
		t.Fatal("expected error for DELETE injection")
	}
	assertInjectionError(t, err, "DELETE")
}

func TestParseFilter_RejectALTER(t *testing.T) {
	_, err := ParseFilter("id = 1 ALTER TABLE users", testQuoter, PostgresPlaceholder)
	if err == nil {
		t.Fatal("expected error for ALTER injection")
	}
	assertInjectionError(t, err, "ALTER")
}

func TestParseFilter_RejectCREATE(t *testing.T) {
	_, err := ParseFilter("id = 1 CREATE TABLE hack", testQuoter, PostgresPlaceholder)
	if err == nil {
		t.Fatal("expected error for CREATE injection")
	}
	assertInjectionError(t, err, "CREATE")
}

func TestParseFilter_RejectTRUNCATE(t *testing.T) {
	_, err := ParseFilter("id = 1 TRUNCATE TABLE users", testQuoter, PostgresPlaceholder)
	if err == nil {
		t.Fatal("expected error for TRUNCATE injection")
	}
	assertInjectionError(t, err, "TRUNCATE")
}

func TestParseFilter_RejectEXEC(t *testing.T) {
	_, err := ParseFilter("id = 1 EXEC sp_configure", testQuoter, PostgresPlaceholder)
	if err == nil {
		t.Fatal("expected error for EXEC injection")
	}
	assertInjectionError(t, err, "EXEC")
}

func TestParseFilter_AllowDangerousWordsInStringLiterals(t *testing.T) {
	// Words like SELECT inside string literals should NOT trigger rejection.
	result, err := ParseFilter("description = 'SELECT your favorite'", testQuoter, PostgresPlaceholder)
	if err != nil {
		t.Fatalf("string literal containing 'SELECT' should be allowed, got: %v", err)
	}
	if result.WhereClause != `"description" = $1` {
		t.Errorf("unexpected where clause: %q", result.WhereClause)
	}
	if len(result.Params) != 1 || result.Params[0] != "SELECT your favorite" {
		t.Errorf("unexpected params: %v", result.Params)
	}
}

func TestParseFilter_AllowDROPInStringLiteral(t *testing.T) {
	result, err := ParseFilter("action = 'DROP off package'", testQuoter, PostgresPlaceholder)
	if err != nil {
		t.Fatalf("string literal containing 'DROP' should be allowed, got: %v", err)
	}
	if result.Params[0] != "DROP off package" {
		t.Errorf("unexpected params: %v", result.Params)
	}
}

// --- Parameterization guarantee tests ---

func TestParseFilter_NoStringInterpolation(t *testing.T) {
	// The where clause must never contain the actual value inline.
	cases := []struct {
		name   string
		filter string
		value  string
	}{
		{"string value", "name = 'Alice'", "Alice"},
		{"numeric string", "id = 42", "42"},
		{"like pattern", "name LIKE '%test%'", "%test%"},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			result, err := ParseFilter(tc.filter, testQuoter, PostgresPlaceholder)
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if strings.Contains(result.WhereClause, tc.value) {
				t.Errorf("where clause %q contains interpolated value %q — MUST use parameters",
					result.WhereClause, tc.value)
			}
			if len(result.Params) == 0 {
				t.Error("expected at least one parameter")
			}
		})
	}
}

// --- Malformed input tests ---

func TestParseFilter_UnterminatedString(t *testing.T) {
	_, err := ParseFilter("name = 'Alice", testQuoter, PostgresPlaceholder)
	if err == nil {
		t.Fatal("expected error for unterminated string")
	}
}

func TestParseFilter_MissingValue(t *testing.T) {
	_, err := ParseFilter("name =", testQuoter, PostgresPlaceholder)
	if err == nil {
		t.Fatal("expected error for missing value")
	}
}

func TestParseFilter_MissingOperator(t *testing.T) {
	_, err := ParseFilter("name 'Alice'", testQuoter, PostgresPlaceholder)
	if err == nil {
		t.Fatal("expected error for missing operator")
	}
}

func TestParseFilter_TrailingAND(t *testing.T) {
	_, err := ParseFilter("name = 'Alice' AND", testQuoter, PostgresPlaceholder)
	if err == nil {
		t.Fatal("expected error for trailing AND")
	}
}

func TestParseFilter_InvalidIdentifier(t *testing.T) {
	_, err := ParseFilter("123bad = 'x'", testQuoter, PostgresPlaceholder)
	if err == nil {
		t.Fatal("expected error for identifier starting with digit")
	}
}

func TestParseFilter_RejectINTERSECT(t *testing.T) {
	_, err := ParseFilter("id = 1 INTERSECT ALL", testQuoter, PostgresPlaceholder)
	if err == nil {
		t.Fatal("expected error for INTERSECT injection")
	}
	assertInjectionError(t, err, "INTERSECT")
}

func TestParseFilter_RejectEXCEPT(t *testing.T) {
	_, err := ParseFilter("id = 1 EXCEPT ALL", testQuoter, PostgresPlaceholder)
	if err == nil {
		t.Fatal("expected error for EXCEPT injection")
	}
	assertInjectionError(t, err, "EXCEPT")
}

func TestParseFilter_CaseInsensitiveKeywords(t *testing.T) {
	// AND/OR should work case-insensitively.
	result, err := ParseFilter("age > 21 and name = 'Alice'", testQuoter, PostgresPlaceholder)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if result.WhereClause != `"age" > $1 AND "name" = $2` {
		t.Errorf("unexpected where clause: %q", result.WhereClause)
	}
}

func TestParseFilter_CaseInsensitiveIS(t *testing.T) {
	result, err := ParseFilter("email is null", testQuoter, PostgresPlaceholder)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if result.WhereClause != `"email" IS NULL` {
		t.Errorf("unexpected where clause: %q", result.WhereClause)
	}
}

func TestParseFilter_BetweenStrings(t *testing.T) {
	result, err := ParseFilter("name BETWEEN 'A' AND 'M'", testQuoter, PostgresPlaceholder)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if result.WhereClause != `"name" BETWEEN $1 AND $2` {
		t.Errorf("unexpected where clause: %q", result.WhereClause)
	}
	if len(result.Params) != 2 || result.Params[0] != "A" || result.Params[1] != "M" {
		t.Errorf("unexpected params: %v", result.Params)
	}
}

func TestParseFilter_InWithNumbers(t *testing.T) {
	result, err := ParseFilter("id IN (1, 2, 3)", testQuoter, PostgresPlaceholder)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if result.WhereClause != `"id" IN ($1, $2, $3)` {
		t.Errorf("unexpected where clause: %q", result.WhereClause)
	}
	if len(result.Params) != 3 {
		t.Errorf("expected 3 params, got %d", len(result.Params))
	}
}

func TestParseFilter_ComplexExpression(t *testing.T) {
	filter := "age > 18 AND status = 'active' OR role = 'admin'"
	result, err := ParseFilter(filter, testQuoter, PostgresPlaceholder)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	expected := `"age" > $1 AND "status" = $2 OR "role" = $3`
	if result.WhereClause != expected {
		t.Errorf("unexpected where clause:\n  got:  %q\n  want: %q", result.WhereClause, expected)
	}
	if len(result.Params) != 3 {
		t.Errorf("expected 3 params, got %d", len(result.Params))
	}
}

// --- Sanitizer tests ---

func TestSanitizeFilterInput_AllowsCleanInput(t *testing.T) {
	cases := []string{
		"age > 21",
		"name = 'Alice'",
		"status IN ('active', 'pending')",
		"created_at BETWEEN '2024-01-01' AND '2024-12-31'",
		"description LIKE '%test%'",
	}
	for _, input := range cases {
		if err := SanitizeFilterInput(input); err != nil {
			t.Errorf("clean input %q was rejected: %v", input, err)
		}
	}
}

func TestValidateIdentifier_ValidNames(t *testing.T) {
	valid := []string{"users", "user_id", "UserName", "_private", "table1", "schema_name.table_name"}
	for _, name := range valid {
		if err := ValidateIdentifier(name); err != nil {
			t.Errorf("valid identifier %q was rejected: %v", name, err)
		}
	}
}

func TestValidateIdentifier_InvalidNames(t *testing.T) {
	invalid := []string{"", "123abc", "user-id", "user;id", "table name", "col$umn"}
	for _, name := range invalid {
		if err := ValidateIdentifier(name); err == nil {
			t.Errorf("invalid identifier %q was accepted", name)
		}
	}
}

func TestValidateIdentifier_RejectsKeywords(t *testing.T) {
	keywords := []string{"SELECT", "select", "DROP", "drop", "INSERT", "DELETE"}
	for _, kw := range keywords {
		if err := ValidateIdentifier(kw); err == nil {
			t.Errorf("keyword %q should be rejected as identifier", kw)
		}
	}
}

func TestSanitizeOrderBy_Valid(t *testing.T) {
	cases := []string{
		"name",
		"name ASC",
		"name DESC",
		"name ASC, age DESC",
		"created_at DESC",
	}
	for _, ob := range cases {
		if err := SanitizeOrderBy(ob); err != nil {
			t.Errorf("valid ORDER BY %q was rejected: %v", ob, err)
		}
	}
}

func TestSanitizeOrderBy_Invalid(t *testing.T) {
	cases := []string{
		"name; DROP TABLE",
		"1=1",
		"name ASCENDING",
	}
	for _, ob := range cases {
		if err := SanitizeOrderBy(ob); err == nil {
			t.Errorf("invalid ORDER BY %q was accepted", ob)
		}
	}
}

// assertInjectionError checks that an error message contains the expected pattern.
func assertInjectionError(t *testing.T, err error, expectedPattern string) {
	t.Helper()
	if err == nil {
		t.Fatal("expected error, got nil")
	}
	if !strings.Contains(strings.ToLower(err.Error()), strings.ToLower(expectedPattern)) {
		t.Errorf("error %q does not mention expected pattern %q", err.Error(), expectedPattern)
	}
}
