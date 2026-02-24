package query

import (
	"encoding/json"
	"fmt"
	"strconv"
	"strings"
	"unicode"
)

// PlaceholderFunc generates a parameter placeholder for the given 1-based
// index. Different databases use different styles: PostgreSQL uses $1, $2;
// MySQL/SQLite use ?; SQL Server uses @p1, @p2.
type PlaceholderFunc func(index int) string

// PostgresPlaceholder returns $1, $2, $3, ...
func PostgresPlaceholder(index int) string { return fmt.Sprintf("$%d", index) }

// QuestionPlaceholder returns ? for all indices (MySQL, SQLite).
func QuestionPlaceholder(_ int) string { return "?" }

// MSSQLPlaceholder returns @p1, @p2, @p3, ...
func MSSQLPlaceholder(index int) string { return fmt.Sprintf("@p%d", index) }

// ParsedFilter is the result of parsing a filter expression. It contains the
// parameterized WHERE clause and the ordered list of parameter values.
type ParsedFilter struct {
	// WhereClause is the SQL WHERE clause with parameter placeholders.
	// Example: "age > $1 AND state = $2"
	WhereClause string

	// Params is the ordered list of parameter values corresponding to
	// placeholders in WhereClause.
	Params []any
}

// ParseFilter parses a filter expression (string-based or JSON) into a
// parameterized WHERE clause. The quoter function is used to quote identifiers,
// and the placeholder function generates parameter placeholders.
//
// String-based filters follow the EBNF grammar specified in the project docs.
// JSON filters use the format: {"column": {"op": value}} or {"column": value}
// for equality.
func ParseFilter(input string, quoter func(string) string, placeholder PlaceholderFunc) (*ParsedFilter, error) {
	input = strings.TrimSpace(input)
	if input == "" {
		return &ParsedFilter{}, nil
	}

	// Run injection check first as defense-in-depth.
	if err := SanitizeFilterInput(input); err != nil {
		return nil, err
	}

	// Detect JSON object filters.
	if len(input) > 0 && input[0] == '{' {
		return parseJSONFilter(input, quoter, placeholder)
	}

	// Parse string-based filter expression.
	return parseStringFilter(input, quoter, placeholder)
}

// parseJSONFilter handles JSON object-based filters.
// Format: {"column": value} for equality, {"column": {"$gt": value}} for operators.
func parseJSONFilter(input string, quoter func(string) string, placeholder PlaceholderFunc) (*ParsedFilter, error) {
	var obj map[string]json.RawMessage
	if err := json.Unmarshal([]byte(input), &obj); err != nil {
		return nil, fmt.Errorf("invalid JSON filter: %w", err)
	}

	var clauses []string
	var params []any
	paramIdx := 1

	// Process keys in a deterministic order for testability.
	keys := sortedKeys(obj)

	for _, col := range keys {
		raw := obj[col]
		if err := ValidateIdentifier(col); err != nil {
			return nil, fmt.Errorf("invalid column in JSON filter: %w", err)
		}
		quotedCol := quoter(col)

		// Try to unmarshal as an operator object.
		var opObj map[string]json.RawMessage
		if err := json.Unmarshal(raw, &opObj); err == nil && len(opObj) > 0 {
			// Check if this is an operator object (keys start with $).
			isOpObj := false
			for k := range opObj {
				if strings.HasPrefix(k, "$") {
					isOpObj = true
					break
				}
			}
			if isOpObj {
				for _, opKey := range sortedKeys(opObj) {
					op, err := jsonOperator(opKey)
					if err != nil {
						return nil, err
					}
					var val any
					if err := json.Unmarshal(opObj[opKey], &val); err != nil {
						return nil, fmt.Errorf("invalid value for %s.%s: %w", col, opKey, err)
					}
					clauses = append(clauses, fmt.Sprintf("%s %s %s", quotedCol, op, placeholder(paramIdx)))
					params = append(params, val)
					paramIdx++
				}
				continue
			}
		}

		// Simple equality: {"column": value}
		var val any
		if err := json.Unmarshal(raw, &val); err != nil {
			return nil, fmt.Errorf("invalid value for column %q: %w", col, err)
		}
		if val == nil {
			clauses = append(clauses, fmt.Sprintf("%s IS NULL", quotedCol))
		} else {
			clauses = append(clauses, fmt.Sprintf("%s = %s", quotedCol, placeholder(paramIdx)))
			params = append(params, val)
			paramIdx++
		}
	}

	return &ParsedFilter{
		WhereClause: strings.Join(clauses, " AND "),
		Params:      params,
	}, nil
}

// jsonOperator maps JSON filter operator keys to SQL operators.
func jsonOperator(key string) (string, error) {
	switch key {
	case "$eq":
		return "=", nil
	case "$ne":
		return "!=", nil
	case "$gt":
		return ">", nil
	case "$gte":
		return ">=", nil
	case "$lt":
		return "<", nil
	case "$lte":
		return "<=", nil
	default:
		return "", fmt.Errorf("unsupported JSON filter operator: %q", key)
	}
}

// sortedKeys returns the keys of a map in sorted order.
func sortedKeys[V any](m map[string]V) []string {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	// Simple insertion sort — maps are small.
	for i := 1; i < len(keys); i++ {
		for j := i; j > 0 && keys[j] < keys[j-1]; j-- {
			keys[j], keys[j-1] = keys[j-1], keys[j]
		}
	}
	return keys
}

// --- String-based filter parser ---

// tokenType identifies the kind of token produced by the lexer.
type tokenType int

const (
	tokEOF tokenType = iota
	tokIdentifier
	tokNumber
	tokString
	tokBoolean
	tokNull
	tokOperator
	tokLParen
	tokRParen
	tokComma
	tokAND
	tokOR
	tokIS
	tokNOT
	tokIN
	tokLIKE
	tokBETWEEN
)

// token holds a single lexed token.
type token struct {
	typ tokenType
	val string
	pos int
}

// lexer tokenizes a filter expression string.
type lexer struct {
	input  string
	pos    int
	tokens []token
}

// lex tokenizes the entire input.
func lex(input string) ([]token, error) {
	l := &lexer{input: input}
	if err := l.run(); err != nil {
		return nil, err
	}
	return l.tokens, nil
}

func (l *lexer) run() error {
	for l.pos < len(l.input) {
		l.skipWhitespace()
		if l.pos >= len(l.input) {
			break
		}

		ch := l.input[l.pos]

		switch {
		case ch == '(':
			l.tokens = append(l.tokens, token{tokLParen, "(", l.pos})
			l.pos++
		case ch == ')':
			l.tokens = append(l.tokens, token{tokRParen, ")", l.pos})
			l.pos++
		case ch == ',':
			l.tokens = append(l.tokens, token{tokComma, ",", l.pos})
			l.pos++
		case ch == '\'':
			tok, err := l.readString()
			if err != nil {
				return err
			}
			l.tokens = append(l.tokens, tok)
		case ch == '!' && l.pos+1 < len(l.input) && l.input[l.pos+1] == '=':
			l.tokens = append(l.tokens, token{tokOperator, "!=", l.pos})
			l.pos += 2
		case ch == '<' && l.pos+1 < len(l.input) && l.input[l.pos+1] == '>':
			l.tokens = append(l.tokens, token{tokOperator, "<>", l.pos})
			l.pos += 2
		case ch == '<' && l.pos+1 < len(l.input) && l.input[l.pos+1] == '=':
			l.tokens = append(l.tokens, token{tokOperator, "<=", l.pos})
			l.pos += 2
		case ch == '>' && l.pos+1 < len(l.input) && l.input[l.pos+1] == '=':
			l.tokens = append(l.tokens, token{tokOperator, ">=", l.pos})
			l.pos += 2
		case ch == '=':
			l.tokens = append(l.tokens, token{tokOperator, "=", l.pos})
			l.pos++
		case ch == '<':
			l.tokens = append(l.tokens, token{tokOperator, "<", l.pos})
			l.pos++
		case ch == '>':
			l.tokens = append(l.tokens, token{tokOperator, ">", l.pos})
			l.pos++
		case ch == '-' || (ch >= '0' && ch <= '9'):
			tok := l.readNumber()
			l.tokens = append(l.tokens, tok)
		case isIdentStart(ch):
			tok := l.readIdentifier()
			l.tokens = append(l.tokens, tok)
		default:
			return fmt.Errorf("unexpected character %q at position %d", ch, l.pos)
		}
	}

	l.tokens = append(l.tokens, token{tokEOF, "", l.pos})
	return nil
}

func (l *lexer) skipWhitespace() {
	for l.pos < len(l.input) && unicode.IsSpace(rune(l.input[l.pos])) {
		l.pos++
	}
}

func (l *lexer) readString() (token, error) {
	start := l.pos
	l.pos++ // skip opening quote
	var b strings.Builder
	for l.pos < len(l.input) {
		if l.input[l.pos] == '\'' {
			if l.pos+1 < len(l.input) && l.input[l.pos+1] == '\'' {
				// Escaped quote.
				b.WriteByte('\'')
				l.pos += 2
			} else {
				l.pos++ // skip closing quote
				return token{tokString, b.String(), start}, nil
			}
		} else {
			b.WriteByte(l.input[l.pos])
			l.pos++
		}
	}
	return token{}, fmt.Errorf("unterminated string literal at position %d", start)
}

func (l *lexer) readNumber() token {
	start := l.pos
	if l.input[l.pos] == '-' {
		l.pos++
	}
	for l.pos < len(l.input) && l.input[l.pos] >= '0' && l.input[l.pos] <= '9' {
		l.pos++
	}
	if l.pos < len(l.input) && l.input[l.pos] == '.' {
		l.pos++
		for l.pos < len(l.input) && l.input[l.pos] >= '0' && l.input[l.pos] <= '9' {
			l.pos++
		}
	}
	return token{tokNumber, l.input[start:l.pos], start}
}

func (l *lexer) readIdentifier() token {
	start := l.pos
	for l.pos < len(l.input) && isIdentChar(l.input[l.pos]) {
		l.pos++
	}
	val := l.input[start:l.pos]
	upper := strings.ToUpper(val)

	var typ tokenType
	switch upper {
	case "AND":
		typ = tokAND
	case "OR":
		typ = tokOR
	case "IS":
		typ = tokIS
	case "NOT":
		typ = tokNOT
	case "IN":
		typ = tokIN
	case "LIKE":
		typ = tokLIKE
	case "BETWEEN":
		typ = tokBETWEEN
	case "NULL":
		typ = tokNull
	case "TRUE", "FALSE":
		typ = tokBoolean
	default:
		typ = tokIdentifier
	}

	return token{typ, val, start}
}

func isIdentStart(ch byte) bool {
	return (ch >= 'a' && ch <= 'z') || (ch >= 'A' && ch <= 'Z') || ch == '_'
}

func isIdentChar(ch byte) bool {
	return isIdentStart(ch) || (ch >= '0' && ch <= '9')
}

// parser builds a ParsedFilter from tokens.
type parser struct {
	tokens      []token
	pos         int
	quoter      func(string) string
	placeholder PlaceholderFunc
	params      []any
	paramIdx    int
}

// parseStringFilter parses a string-based filter expression.
func parseStringFilter(input string, quoter func(string) string, placeholder PlaceholderFunc) (*ParsedFilter, error) {
	tokens, err := lex(input)
	if err != nil {
		return nil, fmt.Errorf("filter lexer error: %w", err)
	}

	p := &parser{
		tokens:      tokens,
		quoter:      quoter,
		placeholder: placeholder,
		paramIdx:    1,
	}

	clause, err := p.parseExpression()
	if err != nil {
		return nil, fmt.Errorf("filter parse error: %w", err)
	}

	// Ensure we consumed all tokens.
	if p.current().typ != tokEOF {
		return nil, fmt.Errorf("unexpected token %q at position %d", p.current().val, p.current().pos)
	}

	return &ParsedFilter{
		WhereClause: clause,
		Params:      p.params,
	}, nil
}

func (p *parser) current() token {
	if p.pos >= len(p.tokens) {
		return token{tokEOF, "", len(p.tokens)}
	}
	return p.tokens[p.pos]
}

func (p *parser) advance() token {
	tok := p.current()
	p.pos++
	return tok
}

func (p *parser) expect(typ tokenType) (token, error) {
	tok := p.current()
	if tok.typ != typ {
		return tok, fmt.Errorf("expected %d but got %q at position %d", typ, tok.val, tok.pos)
	}
	p.pos++
	return tok, nil
}

// addParam adds a value to the parameter list and returns the placeholder string.
func (p *parser) addParam(val any) string {
	p.params = append(p.params, val)
	ph := p.placeholder(p.paramIdx)
	p.paramIdx++
	return ph
}

// parseExpression handles: comparison { ("AND" | "OR") comparison }
func (p *parser) parseExpression() (string, error) {
	left, err := p.parseComparison()
	if err != nil {
		return "", err
	}

	for p.current().typ == tokAND || p.current().typ == tokOR {
		op := strings.ToUpper(p.advance().val)
		right, err := p.parseComparison()
		if err != nil {
			return "", err
		}
		left = left + " " + op + " " + right
	}

	return left, nil
}

// parseComparison handles all comparison forms.
func (p *parser) parseComparison() (string, error) {
	// Expect a column identifier.
	colTok := p.current()
	if colTok.typ != tokIdentifier {
		return "", fmt.Errorf("expected column name, got %q at position %d", colTok.val, colTok.pos)
	}
	p.advance()

	colName := colTok.val
	if err := ValidateIdentifier(colName); err != nil {
		return "", fmt.Errorf("invalid column name: %w", err)
	}
	quotedCol := p.quoter(colName)

	cur := p.current()

	switch cur.typ {
	case tokIS:
		return p.parseIsNull(quotedCol)
	case tokIN:
		return p.parseIn(quotedCol, false)
	case tokNOT:
		// NOT IN
		p.advance()
		if p.current().typ != tokIN {
			return "", fmt.Errorf("expected IN after NOT at position %d", p.current().pos)
		}
		return p.parseIn(quotedCol, true)
	case tokLIKE:
		return p.parseLike(quotedCol)
	case tokBETWEEN:
		return p.parseBetween(quotedCol)
	case tokOperator:
		return p.parseOperatorComparison(quotedCol)
	default:
		return "", fmt.Errorf("expected operator after column %q, got %q at position %d", colName, cur.val, cur.pos)
	}
}

// parseIsNull handles: IS NULL | IS NOT NULL
func (p *parser) parseIsNull(quotedCol string) (string, error) {
	p.advance() // consume IS

	if p.current().typ == tokNOT {
		p.advance() // consume NOT
		if _, err := p.expect(tokNull); err != nil {
			return "", fmt.Errorf("expected NULL after IS NOT")
		}
		return quotedCol + " IS NOT NULL", nil
	}

	if _, err := p.expect(tokNull); err != nil {
		return "", fmt.Errorf("expected NULL after IS")
	}
	return quotedCol + " IS NULL", nil
}

// parseIn handles: IN (value, value, ...) and NOT IN (...)
func (p *parser) parseIn(quotedCol string, negated bool) (string, error) {
	p.advance() // consume IN
	if _, err := p.expect(tokLParen); err != nil {
		return "", fmt.Errorf("expected '(' after IN")
	}

	var placeholders []string
	for {
		val, err := p.parseValue()
		if err != nil {
			return "", fmt.Errorf("in IN list: %w", err)
		}
		ph := p.addParam(val)
		placeholders = append(placeholders, ph)

		if p.current().typ == tokComma {
			p.advance()
		} else {
			break
		}
	}

	if _, err := p.expect(tokRParen); err != nil {
		return "", fmt.Errorf("expected ')' to close IN list")
	}

	op := "IN"
	if negated {
		op = "NOT IN"
	}
	return fmt.Sprintf("%s %s (%s)", quotedCol, op, strings.Join(placeholders, ", ")), nil
}

// parseLike handles: LIKE string_literal
func (p *parser) parseLike(quotedCol string) (string, error) {
	p.advance() // consume LIKE

	strTok := p.current()
	if strTok.typ != tokString {
		return "", fmt.Errorf("LIKE requires a string pattern, got %q at position %d", strTok.val, strTok.pos)
	}
	p.advance()

	ph := p.addParam(strTok.val)
	return fmt.Sprintf("%s LIKE %s", quotedCol, ph), nil
}

// parseBetween handles: BETWEEN value AND value
func (p *parser) parseBetween(quotedCol string) (string, error) {
	p.advance() // consume BETWEEN

	low, err := p.parseValue()
	if err != nil {
		return "", fmt.Errorf("in BETWEEN low: %w", err)
	}

	if _, err := p.expect(tokAND); err != nil {
		return "", fmt.Errorf("expected AND in BETWEEN expression")
	}

	high, err := p.parseValue()
	if err != nil {
		return "", fmt.Errorf("in BETWEEN high: %w", err)
	}

	phLow := p.addParam(low)
	phHigh := p.addParam(high)
	return fmt.Sprintf("%s BETWEEN %s AND %s", quotedCol, phLow, phHigh), nil
}

// parseOperatorComparison handles: column op value
func (p *parser) parseOperatorComparison(quotedCol string) (string, error) {
	opTok := p.advance() // consume operator
	op := opTok.val

	// Check for NULL comparison with =.
	if p.current().typ == tokNull {
		p.advance()
		if op == "=" {
			return quotedCol + " IS NULL", nil
		} else if op == "!=" || op == "<>" {
			return quotedCol + " IS NOT NULL", nil
		}
		return "", fmt.Errorf("cannot use operator %q with NULL", op)
	}

	val, err := p.parseValue()
	if err != nil {
		return "", fmt.Errorf("after %q operator: %w", op, err)
	}

	ph := p.addParam(val)
	return fmt.Sprintf("%s %s %s", quotedCol, op, ph), nil
}

// parseValue parses a literal value (string, number, boolean, or NULL).
func (p *parser) parseValue() (any, error) {
	tok := p.current()
	switch tok.typ {
	case tokString:
		p.advance()
		return tok.val, nil
	case tokNumber:
		p.advance()
		return parseNumber(tok.val)
	case tokBoolean:
		p.advance()
		return strings.ToLower(tok.val) == "true", nil
	case tokNull:
		p.advance()
		return nil, nil
	default:
		return nil, fmt.Errorf("expected value, got %q at position %d", tok.val, tok.pos)
	}
}

// parseNumber converts a number string to int64 or float64.
func parseNumber(s string) (any, error) {
	if strings.Contains(s, ".") {
		f, err := strconv.ParseFloat(s, 64)
		if err != nil {
			return nil, fmt.Errorf("invalid number %q: %w", s, err)
		}
		return f, nil
	}
	n, err := strconv.ParseInt(s, 10, 64)
	if err != nil {
		return nil, fmt.Errorf("invalid number %q: %w", s, err)
	}
	return n, nil
}
