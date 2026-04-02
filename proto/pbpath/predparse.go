package pbpath

import (
	"fmt"
	"strconv"

	"google.golang.org/protobuf/reflect/protoreflect"
)

// parsePredicate parses a filter predicate expression from the given scanner.
// The scanner position must be immediately after the opening "[?(" tokens.
// It consumes tokens up to and including the closing ")]" sequence
// (cparen followed by cbrack).
//
// Grammar (informal):
//
//	predicate  = orExpr
//	orExpr     = andExpr { "||" andExpr }
//	andExpr    = unaryExpr { "&&" unaryExpr }
//	unaryExpr  = "!" unaryExpr | primary
//	primary    = comparison | truthyCheck | "(" orExpr ")"
//	comparison = atom comparator atom
//	truthyCheck = relPath
//	comparator = "==" | "!=" | "<" | "<=" | ">" | ">="
//	atom       = relPath | stringLit | intLit | floatLit | "true" | "false"
//	relPath    = "." ident { "." ident }
//
// The returned Expr tree uses [FilterPathRef] for relative path references,
// [Literal] for constants, and the corresponding [FuncEq]/[FuncNe]/etc.
// comparison functions.
//
// md is the message descriptor at the cursor position (the message type that
// the predicate's relative paths are resolved against).
func parsePredicate(s *scanner, md protoreflect.MessageDescriptor) (Expr, error) {
	expr, err := parseOrExpr(s, md)
	if err != nil {
		return nil, err
	}
	// Expect ')' ']' to close the [?(...)] syntax.
	tok := s.scan()
	if tok.Kind != cparen {
		return nil, fmt.Errorf("predicate parse: expected ')' at position %d, got %s", tok.Pos, describeToken(tok))
	}
	tok = s.scan()
	if tok.Kind != cbrack {
		return nil, fmt.Errorf("predicate parse: expected ']' at position %d, got %s", tok.Pos, describeToken(tok))
	}
	return expr, nil
}

// parsePredicateExpr parses a standalone predicate expression string.
// Unlike [parsePredicate], this does NOT expect surrounding [?(...)] delimiters.
// The full input string should be the predicate body (e.g. ".name == \"x\"").
//
// md is the message descriptor for path resolution.
func parsePredicateExpr(md protoreflect.MessageDescriptor, input string) (Expr, error) {
	s := &scanner{buf: []byte(input)}
	expr, err := parseOrExpr(s, md)
	if err != nil {
		return nil, err
	}
	tok := s.scan()
	if tok.Kind != eof {
		return nil, fmt.Errorf("predicate parse: unexpected trailing token %s at position %d", describeToken(tok), tok.Pos)
	}
	return expr, nil
}

// parseOrExpr parses:  andExpr { "||" andExpr }
func parseOrExpr(s *scanner, md protoreflect.MessageDescriptor) (Expr, error) {
	left, err := parseAndExpr(s, md)
	if err != nil {
		return nil, err
	}
	for {
		tok := s.scan()
		if tok.Kind != pipepipe {
			s.unscan(tok)
			return left, nil
		}
		right, err := parseAndExpr(s, md)
		if err != nil {
			return nil, err
		}
		left = FuncOr(left, right)
	}
}

// parseAndExpr parses:  unaryExpr { "&&" unaryExpr }
func parseAndExpr(s *scanner, md protoreflect.MessageDescriptor) (Expr, error) {
	left, err := parseUnaryExpr(s, md)
	if err != nil {
		return nil, err
	}
	for {
		tok := s.scan()
		if tok.Kind != ampamp {
			s.unscan(tok)
			return left, nil
		}
		right, err := parseUnaryExpr(s, md)
		if err != nil {
			return nil, err
		}
		left = FuncAnd(left, right)
	}
}

// parseUnaryExpr parses:  "!" unaryExpr | primary
func parseUnaryExpr(s *scanner, md protoreflect.MessageDescriptor) (Expr, error) {
	tok := s.scan()
	if tok.Kind == bang {
		inner, err := parseUnaryExpr(s, md)
		if err != nil {
			return nil, err
		}
		return FuncNot(inner), nil
	}
	s.unscan(tok)
	return parsePrimary(s, md)
}

// parsePrimary parses:
//
//	comparison   (relPath comparator atom)
//	truthyCheck  (relPath alone)
//	"(" orExpr ")"
//	atom comparator atom  (for atom-first comparisons like 42 < .field)
func parsePrimary(s *scanner, md protoreflect.MessageDescriptor) (Expr, error) {
	tok := s.scan()

	// Grouped sub-expression: "(" orExpr ")"
	if tok.Kind == oparen {
		inner, err := parseOrExpr(s, md)
		if err != nil {
			return nil, err
		}
		close := s.scan()
		if close.Kind != cparen {
			return nil, fmt.Errorf("predicate parse: expected ')' at position %d, got %s", close.Pos, describeToken(close))
		}
		return inner, nil
	}

	s.unscan(tok)
	left, err := parseAtom(s, md)
	if err != nil {
		return nil, err
	}

	// Try to parse a comparison operator.
	op := s.scan()
	cmpFunc := comparatorFunc(op.Kind)
	if cmpFunc == nil {
		// No comparator — this is a truthy check (e.g. just ".field").
		s.unscan(op)
		return left, nil
	}

	right, err := parseAtom(s, md)
	if err != nil {
		return nil, err
	}
	return cmpFunc(left, right), nil
}

// parseAtom parses a single value:
//
//	relPath    ".field" or ".field.sub"
//	stringLit  "hello" or 'hello'
//	intLit     42, -1, 0xFF
//	floatLit   3.14
//	"true" / "false"
func parseAtom(s *scanner, md protoreflect.MessageDescriptor) (Expr, error) {
	tok := s.scan()

	switch tok.Kind {
	case dot:
		// Relative path: .field or .field.sub
		return parseRelPath(s, md)

	case strlit:
		return Literal(ScalarString(tok.Text), protoreflect.StringKind), nil

	case intlit:
		n, err := strconv.ParseInt(tok.Text, 0, 64)
		if err != nil {
			return nil, fmt.Errorf("predicate parse: invalid integer %q at position %d: %v", tok.Text, tok.Pos, err)
		}
		return Literal(ScalarInt64(n), protoreflect.Int64Kind), nil

	case floatlit:
		f, err := strconv.ParseFloat(tok.Text, 64)
		if err != nil {
			return nil, fmt.Errorf("predicate parse: invalid float %q at position %d: %v", tok.Text, tok.Pos, err)
		}
		return Literal(ScalarFloat64(f), protoreflect.DoubleKind), nil

	case ident:
		switch tok.Text {
		case "true":
			return Literal(ScalarBool(true), protoreflect.BoolKind), nil
		case "false":
			return Literal(ScalarBool(false), protoreflect.BoolKind), nil
		default:
			return nil, fmt.Errorf("predicate parse: unexpected identifier %q at position %d (did you forget a leading '.'?)", tok.Text, tok.Pos)
		}

	default:
		return nil, fmt.Errorf("predicate parse: expected value at position %d, got %s", tok.Pos, describeToken(tok))
	}
}

// parseRelPath parses a dotted relative path after the leading "." has been
// consumed. Returns a [FilterPathRef] expression with resolved field descriptors.
//
// Examples: after consuming ".", sees "name" → path "name"
//
//	after consuming ".", sees "inner" "." "id" → path "inner.id"
func parseRelPath(s *scanner, md protoreflect.MessageDescriptor) (Expr, error) {
	tok := s.scan()
	if tok.Kind != ident {
		return nil, fmt.Errorf("predicate parse: expected field name after '.' at position %d, got %s", tok.Pos, describeToken(tok))
	}
	path := tok.Text

	// Validate the first field exists in the message descriptor.
	fd := md.Fields().ByTextName(tok.Text)
	if fd == nil {
		return nil, fmt.Errorf("predicate parse: field %q not found in %s", tok.Text, md.FullName())
	}
	fields := []protoreflect.FieldDescriptor{fd}

	// Consume additional ".ident" segments.
	for {
		dotTok := s.scan()
		if dotTok.Kind != dot {
			s.unscan(dotTok)
			break
		}
		field := s.scan()
		if field.Kind != ident {
			return nil, fmt.Errorf("predicate parse: expected field name after '.' at position %d, got %s", field.Pos, describeToken(field))
		}
		path += "." + field.Text

		// Validate the sub-field exists.
		if fd.Message() == nil {
			return nil, fmt.Errorf("predicate parse: field %q is not a message, cannot access sub-field %q", path[:len(path)-len(field.Text)-1], field.Text)
		}
		subFd := fd.Message().Fields().ByTextName(field.Text)
		if subFd == nil {
			return nil, fmt.Errorf("predicate parse: field %q not found in %s", field.Text, fd.Message().FullName())
		}
		fd = subFd
		fields = append(fields, fd)
	}

	return FilterPathRef(path, fields...), nil
}

// comparatorFunc returns the Func constructor for a comparison token,
// or nil if the token is not a comparator.
func comparatorFunc(k tokenKind) func(a, b Expr) Expr {
	switch k {
	case eqeq:
		return FuncEq
	case bangeq:
		return FuncNe
	case langle:
		return FuncLt
	case langleeq:
		return FuncLe
	case rangle:
		return FuncGt
	case rangleeq:
		return FuncGe
	default:
		return nil
	}
}

// unscan pushes a token back so the next scan() call returns it.
// Only one token of look-ahead is supported.
func (s *scanner) unscan(tok *token) {
	s.pos = tok.Pos
}

// describeToken returns a human-readable description of a token for error messages.
func describeToken(tok *token) string {
	switch tok.Kind {
	case ident:
		return fmt.Sprintf("identifier %q", tok.Text)
	case intlit:
		return fmt.Sprintf("integer %s", tok.Text)
	case floatlit:
		return fmt.Sprintf("float %s", tok.Text)
	case strlit:
		return fmt.Sprintf("string %q", tok.Text)
	case dot:
		return "'.'"
	case oparen:
		return "'('"
	case cparen:
		return "')'"
	case obrack:
		return "'['"
	case cbrack:
		return "']'"
	case colon:
		return "':'"
	case asterisk:
		return "'*'"
	case pipe:
		return "'|'"
	case question:
		return "'?'"
	case eqeq:
		return "'=='"
	case bangeq:
		return "'!='"
	case langle:
		return "'<'"
	case langleeq:
		return "'<='"
	case rangle:
		return "'>'"
	case rangleeq:
		return "'>='"
	case comma:
		return "','"
	case bang:
		return "'!'"
	case ampamp:
		return "'&&'"
	case pipepipe:
		return "'||'"
	case illegal:
		return fmt.Sprintf("illegal %q", tok.Text)
	case eof:
		return "end of input"
	default:
		return fmt.Sprintf("token(%d)", tok.Kind)
	}
}
