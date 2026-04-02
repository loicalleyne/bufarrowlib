package pbpath

import (
	"fmt"
	"strconv"

	"google.golang.org/protobuf/reflect/protoreflect"
)

// ParsePipeline parses a jq-style pipeline string against a message descriptor
// and returns a compiled [Pipeline] ready for execution.
//
// Grammar:
//
//	pipeline       = comma_expr [ "as" "$" ident "|" pipeline ] { "|" comma_expr [ "as" "$" ident "|" pipeline ] }
//	comma_expr     = alt_expr { "," alt_expr }
//	alt_expr       = or_expr { "//" or_expr }
//	or_expr        = and_expr { "or" and_expr }
//	and_expr       = compare_expr { "and" compare_expr }
//	compare_expr   = add_expr [ ("==" | "!=" | "<" | "<=" | ">" | ">=") add_expr ]
//	add_expr       = mul_expr { ("+" | "-") mul_expr }
//	mul_expr       = postfix_expr { ("*" | "/" | "%") postfix_expr }
//	postfix_expr   = primary { suffix } [ "?" ]
//	suffix         = "." ident           // field access
//	               | "[" "]"             // iterate
//	               | "[" integer "]"     // index
//	primary        = "."                 // identity (or start of path/iterate/index)
//	               | "." ident           // field access on input
//	               | "." "[" "]"         // iterate on input
//	               | "." "[" int "]"     // index on input
//	               | "[" pipeline "]"    // collect
//	               | "(" pipeline ")"    // grouping
//	               | ident [ "(" pipeline { ";" pipeline } ")" ]  // function call
//	               | "$" ident           // variable reference
//	               | "-" primary         // unary negation
//	               | "!" primary         // unary not
//	               | "@" ident           // format string (@base64, @csv, @text, etc.)
//	               | "if" expr "then" pipeline {"elif" expr "then" pipeline} ["else" pipeline] "end"
//	               | "try" primary ["catch" primary]
//	               | "reduce" postfix_expr "as" "$" ident "(" pipeline ";" pipeline ")"
//	               | "foreach" postfix_expr "as" "$" ident "(" pipeline ";" pipeline [";" pipeline] ")"
//	               | "label" "$" ident "|" pipeline
//	               | "break" "$" ident
//	               | "def" ident [ "(" ident { ";" ident } ")" ] ":" pipeline ";" pipeline  // user-defined function
//	               | "{" [ obj_entry { "," obj_entry } [","] ] "}"  // object construction
//	               | string_interp       // string interpolation
//	               | literal
//	string_interp  = strbegin { pipeline strmid } pipeline strend  // "text \(expr) text"
//	obj_entry      = ident ":" alt_expr                       // static key
//	               | string ":" alt_expr                      // string key
//	               | "(" pipeline ")" ":" alt_expr            // dynamic key
//	               | ident                                    // shorthand for ident: .ident
//	literal        = string | integer | float | "true" | "false" | "null"
func ParsePipeline(md protoreflect.MessageDescriptor, input string) (*Pipeline, error) {
	if md == nil {
		return nil, fmt.Errorf("ParsePipeline: message descriptor must be non-nil")
	}
	pp := &pipeParser{
		s:    &scanner{buf: []byte(input)},
		md:   md,
		desc: md,
	}
	exprs, err := pp.parsePipeline()
	if err != nil {
		return nil, fmt.Errorf("pipeline %q: %v", input, err)
	}
	// Expect EOF.
	tok := pp.s.scan()
	if tok.Kind != eof {
		return nil, fmt.Errorf("pipeline %q: unexpected token %s at position %d after complete pipeline",
			input, pipeDescribeToken(tok), tok.Pos)
	}
	return &Pipeline{exprs: exprs, md: md}, nil
}

// pipeParser holds state for the recursive descent pipeline parser.
type pipeParser struct {
	s    *scanner
	md   protoreflect.MessageDescriptor
	desc protoreflect.Descriptor // cursor — current schema position
	// unscanBuf stores a token that was put back via pipeUnscan.
	unscanBuf *token
	// userFuncs tracks user-defined functions from `def` for parse-time lookup.
	userFuncs []*pipeUserFunc
}

// pipeScan returns the next token, respecting any unscan buffer.
func (pp *pipeParser) pipeScan() *token {
	if pp.unscanBuf != nil {
		t := pp.unscanBuf
		pp.unscanBuf = nil
		return t
	}
	return pp.s.scan()
}

// pipeUnscan puts a token back so the next pipeScan returns it.
func (pp *pipeParser) pipeUnscan(t *token) {
	pp.unscanBuf = t
}

// ── Pipeline: comma_expr { "|" comma_expr } ─────────────────────────────

func (pp *pipeParser) parsePipeline() ([]PipeExpr, error) {
	savedDesc := pp.desc // cursor before the expr
	first, err := pp.parseCommaExpr()
	if err != nil {
		return nil, err
	}
	// Check for "as $name | body" variable binding.
	if bind, err := pp.tryParseVarBind(first, savedDesc); err != nil {
		return nil, err
	} else if bind != nil {
		return []PipeExpr{bind}, nil
	}
	exprs := []PipeExpr{first}
	for {
		tok := pp.pipeScan()
		if tok.Kind != pipe {
			pp.pipeUnscan(tok)
			return exprs, nil
		}
		savedDesc = pp.desc
		next, err := pp.parseCommaExpr()
		if err != nil {
			return nil, err
		}
		if bind, err := pp.tryParseVarBind(next, savedDesc); err != nil {
			return nil, err
		} else if bind != nil {
			exprs = append(exprs, bind)
			return exprs, nil
		}
		exprs = append(exprs, next)
	}
}

// tryParseVarBind checks for "as $name | body" after an expression.
// Returns nil, nil if no "as" keyword follows. descBeforeExpr is the
// schema cursor from before the expr was parsed — the body should be
// parsed with this cursor since at runtime the body receives the same
// input as the expr, not the expr's output.
func (pp *pipeParser) tryParseVarBind(expr PipeExpr, descBeforeExpr protoreflect.Descriptor) (PipeExpr, error) {
	tok := pp.pipeScan()
	if tok.Kind != ident || tok.Text != "as" {
		pp.pipeUnscan(tok)
		return nil, nil
	}
	dollarTok := pp.pipeScan()
	if dollarTok.Kind != dollar {
		return nil, fmt.Errorf("expected '$' after 'as' at position %d, got %s",
			dollarTok.Pos, pipeDescribeToken(dollarTok))
	}
	nameTok := pp.pipeScan()
	if nameTok.Kind != ident {
		return nil, fmt.Errorf("expected variable name after '$' at position %d, got %s",
			nameTok.Pos, pipeDescribeToken(nameTok))
	}
	pipeTok := pp.pipeScan()
	if pipeTok.Kind != pipe {
		return nil, fmt.Errorf("expected '|' after 'as $%s' at position %d, got %s",
			nameTok.Text, pipeTok.Pos, pipeDescribeToken(pipeTok))
	}
	// Restore the cursor to before the expr, since the body receives
	// the same input as the expr (not the expr's output).
	pp.desc = descBeforeExpr
	// The entire remaining pipeline becomes the body.
	bodyExprs, err := pp.parsePipeline()
	if err != nil {
		return nil, err
	}
	body := &Pipeline{exprs: bodyExprs, md: pp.md}
	return &pipeVarBind{name: nameTok.Text, expr: expr, body: body}, nil
}

// ── Comma: alt_expr { "," alt_expr } ────────────────────────────────────

func (pp *pipeParser) parseCommaExpr() (PipeExpr, error) {
	left, err := pp.parseAltExpr()
	if err != nil {
		return nil, err
	}
	for {
		tok := pp.pipeScan()
		if tok.Kind != comma {
			pp.pipeUnscan(tok)
			return left, nil
		}
		right, err := pp.parseAltExpr()
		if err != nil {
			return nil, err
		}
		left = &pipeComma{left: left, right: right}
	}
}

// ── Alternative: or_expr { "//" or_expr } ───────────────────────────────

func (pp *pipeParser) parseAltExpr() (PipeExpr, error) {
	left, err := pp.parseOrExpr()
	if err != nil {
		return nil, err
	}
	for {
		tok := pp.pipeScan()
		if tok.Kind == slashslash {
			right, err := pp.parseOrExpr()
			if err != nil {
				return nil, err
			}
			left = &pipeAlternative{left: left, right: right}
			continue
		}
		pp.pipeUnscan(tok)
		return left, nil
	}
}

// ── Or: and_expr { "or" and_expr } ─────────────────────────────────────

func (pp *pipeParser) parseOrExpr() (PipeExpr, error) {
	left, err := pp.parseAndExpr()
	if err != nil {
		return nil, err
	}
	for {
		tok := pp.pipeScan()
		if tok.Kind == ident && tok.Text == "or" {
			right, err := pp.parseAndExpr()
			if err != nil {
				return nil, err
			}
			left = &pipeBoolOr{left: left, right: right}
			continue
		}
		pp.pipeUnscan(tok)
		return left, nil
	}
}

// ── And: compare_expr { "and" compare_expr } ───────────────────────────

func (pp *pipeParser) parseAndExpr() (PipeExpr, error) {
	left, err := pp.parseCompareExpr()
	if err != nil {
		return nil, err
	}
	for {
		tok := pp.pipeScan()
		if tok.Kind == ident && tok.Text == "and" {
			right, err := pp.parseCompareExpr()
			if err != nil {
				return nil, err
			}
			left = &pipeBoolAnd{left: left, right: right}
			continue
		}
		pp.pipeUnscan(tok)
		return left, nil
	}
}

// ── Compare: add_expr [ op add_expr ] ───────────────────────────────────

func (pp *pipeParser) parseCompareExpr() (PipeExpr, error) {
	left, err := pp.parseAddExpr()
	if err != nil {
		return nil, err
	}
	tok := pp.pipeScan()
	switch tok.Kind {
	case eqeq, bangeq, langle, langleeq, rangle, rangleeq:
		right, err := pp.parseAddExpr()
		if err != nil {
			return nil, err
		}
		return &pipeCompare{op: tok.Kind, left: left, right: right}, nil
	default:
		pp.pipeUnscan(tok)
		return left, nil
	}
}

// ── Add: mul_expr { ("+" | "-") mul_expr } ──────────────────────────────

func (pp *pipeParser) parseAddExpr() (PipeExpr, error) {
	left, err := pp.parseMulExpr()
	if err != nil {
		return nil, err
	}
	for {
		tok := pp.pipeScan()
		if tok.Kind == plus {
			right, err := pp.parseMulExpr()
			if err != nil {
				return nil, err
			}
			left = &pipeArith{op: plus, left: left, right: right}
			continue
		}
		if tok.Kind == minus {
			right, err := pp.parseMulExpr()
			if err != nil {
				return nil, err
			}
			left = &pipeArith{op: minus, left: left, right: right}
			continue
		}
		// Handle scanner ambiguity: -N was consumed as a negative intlit/floatlit.
		// In binary context (we already have a left operand), reinterpret as subtraction.
		if (tok.Kind == intlit || tok.Kind == floatlit) && len(tok.Text) > 0 && tok.Text[0] == '-' {
			posText := tok.Text[1:]
			posTok := &token{Kind: tok.Kind, Pos: tok.Pos + 1, Text: posText}
			pp.pipeUnscan(posTok)
			right, err := pp.parseMulExpr()
			if err != nil {
				return nil, err
			}
			left = &pipeArith{op: minus, left: left, right: right}
			continue
		}
		pp.pipeUnscan(tok)
		return left, nil
	}
}

// ── Mul: postfix_expr { ("*" | "/" | "%") postfix_expr } ────────────────

func (pp *pipeParser) parseMulExpr() (PipeExpr, error) {
	left, err := pp.parsePostfixExpr()
	if err != nil {
		return nil, err
	}
	for {
		tok := pp.pipeScan()
		switch tok.Kind {
		case asterisk, slash, percent:
			right, err := pp.parsePostfixExpr()
			if err != nil {
				return nil, err
			}
			left = &pipeArith{op: tok.Kind, left: left, right: right}
		default:
			pp.pipeUnscan(tok)
			return left, nil
		}
	}
}

// ── Postfix: primary { suffix } [ "?" ] ─────────────────────────────────

func (pp *pipeParser) parsePostfixExpr() (PipeExpr, error) {
	primary, curDesc, err := pp.parsePrimary()
	if err != nil {
		return nil, err
	}
	// Apply suffixes: .field, [], [n]
	for {
		tok := pp.pipeScan()
		switch tok.Kind {
		case dot:
			// .field suffix
			nameTok := pp.pipeScan()
			if nameTok.Kind != ident {
				return nil, fmt.Errorf("expected field name after '.' at position %d, got %s",
					nameTok.Pos, pipeDescribeToken(nameTok))
			}
			if curDesc == nil {
				// Dynamic field access — no static schema context (e.g., variable reference).
				primary = &pipeChain{left: primary, right: &pipeDynamicAccess{field: nameTok.Text}}
				// curDesc remains nil for subsequent accesses.
				continue
			}
			fd, newDesc, err := pp.resolveField(curDesc, nameTok.Text, nameTok.Pos)
			if err != nil {
				return nil, err
			}
			curDesc = newDesc
			// Wrap: apply field access to the result of previous expr.
			primary = &pipeChain{left: primary, right: &pipePathAccess{fields: []protoreflect.FieldDescriptor{fd}}}
		case obrack:
			// [] or [n] suffix
			inner := pp.pipeScan()
			if inner.Kind == cbrack {
				// [] — iterate
				curDesc = pp.advanceDescForIterate(curDesc)
				primary = &pipeChain{left: primary, right: pipeIterate{}}
			} else if inner.Kind == intlit {
				// [n]
				idx, err := strconv.ParseInt(inner.Text, 0, 64)
				if err != nil {
					return nil, fmt.Errorf("invalid index %q at position %d", inner.Text, inner.Pos)
				}
				close := pp.pipeScan()
				if close.Kind != cbrack {
					return nil, fmt.Errorf("expected ']' at position %d, got %s", close.Pos, pipeDescribeToken(close))
				}
				curDesc = pp.advanceDescForIterate(curDesc)
				primary = &pipeChain{left: primary, right: &pipeIndex{index: int(idx)}}
			} else {
				return nil, fmt.Errorf("expected ']' or integer after '[' at position %d, got %s",
					inner.Pos, pipeDescribeToken(inner))
			}
		default:
			pp.pipeUnscan(tok)
			// Check for optional '?' suffix.
			qTok := pp.pipeScan()
			if qTok.Kind == question {
				primary = &pipeOptional{inner: primary}
			} else {
				pp.pipeUnscan(qTok)
			}
			// Update the parser cursor so that subsequent pipe stages
			// (after |) resolve fields against the correct descriptor.
			// When curDesc is nil (e.g., after object construction, literals,
			// or variables), clear pp.desc so subsequent field access uses
			// dynamic resolution rather than an incorrect static schema.
			pp.desc = curDesc
			return primary, nil
		}
	}
}

// ── Primary expressions ─────────────────────────────────────────────────

// parsePrimary returns the parsed expression and the schema descriptor
// at the "output" of that expression (for suffix resolution).
func (pp *pipeParser) parsePrimary() (PipeExpr, protoreflect.Descriptor, error) {
	tok := pp.pipeScan()
	switch tok.Kind {
	case dot:
		return pp.parseDotPrimary()
	case obrack:
		return pp.parseCollect()
	case oparen:
		return pp.parseGrouping()
	case ident:
		return pp.parseIdentPrimary(tok)
	case intlit:
		return pp.parseLiteralInt(tok)
	case floatlit:
		return pp.parseLiteralFloat(tok)
	case strlit:
		return pp.parseLiteralString(tok)
	case strbegin:
		return pp.parseStringInterp(tok)
	case bang:
		// Unary not: !expr
		child, desc, err := pp.parsePrimary()
		if err != nil {
			return nil, nil, err
		}
		return &pipeChain{left: child, right: pipeNot{}}, desc, nil
	case minus:
		// Unary negation: -expr (standalone minus not consumed by decimal regex)
		child, desc, err := pp.parsePrimary()
		if err != nil {
			return nil, nil, err
		}
		return &pipeNegate{inner: child}, desc, nil
	case dollar:
		// Variable reference: $name
		nameTok := pp.pipeScan()
		if nameTok.Kind != ident {
			return nil, nil, fmt.Errorf("expected variable name after '$' at position %d, got %s",
				nameTok.Pos, pipeDescribeToken(nameTok))
		}
		return &pipeVarRef{name: nameTok.Text}, nil, nil
	case at:
		// Format string: @base64, @csv, @json, etc.
		nameTok := pp.pipeScan()
		if nameTok.Kind != ident {
			return nil, nil, fmt.Errorf("expected format name after '@' at position %d, got %s",
				nameTok.Pos, pipeDescribeToken(nameTok))
		}
		fn, ok := pipeFormatStrings[nameTok.Text]
		if !ok {
			return nil, nil, fmt.Errorf("unknown format string @%s at position %d", nameTok.Text, tok.Pos)
		}
		return &pipeBuiltin{name: "@" + nameTok.Text, fn: fn}, nil, nil
	case obrace:
		return pp.parseObjectConstruct()
	default:
		return nil, nil, fmt.Errorf("unexpected token %s at position %d",
			pipeDescribeToken(tok), tok.Pos)
	}
}

// parseDotPrimary handles tokens starting with ".":
//   - "."          → identity
//   - ".field..."  → path access
//   - ".[]"        → iterate
//   - ".[n]"       → index
func (pp *pipeParser) parseDotPrimary() (PipeExpr, protoreflect.Descriptor, error) {
	tok := pp.pipeScan()
	switch tok.Kind {
	case ident:
		// .field — possibly chained: .field.sub.deep
		return pp.parseDotPath(tok)
	case obrack:
		// .[] or .[n]
		return pp.parseDotBracket()
	default:
		// Just "." — identity.
		pp.pipeUnscan(tok)
		return pipeIdentity{}, pp.desc, nil
	}
}

// parseDotPath handles ".field" possibly chained to ".field.sub.deep".
// Also handles ".field[]" and ".field[n]" as suffixes.
func (pp *pipeParser) parseDotPath(firstIdent *token) (PipeExpr, protoreflect.Descriptor, error) {
	if pp.desc == nil {
		// No schema context — use dynamic access (e.g., after object construction).
		expr := PipeExpr(&pipeDynamicAccess{field: firstIdent.Text})
		for {
			tok := pp.pipeScan()
			if tok.Kind != dot {
				pp.pipeUnscan(tok)
				return expr, nil, nil
			}
			nameTok := pp.pipeScan()
			if nameTok.Kind != ident {
				pp.pipeUnscan(nameTok)
				pp.pipeUnscan(tok)
				return expr, nil, nil
			}
			expr = &pipeChain{left: expr, right: &pipeDynamicAccess{field: nameTok.Text}}
		}
	}
	fd, curDesc, err := pp.resolveField(pp.desc, firstIdent.Text, firstIdent.Pos)
	if err != nil {
		return nil, nil, err
	}
	fields := []protoreflect.FieldDescriptor{fd}

	// Consume chained ".field" segments as a single pipePathAccess.
	for {
		tok := pp.pipeScan()
		if tok.Kind != dot {
			pp.pipeUnscan(tok)
			break
		}
		nameTok := pp.pipeScan()
		if nameTok.Kind != ident {
			// Could be .field.[] or .field.[n] — unscan both and break.
			pp.pipeUnscan(nameTok)
			pp.pipeUnscan(tok)
			break
		}
		nextFD, nextDesc, err := pp.resolveField(curDesc, nameTok.Text, nameTok.Pos)
		if err != nil {
			return nil, nil, err
		}
		fields = append(fields, nextFD)
		curDesc = nextDesc
	}

	return &pipePathAccess{fields: fields}, curDesc, nil
}

// parseDotBracket handles ".[]" and ".[n]" after the leading dot.
func (pp *pipeParser) parseDotBracket() (PipeExpr, protoreflect.Descriptor, error) {
	tok := pp.pipeScan()
	if tok.Kind == cbrack {
		// .[] — iterate on identity input.
		desc := pp.advanceDescForIterate(pp.desc)
		return pipeIterate{}, desc, nil
	}
	if tok.Kind == intlit {
		// .[n] — index on identity input.
		idx, err := strconv.ParseInt(tok.Text, 0, 64)
		if err != nil {
			return nil, nil, fmt.Errorf("invalid index %q at position %d", tok.Text, tok.Pos)
		}
		close := pp.pipeScan()
		if close.Kind != cbrack {
			return nil, nil, fmt.Errorf("expected ']' at position %d, got %s",
				close.Pos, pipeDescribeToken(close))
		}
		desc := pp.advanceDescForIterate(pp.desc)
		return &pipeIndex{index: int(idx)}, desc, nil
	}
	return nil, nil, fmt.Errorf("expected ']' or integer after '.[' at position %d, got %s",
		tok.Pos, pipeDescribeToken(tok))
}

// parseCollect handles "[" pipeline "]" — collect outputs into a list.
func (pp *pipeParser) parseCollect() (PipeExpr, protoreflect.Descriptor, error) {
	savedDesc := pp.desc
	inner, err := pp.parsePipeline()
	if err != nil {
		return nil, nil, err
	}
	close := pp.pipeScan()
	if close.Kind != cbrack {
		return nil, nil, fmt.Errorf("expected ']' at position %d, got %s",
			close.Pos, pipeDescribeToken(close))
	}
	pp.desc = savedDesc // restore cursor after collect
	pipeline := &Pipeline{exprs: inner, md: pp.md}
	return &pipeCollect{inner: pipeline}, savedDesc, nil
}

// parseGrouping handles "(" pipeline ")".
func (pp *pipeParser) parseGrouping() (PipeExpr, protoreflect.Descriptor, error) {
	savedDesc := pp.desc
	inner, err := pp.parsePipeline()
	if err != nil {
		return nil, nil, err
	}
	close := pp.pipeScan()
	if close.Kind != cparen {
		return nil, nil, fmt.Errorf("expected ')' at position %d, got %s",
			close.Pos, pipeDescribeToken(close))
	}
	// A grouped pipeline acts as a single expression.
	// Wrap multiple pipe stages into a pipeGroup; single stage passes through.
	if len(inner) == 1 {
		return inner[0], savedDesc, nil
	}
	pipeline := &Pipeline{exprs: inner, md: pp.md}
	return &pipeGroupExpr{inner: pipeline}, savedDesc, nil
}

// parseObjectConstruct parses {key: pipeline, ...} object construction.
// The opening '{' has already been consumed.
//
// Syntax:
//
//	{} — empty object
//	{ident: pipeline, ...} — static string key
//	{"lit": pipeline, ...} — string literal key
//	{(expr): pipeline, ...} — dynamic key
//	{ident, ...} — shorthand for {ident: .ident}
func (pp *pipeParser) parseObjectConstruct() (PipeExpr, protoreflect.Descriptor, error) {
	// Check for empty object: {}
	tok := pp.pipeScan()
	if tok.Kind == cbrace {
		return &pipeObjectConstruct{entries: nil}, nil, nil
	}
	pp.pipeUnscan(tok)

	var entries []pipeObjEntry

	for {
		entry, err := pp.parseObjEntry()
		if err != nil {
			return nil, nil, err
		}
		entries = append(entries, entry)

		// Expect ',' or '}'.
		sep := pp.pipeScan()
		if sep.Kind == cbrace {
			break
		}
		if sep.Kind != comma {
			return nil, nil, fmt.Errorf("expected ',' or '}' in object at position %d, got %s",
				sep.Pos, pipeDescribeToken(sep))
		}
		// Allow trailing comma: {a:1,}
		peek := pp.pipeScan()
		if peek.Kind == cbrace {
			break
		}
		pp.pipeUnscan(peek)
	}

	return &pipeObjectConstruct{entries: entries}, nil, nil
}

// parseObjEntry parses a single key: value entry in an object constructor.
func (pp *pipeParser) parseObjEntry() (pipeObjEntry, error) {
	tok := pp.pipeScan()

	switch tok.Kind {
	case ident:
		// Could be "key: value" or shorthand "key".
		next := pp.pipeScan()
		if next.Kind == colon {
			// key: value — parse at alternative level (comma separates entries).
			savedDesc := pp.desc
			valExpr, err := pp.parseAltExpr()
			if err != nil {
				return pipeObjEntry{}, err
			}
			pp.desc = savedDesc
			valPipeline := &Pipeline{exprs: []PipeExpr{valExpr}, md: pp.md}
			return pipeObjEntry{staticKey: tok.Text, value: valPipeline}, nil
		}
		// Shorthand: just {name} meaning {name: .name}
		pp.pipeUnscan(next)
		return pipeObjEntry{staticKey: tok.Text}, nil

	case strlit:
		// "key": value
		next := pp.pipeScan()
		if next.Kind != colon {
			return pipeObjEntry{}, fmt.Errorf("expected ':' after string key at position %d, got %s",
				next.Pos, pipeDescribeToken(next))
		}
		savedDesc := pp.desc
		valExpr, err := pp.parseAltExpr()
		if err != nil {
			return pipeObjEntry{}, err
		}
		pp.desc = savedDesc
		valPipeline := &Pipeline{exprs: []PipeExpr{valExpr}, md: pp.md}
		return pipeObjEntry{staticKey: tok.Text, value: valPipeline}, nil

	case oparen:
		// Dynamic key: (expr): value
		savedDesc := pp.desc
		keyExprs, err := pp.parsePipeline()
		if err != nil {
			return pipeObjEntry{}, err
		}
		pp.desc = savedDesc
		close := pp.pipeScan()
		if close.Kind != cparen {
			return pipeObjEntry{}, fmt.Errorf("expected ')' after dynamic key at position %d, got %s",
				close.Pos, pipeDescribeToken(close))
		}
		colonTok := pp.pipeScan()
		if colonTok.Kind != colon {
			return pipeObjEntry{}, fmt.Errorf("expected ':' after dynamic key expression at position %d, got %s",
				colonTok.Pos, pipeDescribeToken(colonTok))
		}
		valExpr, err := pp.parseAltExpr()
		if err != nil {
			return pipeObjEntry{}, err
		}
		keyPipeline := &Pipeline{exprs: keyExprs, md: pp.md}
		valPipeline := &Pipeline{exprs: []PipeExpr{valExpr}, md: pp.md}
		return pipeObjEntry{keyExpr: keyPipeline, value: valPipeline}, nil

	case at:
		// @format as key: @base64: value, etc. — unusual but handle identifier-like
		nameTok := pp.pipeScan()
		if nameTok.Kind != ident {
			return pipeObjEntry{}, fmt.Errorf("expected format name after '@' at position %d, got %s",
				nameTok.Pos, pipeDescribeToken(nameTok))
		}
		key := "@" + nameTok.Text
		next := pp.pipeScan()
		if next.Kind == colon {
			savedDesc := pp.desc
			valExpr, err := pp.parseAltExpr()
			if err != nil {
				return pipeObjEntry{}, err
			}
			pp.desc = savedDesc
			valPipeline := &Pipeline{exprs: []PipeExpr{valExpr}, md: pp.md}
			return pipeObjEntry{staticKey: key, value: valPipeline}, nil
		}
		pp.pipeUnscan(next)
		return pipeObjEntry{staticKey: key}, nil

	default:
		return pipeObjEntry{}, fmt.Errorf("expected object key (identifier, string, or parenthesized expression) at position %d, got %s",
			tok.Pos, pipeDescribeToken(tok))
	}
}

// parseIdentPrimary handles identifiers: bare builtins, "true"/"false"/"null",
// keyword constructs (if, try, reduce, foreach, label, break), and function calls.
func (pp *pipeParser) parseIdentPrimary(tok *token) (PipeExpr, protoreflect.Descriptor, error) {
	switch tok.Text {
	case "true":
		return &pipeLiteral{val: ScalarBool(true)}, nil, nil
	case "false":
		return &pipeLiteral{val: ScalarBool(false)}, nil, nil
	case "null":
		return &pipeLiteral{val: Null()}, nil, nil
	case "if":
		return pp.parseIf()
	case "try":
		return pp.parseTry()
	case "reduce":
		return pp.parseReduce()
	case "foreach":
		return pp.parseForeach()
	case "label":
		return pp.parseLabel()
	case "break":
		return pp.parseBreak()
	case "def":
		return pp.parseDef()
	}

	// Check for function call: ident "(" ... ")"
	next := pp.pipeScan()
	if next.Kind == oparen {
		return pp.parseFuncCall(tok)
	}
	pp.pipeUnscan(next)

	// Bare built-in (no parens).
	if builtin := lookupBuiltin(tok.Text); builtin != nil {
		return builtin, nil, nil // cursor type becomes unknown after a function
	}

	// User-defined zero-arg function (bare name).
	for _, fn := range pp.userFuncs {
		if fn.name == tok.Text && len(fn.params) == 0 {
			return &pipeUserFuncCall{name: fn.name, arity: 0, args: nil}, nil, nil
		}
	}

	// Check if this is a function that requires arguments.
	if tok.Text == "select" {
		return nil, nil, fmt.Errorf("expected '(' after 'select' at position %d", tok.Pos)
	}
	if _, ok := pipeFuncsWith1Arg[tok.Text]; ok {
		return nil, nil, fmt.Errorf("function %q requires arguments: %s(...) at position %d", tok.Text, tok.Text, tok.Pos)
	}
	if _, ok := pipeFuncsWith2Args[tok.Text]; ok {
		return nil, nil, fmt.Errorf("function %q requires arguments: %s(...; ...) at position %d", tok.Text, tok.Text, tok.Pos)
	}

	return nil, nil, fmt.Errorf("unknown function or identifier %q at position %d", tok.Text, tok.Pos)
}

// ── Keyword parsers ─────────────────────────────────────────────────────

// parseIf handles: if cond then body {elif cond then body} [else body] end
func (pp *pipeParser) parseIf() (PipeExpr, protoreflect.Descriptor, error) {
	// Parse condition (full pipeline expression up to "then").
	condExpr, err := pp.parseCommaExpr()
	if err != nil {
		return nil, nil, fmt.Errorf("if: condition: %v", err)
	}
	// Expect "then".
	tok := pp.pipeScan()
	if tok.Kind != ident || tok.Text != "then" {
		return nil, nil, fmt.Errorf("expected 'then' after if-condition at position %d, got %s",
			tok.Pos, pipeDescribeToken(tok))
	}
	// Parse then-body as a full pipeline.
	thenExprs, err := pp.parsePipeline()
	if err != nil {
		return nil, nil, fmt.Errorf("if: then body: %v", err)
	}
	thenPipe := &Pipeline{exprs: thenExprs, md: pp.md}

	result := &pipeIfThenElse{cond: condExpr, thenBody: thenPipe}

	// Look for elif / else / end.
	for {
		tok = pp.pipeScan()
		if tok.Kind != ident {
			return nil, nil, fmt.Errorf("expected 'elif', 'else', or 'end' at position %d, got %s",
				tok.Pos, pipeDescribeToken(tok))
		}
		switch tok.Text {
		case "elif":
			elifCond, err := pp.parseCommaExpr()
			if err != nil {
				return nil, nil, fmt.Errorf("elif: condition: %v", err)
			}
			thenTok := pp.pipeScan()
			if thenTok.Kind != ident || thenTok.Text != "then" {
				return nil, nil, fmt.Errorf("expected 'then' after elif-condition at position %d, got %s",
					thenTok.Pos, pipeDescribeToken(thenTok))
			}
			elifBody, err := pp.parsePipeline()
			if err != nil {
				return nil, nil, fmt.Errorf("elif: body: %v", err)
			}
			result.elifs = append(result.elifs, pipeElif{
				cond: elifCond,
				body: &Pipeline{exprs: elifBody, md: pp.md},
			})
		case "else":
			elseExprs, err := pp.parsePipeline()
			if err != nil {
				return nil, nil, fmt.Errorf("if: else body: %v", err)
			}
			result.elseBody = &Pipeline{exprs: elseExprs, md: pp.md}
			endTok := pp.pipeScan()
			if endTok.Kind != ident || endTok.Text != "end" {
				return nil, nil, fmt.Errorf("expected 'end' after else body at position %d, got %s",
					endTok.Pos, pipeDescribeToken(endTok))
			}
			return result, nil, nil
		case "end":
			return result, nil, nil
		default:
			return nil, nil, fmt.Errorf("expected 'elif', 'else', or 'end' at position %d, got %q",
				tok.Pos, tok.Text)
		}
	}
}

// parseTry handles: try expr [catch expr]
func (pp *pipeParser) parseTry() (PipeExpr, protoreflect.Descriptor, error) {
	tryExpr, _, err := pp.parsePrimary()
	if err != nil {
		return nil, nil, fmt.Errorf("try: %v", err)
	}
	// Check for catch.
	tok := pp.pipeScan()
	if tok.Kind == ident && tok.Text == "catch" {
		catchExpr, _, err := pp.parsePrimary()
		if err != nil {
			return nil, nil, fmt.Errorf("catch: %v", err)
		}
		catchPipe := &Pipeline{exprs: []PipeExpr{catchExpr}, md: pp.md}
		return &pipeTryCatch{tryBody: tryExpr, catchBody: catchPipe}, nil, nil
	}
	pp.pipeUnscan(tok)
	return &pipeTryCatch{tryBody: tryExpr}, nil, nil
}

// parseReduce handles: reduce expr as $name (init; update)
func (pp *pipeParser) parseReduce() (PipeExpr, protoreflect.Descriptor, error) {
	// Parse the stream expression.
	streamExpr, err := pp.parsePostfixExpr()
	if err != nil {
		return nil, nil, fmt.Errorf("reduce: stream: %v", err)
	}
	// Expect "as".
	tok := pp.pipeScan()
	if tok.Kind != ident || tok.Text != "as" {
		return nil, nil, fmt.Errorf("expected 'as' after reduce expression at position %d, got %s",
			tok.Pos, pipeDescribeToken(tok))
	}
	// Expect $name.
	dollarTok := pp.pipeScan()
	if dollarTok.Kind != dollar {
		return nil, nil, fmt.Errorf("expected '$' in reduce at position %d, got %s",
			dollarTok.Pos, pipeDescribeToken(dollarTok))
	}
	nameTok := pp.pipeScan()
	if nameTok.Kind != ident {
		return nil, nil, fmt.Errorf("expected variable name in reduce at position %d, got %s",
			nameTok.Pos, pipeDescribeToken(nameTok))
	}
	// Expect "(".
	openTok := pp.pipeScan()
	if openTok.Kind != oparen {
		return nil, nil, fmt.Errorf("expected '(' after reduce variable at position %d, got %s",
			openTok.Pos, pipeDescribeToken(openTok))
	}
	// Parse init pipeline.
	initExprs, err := pp.parsePipeline()
	if err != nil {
		return nil, nil, fmt.Errorf("reduce: init: %v", err)
	}
	// Expect ";".
	semiTok := pp.pipeScan()
	if semiTok.Kind != semicolon {
		return nil, nil, fmt.Errorf("expected ';' after reduce init at position %d, got %s",
			semiTok.Pos, pipeDescribeToken(semiTok))
	}
	// Parse update pipeline.
	updateExprs, err := pp.parsePipeline()
	if err != nil {
		return nil, nil, fmt.Errorf("reduce: update: %v", err)
	}
	// Expect ")".
	closeTok := pp.pipeScan()
	if closeTok.Kind != cparen {
		return nil, nil, fmt.Errorf("expected ')' after reduce update at position %d, got %s",
			closeTok.Pos, pipeDescribeToken(closeTok))
	}
	return &pipeReduce{
		expr:   streamExpr,
		varN:   nameTok.Text,
		init:   &Pipeline{exprs: initExprs, md: pp.md},
		update: &Pipeline{exprs: updateExprs, md: pp.md},
	}, nil, nil
}

// parseForeach handles: foreach expr as $name (init; update [; extract])
func (pp *pipeParser) parseForeach() (PipeExpr, protoreflect.Descriptor, error) {
	// Parse the stream expression.
	streamExpr, err := pp.parsePostfixExpr()
	if err != nil {
		return nil, nil, fmt.Errorf("foreach: stream: %v", err)
	}
	// Expect "as".
	tok := pp.pipeScan()
	if tok.Kind != ident || tok.Text != "as" {
		return nil, nil, fmt.Errorf("expected 'as' after foreach expression at position %d, got %s",
			tok.Pos, pipeDescribeToken(tok))
	}
	// Expect $name.
	dollarTok := pp.pipeScan()
	if dollarTok.Kind != dollar {
		return nil, nil, fmt.Errorf("expected '$' in foreach at position %d, got %s",
			dollarTok.Pos, pipeDescribeToken(dollarTok))
	}
	nameTok := pp.pipeScan()
	if nameTok.Kind != ident {
		return nil, nil, fmt.Errorf("expected variable name in foreach at position %d, got %s",
			nameTok.Pos, pipeDescribeToken(nameTok))
	}
	// Expect "(".
	openTok := pp.pipeScan()
	if openTok.Kind != oparen {
		return nil, nil, fmt.Errorf("expected '(' after foreach variable at position %d, got %s",
			openTok.Pos, pipeDescribeToken(openTok))
	}
	// Parse init pipeline.
	initExprs, err := pp.parsePipeline()
	if err != nil {
		return nil, nil, fmt.Errorf("foreach: init: %v", err)
	}
	// Expect ";".
	semiTok := pp.pipeScan()
	if semiTok.Kind != semicolon {
		return nil, nil, fmt.Errorf("expected ';' after foreach init at position %d, got %s",
			semiTok.Pos, pipeDescribeToken(semiTok))
	}
	// Parse update pipeline.
	updateExprs, err := pp.parsePipeline()
	if err != nil {
		return nil, nil, fmt.Errorf("foreach: update: %v", err)
	}
	// Check for optional extract.
	var extractPipe *Pipeline
	tok = pp.pipeScan()
	if tok.Kind == semicolon {
		extractExprs, err := pp.parsePipeline()
		if err != nil {
			return nil, nil, fmt.Errorf("foreach: extract: %v", err)
		}
		extractPipe = &Pipeline{exprs: extractExprs, md: pp.md}
		tok = pp.pipeScan()
	}
	// Expect ")".
	if tok.Kind != cparen {
		return nil, nil, fmt.Errorf("expected ')' after foreach at position %d, got %s",
			tok.Pos, pipeDescribeToken(tok))
	}
	return &pipeForeach{
		expr:    streamExpr,
		varN:    nameTok.Text,
		init:    &Pipeline{exprs: initExprs, md: pp.md},
		update:  &Pipeline{exprs: updateExprs, md: pp.md},
		extract: extractPipe,
	}, nil, nil
}

// parseLabel handles: label $name | body
func (pp *pipeParser) parseLabel() (PipeExpr, protoreflect.Descriptor, error) {
	dollarTok := pp.pipeScan()
	if dollarTok.Kind != dollar {
		return nil, nil, fmt.Errorf("expected '$' after 'label' at position %d, got %s",
			dollarTok.Pos, pipeDescribeToken(dollarTok))
	}
	nameTok := pp.pipeScan()
	if nameTok.Kind != ident {
		return nil, nil, fmt.Errorf("expected label name after '$' at position %d, got %s",
			nameTok.Pos, pipeDescribeToken(nameTok))
	}
	pipeTok := pp.pipeScan()
	if pipeTok.Kind != pipe {
		return nil, nil, fmt.Errorf("expected '|' after 'label $%s' at position %d, got %s",
			nameTok.Text, pipeTok.Pos, pipeDescribeToken(pipeTok))
	}
	bodyExprs, err := pp.parsePipeline()
	if err != nil {
		return nil, nil, fmt.Errorf("label: body: %v", err)
	}
	return &pipeLabel{
		name: nameTok.Text,
		body: &Pipeline{exprs: bodyExprs, md: pp.md},
	}, nil, nil
}

// parseBreak handles: break $name
func (pp *pipeParser) parseBreak() (PipeExpr, protoreflect.Descriptor, error) {
	dollarTok := pp.pipeScan()
	if dollarTok.Kind != dollar {
		return nil, nil, fmt.Errorf("expected '$' after 'break' at position %d, got %s",
			dollarTok.Pos, pipeDescribeToken(dollarTok))
	}
	nameTok := pp.pipeScan()
	if nameTok.Kind != ident {
		return nil, nil, fmt.Errorf("expected label name after 'break $' at position %d, got %s",
			nameTok.Pos, pipeDescribeToken(nameTok))
	}
	return &pipeBreak{name: nameTok.Text}, nil, nil
}

// parseDef handles: def name: body; expr  OR  def name(params): body; expr
//
// Syntax:
//
//	def name: body; pipeline
//	def name(a; b; c): body; pipeline
//
// where a, b, c are parameter names.
func (pp *pipeParser) parseDef() (PipeExpr, protoreflect.Descriptor, error) {
	nameTok := pp.pipeScan()
	if nameTok.Kind != ident {
		return nil, nil, fmt.Errorf("expected function name after 'def' at position %d, got %s",
			nameTok.Pos, pipeDescribeToken(nameTok))
	}

	var params []string

	// Check for optional parameter list.
	next := pp.pipeScan()
	if next.Kind == oparen {
		// Parse parameter names separated by semicolons.
		for {
			paramTok := pp.pipeScan()
			if paramTok.Kind != ident {
				return nil, nil, fmt.Errorf("expected parameter name in 'def %s' at position %d, got %s",
					nameTok.Text, paramTok.Pos, pipeDescribeToken(paramTok))
			}
			params = append(params, paramTok.Text)
			sep := pp.pipeScan()
			if sep.Kind == cparen {
				break
			}
			if sep.Kind != semicolon {
				return nil, nil, fmt.Errorf("expected ';' or ')' in 'def %s' parameters at position %d, got %s",
					nameTok.Text, sep.Pos, pipeDescribeToken(sep))
			}
		}
		// Expect ':' after closing paren.
		next = pp.pipeScan()
	}

	if next.Kind != colon {
		return nil, nil, fmt.Errorf("expected ':' after 'def %s' at position %d, got %s",
			nameTok.Text, next.Pos, pipeDescribeToken(next))
	}

	// Create the function definition early so it's available for recursion
	// in the body, and for use in the rest pipeline.
	fn := &pipeUserFunc{
		name:   nameTok.Text,
		params: params,
	}

	// Register parameters as pseudo-builtins during body parsing.
	// Parameters in jq are actually zero-arg functions that return their bound value.
	for _, p := range params {
		pp.userFuncs = append(pp.userFuncs, &pipeUserFunc{
			name:   p,
			params: nil, // zero-arg
		})
	}

	// Register the function itself for recursive calls.
	pp.userFuncs = append(pp.userFuncs, fn)

	// Parse body until semicolon.
	bodyExprs, err := pp.parsePipeline()
	if err != nil {
		return nil, nil, fmt.Errorf("def %s: body: %w", nameTok.Text, err)
	}
	fn.body = &Pipeline{exprs: bodyExprs, md: pp.md}

	// Remove parameter pseudo-builtins (they are only valid inside the body).
	// Keep the function itself registered for the rest pipeline.
	if len(params) > 0 {
		cleaned := make([]*pipeUserFunc, 0, len(pp.userFuncs))
		paramSet := make(map[string]bool, len(params))
		for _, p := range params {
			paramSet[p] = true
		}
		for _, f := range pp.userFuncs {
			if !paramSet[f.name] || len(f.params) > 0 {
				cleaned = append(cleaned, f)
			}
		}
		pp.userFuncs = cleaned
	}

	// Expect semicolon.
	semi := pp.pipeScan()
	if semi.Kind != semicolon {
		return nil, nil, fmt.Errorf("expected ';' after 'def %s' body at position %d, got %s",
			nameTok.Text, semi.Pos, pipeDescribeToken(semi))
	}

	// Parse the rest of the pipeline (fn is already in pp.userFuncs).
	restExprs, err := pp.parsePipeline()
	if err != nil {
		return nil, nil, fmt.Errorf("def %s: rest: %w", nameTok.Text, err)
	}

	return &pipeDefBind{
		def:  fn,
		body: &Pipeline{exprs: restExprs, md: pp.md},
	}, nil, nil
}

// parseFuncCall handles "name(" args ")" for all function types.
// The opening paren has already been consumed.
//
// Dispatch logic:
//   - name() → zero-arg built-in
//   - name(pipeline) → select, or 1-arg function from pipeFuncsWith1Arg
//   - name(pipeline; pipeline) → 2-arg function from pipeFuncsWith2Args
func (pp *pipeParser) parseFuncCall(nameTok *token) (PipeExpr, protoreflect.Descriptor, error) {
	// Check if zero-arg: name()
	next := pp.pipeScan()
	if next.Kind == cparen {
		// Zero-arg call: name()
		if builtin := lookupBuiltin(nameTok.Text); builtin != nil {
			return builtin, nil, nil
		}
		// User-defined zero-arg function.
		for _, fn := range pp.userFuncs {
			if fn.name == nameTok.Text && len(fn.params) == 0 {
				return &pipeUserFuncCall{name: fn.name, arity: 0, args: nil}, nil, nil
			}
		}
		if _, ok := pipeFuncsWith1Arg[nameTok.Text]; ok {
			return nil, nil, fmt.Errorf("function %q requires an argument at position %d", nameTok.Text, nameTok.Pos)
		}
		if _, ok := pipeFuncsWith2Args[nameTok.Text]; ok {
			return nil, nil, fmt.Errorf("function %q requires arguments at position %d", nameTok.Text, nameTok.Pos)
		}
		return nil, nil, fmt.Errorf("unknown function %q at position %d", nameTok.Text, nameTok.Pos)
	}
	pp.pipeUnscan(next)

	// Parse first argument as a pipeline.
	arg1, err := pp.parsePipeline()
	if err != nil {
		return nil, nil, fmt.Errorf("%s: %v", nameTok.Text, err)
	}

	// Check for semicolon → second argument.
	tok := pp.pipeScan()
	if tok.Kind == semicolon {
		arg2, err := pp.parsePipeline()
		if err != nil {
			return nil, nil, fmt.Errorf("%s: %v", nameTok.Text, err)
		}
		close := pp.pipeScan()
		if close.Kind != cparen {
			return nil, nil, fmt.Errorf("expected ')' after %s arguments at position %d, got %s",
				nameTok.Text, close.Pos, pipeDescribeToken(close))
		}
		if fn, ok := pipeFuncsWith2Args[nameTok.Text]; ok {
			p1 := &Pipeline{exprs: arg1, md: pp.md}
			p2 := &Pipeline{exprs: arg2, md: pp.md}
			return &pipeFuncWith2Pipelines{name: nameTok.Text, arg1: p1, arg2: p2, fn: fn}, pp.desc, nil
		}
		// User-defined 2-arg function.
		for _, fn := range pp.userFuncs {
			if fn.name == nameTok.Text && len(fn.params) == 2 {
				p1 := &Pipeline{exprs: arg1, md: pp.md}
				p2 := &Pipeline{exprs: arg2, md: pp.md}
				return &pipeUserFuncCall{name: fn.name, arity: 2, args: []*Pipeline{p1, p2}}, nil, nil
			}
		}
		return nil, nil, fmt.Errorf("function %q does not accept two arguments (at position %d)",
			nameTok.Text, nameTok.Pos)
	}

	// Single argument — expect closing paren.
	if tok.Kind != cparen {
		return nil, nil, fmt.Errorf("expected ')' or ';' after %s argument at position %d, got %s",
			nameTok.Text, tok.Pos, pipeDescribeToken(tok))
	}

	// select() — predicate filter.
	if nameTok.Text == "select" {
		var predExpr PipeExpr
		if len(arg1) == 1 {
			predExpr = arg1[0]
		} else {
			predExpr = &pipeGroupExpr{inner: &Pipeline{exprs: arg1, md: pp.md}}
		}
		return &pipeSelectExpr{pred: predExpr}, pp.desc, nil
	}

	// path(f) — output paths to matching values.
	if nameTok.Text == "path" {
		p := &Pipeline{exprs: arg1, md: pp.md}
		return &pipePath{filter: p}, nil, nil
	}

	// 1-arg pipeline function.
	if fn, ok := pipeFuncsWith1Arg[nameTok.Text]; ok {
		p := &Pipeline{exprs: arg1, md: pp.md}
		return &pipeFuncWithPipeline{name: nameTok.Text, inner: p, fn: fn}, pp.desc, nil
	}

	// User-defined function call (1-arg).
	for _, fn := range pp.userFuncs {
		if fn.name == nameTok.Text && len(fn.params) == 1 {
			p := &Pipeline{exprs: arg1, md: pp.md}
			return &pipeUserFuncCall{name: fn.name, arity: 1, args: []*Pipeline{p}}, nil, nil
		}
	}

	// Zero-arg function mistakenly called with args.
	if lookupBuiltin(nameTok.Text) != nil {
		return nil, nil, fmt.Errorf("function %q does not accept arguments (at position %d)",
			nameTok.Text, nameTok.Pos)
	}

	return nil, nil, fmt.Errorf("unknown function %q at position %d", nameTok.Text, nameTok.Pos)
}

// ── Literal parsers ─────────────────────────────────────────────────────

func (pp *pipeParser) parseLiteralInt(tok *token) (PipeExpr, protoreflect.Descriptor, error) {
	i, err := strconv.ParseInt(tok.Text, 0, 64)
	if err != nil {
		return nil, nil, fmt.Errorf("invalid integer %q at position %d", tok.Text, tok.Pos)
	}
	return &pipeLiteral{val: ScalarInt64(i)}, nil, nil
}

func (pp *pipeParser) parseLiteralFloat(tok *token) (PipeExpr, protoreflect.Descriptor, error) {
	f, err := strconv.ParseFloat(tok.Text, 64)
	if err != nil {
		return nil, nil, fmt.Errorf("invalid float %q at position %d", tok.Text, tok.Pos)
	}
	return &pipeLiteral{val: ScalarFloat64(f)}, nil, nil
}

func (pp *pipeParser) parseLiteralString(tok *token) (PipeExpr, protoreflect.Descriptor, error) {
	return &pipeLiteral{val: ScalarString(tok.Text)}, nil, nil
}

// parseStringInterp parses a string interpolation expression.
// The scanner has already returned strbegin; the parser alternates between
// expression parsing and strmid/strend tokens.
//
// Token stream example:
//
//	"hello \(.name) world"  →  strbegin("hello ") → .name → strend(" world")
//	"\(.a) and \(.b)"      →  strbegin("") → .a → strmid(" and ") → .b → strend("")
func (pp *pipeParser) parseStringInterp(beginTok *token) (PipeExpr, protoreflect.Descriptor, error) {
	var parts []PipeExpr

	// Add prefix literal from strbegin (may be empty string).
	parts = append(parts, &pipeLiteral{val: ScalarString(beginTok.Text)})

	for {
		// Parse the interpolated expression (a full pipeline).
		savedDesc := pp.desc
		inner, err := pp.parsePipeline()
		if err != nil {
			return nil, nil, fmt.Errorf("in string interpolation at position %d: %w", beginTok.Pos, err)
		}
		pp.desc = savedDesc
		parts = append(parts, &pipeGroupExpr{inner: &Pipeline{exprs: inner, md: pp.md}})

		// Next token must be strmid or strend.
		next := pp.pipeScan()
		switch next.Kind {
		case strmid:
			parts = append(parts, &pipeLiteral{val: ScalarString(next.Text)})
			// Continue loop to parse the next interpolated expression.
		case strend:
			parts = append(parts, &pipeLiteral{val: ScalarString(next.Text)})
			return &pipeStringInterp{parts: parts}, nil, nil
		default:
			return nil, nil, fmt.Errorf("expected string continuation or end at position %d, got %s",
				next.Pos, pipeDescribeToken(next))
		}
	}
}

// resolveField resolves a field name against the current descriptor.
func (pp *pipeParser) resolveField(desc protoreflect.Descriptor, name string, pos int) (protoreflect.FieldDescriptor, protoreflect.Descriptor, error) {
	var md protoreflect.MessageDescriptor
	switch d := desc.(type) {
	case protoreflect.MessageDescriptor:
		md = d
	case protoreflect.FieldDescriptor:
		if d.Message() == nil {
			return nil, nil, fmt.Errorf("cannot access field %q on scalar type at position %d", name, pos)
		}
		md = d.Message()
	default:
		if desc == nil {
			return nil, nil, fmt.Errorf("cannot access field %q: no schema context at position %d", name, pos)
		}
		return nil, nil, fmt.Errorf("cannot access field %q on %T at position %d", name, desc, pos)
	}
	fd := md.Fields().ByTextName(name)
	if fd == nil {
		return nil, nil, fmt.Errorf("field %q not found in %s at position %d", name, md.FullName(), pos)
	}
	// Advance cursor: for message-typed fields, cursor becomes the field descriptor
	// (so we know if it's a list/map). For scalar fields, cursor becomes nil (no further access).
	if fd.Message() != nil {
		return fd, fd, nil
	}
	return fd, nil, nil
}

// advanceDescForIterate returns the element descriptor after iterating.
// For a repeated message field → element message descriptor.
// For a map field → map value descriptor.
// Otherwise returns nil (elements are scalars).
func (pp *pipeParser) advanceDescForIterate(desc protoreflect.Descriptor) protoreflect.Descriptor {
	fd, ok := desc.(protoreflect.FieldDescriptor)
	if !ok {
		return nil
	}
	if fd.IsMap() {
		if fd.MapValue().Message() != nil {
			return fd.MapValue().Message()
		}
		return nil
	}
	if fd.IsList() && fd.Message() != nil {
		return fd.Message()
	}
	return nil
}

// ── pipeChain: sequential execution of left then right ──────────────────

// pipeChain applies left, then feeds each result through right.
// Used to implement postfix suffixes like ".field.sub[]".
type pipeChain struct {
	left, right PipeExpr
}

func (p *pipeChain) exec(ctx *PipeContext, input Value) ([]Value, error) {
	intermediate, err := p.left.exec(ctx, input)
	if err != nil {
		return nil, err
	}
	var out []Value
	for _, v := range intermediate {
		result, err := p.right.exec(ctx, v)
		if err != nil {
			return nil, err
		}
		out = append(out, result...)
	}
	return out, nil
}

// pipeGroupExpr wraps a sub-pipeline as a single PipeExpr.
type pipeGroupExpr struct {
	inner *Pipeline
}

func (p *pipeGroupExpr) exec(ctx *PipeContext, input Value) ([]Value, error) {
	return p.inner.execWith(ctx, []Value{input})
}

// ── Token description for errors ────────────────────────────────────────

func pipeDescribeToken(tok *token) string {
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
	case semicolon:
		return "';'"
	case at:
		return "'@'"
	case bang:
		return "'!'"
	case dollar:
		return "'$'"
	case plus:
		return "'+'"
	case minus:
		return "'-'"
	case slash:
		return "'/'"
	case percent:
		return "'%'"
	case slashslash:
		return "'//'"
	case questionquestion:
		return "'??'"
	case obrace:
		return "'{'"
	case cbrace:
		return "'}'"
	case strbegin:
		return "interpolated string start"
	case strmid:
		return "interpolated string segment"
	case strend:
		return "interpolated string end"
	case ampamp:
		return "'&&'"
	case pipepipe:
		return "'||'"
	case eof:
		return "end of input"
	case illegal:
		return fmt.Sprintf("illegal token %s", tok.Text)
	default:
		return fmt.Sprintf("token(%d)", tok.Kind)
	}
}
