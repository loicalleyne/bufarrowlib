package pbpath

import (
	"bytes"
	"regexp"
	"strconv"
	"unicode/utf8"
)

const maxCodepoint = 0x10FFFF // Unicode standard limit.
var (
	identRe   = regexp.MustCompile("^[a-zA-Z_][a-zA-Z_0-9]*")
	decimalRe = regexp.MustCompile("^-?(0|[1-9][0-9]*)")
	// 0 by itself is found by decimalRe
	octalRe = regexp.MustCompile("^-?(0[0-7]+)")
	hexRe   = regexp.MustCompile("^-?(0[xX][0-9a-fA-F]+)")
	// 1, 2, or 3 octal numerals for \o[o[o]] scanning
	oct13Re = regexp.MustCompile("^[0-7]{1,3}")
	// 1 or 2 hex numerals for \xHH scanning.
	hex12Re = regexp.MustCompile("^[0-9A-Fa-f]{1,2}")
	// 4 hex numerals for \uHHHH scanning.
	hex4Re = regexp.MustCompile("^[0-9A-Fa-f]{4}")
	// 8 hex numerals for \UHHHHHHHH scanning.
	hex8Re  = regexp.MustCompile("^[0-9A-Fa-f]{8}")
	escapes = map[byte]rune{
		'a':  '\a',
		'b':  '\b',
		'f':  '\f',
		'n':  '\n',
		'r':  '\r',
		't':  '\t',
		'v':  '\v',
		'\\': '\\',
		'\'': '\'',
		'"':  '"',
		'?':  '?',
	}
)

// tokenKind identifies the type of a lexical token produced by the scanner.
type tokenKind int

const (
	ident    tokenKind = iota // identifier: field names, root type names, "true"/"false"
	intlit                    // integer literal: decimal, octal, or hex
	strlit                    // string literal: single- or double-quoted
	dot                       // '.'
	oparen                    // '('
	cparen                    // ')'
	obrack                    // '['
	cbrack                    // ']'
	colon                     // ':'
	asterisk                  // '*'
	pipe                      // '|'
	question                  // '?'  (used after '[' to start a filter predicate)
	eqeq                     // '=='
	bangeq                   // '!='
	langle                   // '<'
	langleeq                 // '<='
	rangle                   // '>'
	rangleeq                 // '>='
	comma                    // ','
	bang                     // '!'  (logical not, when not followed by '=')
	ampamp                   // '&&'
	pipepipe                 // '||' (logical or; distinct from pipe '|')
	floatlit                 // floating-point literal (e.g. 3.14, -0.5)
	semicolon                // ';'  (argument separator in function calls)
	at                       // '@'  (format string prefix, e.g. @base64)
	dollar                   // '$'  (variable prefix)
	plus                     // '+'
	minus                    // '-'  (also unary negation)
	slash                    // '/'
	percent                  // '%'
	slashslash               // '//' (alternative operator)
	questionquestion         // '??' (optional operator / try)
	obrace                   // '{'
	cbrace                   // '}'
	strbegin                 // start of interpolated string: text before first \(
	strmid                   // middle segment of interpolated string: text between ) and \(
	strend                   // end of interpolated string: text between ) and closing "
	illegal                  // unrecognized or malformed token
	eof                      // end of input
)

type token struct {
	Kind tokenKind
	Pos  int    // start of the token position
	Text string // if the token carries content beyond its kind, this is it.
}
type escapedRune struct {
	Pos   int // start of the rune position
	Rune  rune
	Valid bool
}
type scanner struct {
	buf         []byte
	pos         int
	interpDepth int // >0 when inside string interpolation \(...)
}

// Scans a numeric escape sequence into a rune.
//
// The buf[s.pos] byte is assumed to be the first number character to interpret if
// s.pos < len(buf). Returns the composed rune.
//
// start: the input position of the escape sequence's leading '\'.
func (s *scanner) number(start int, re *regexp.Regexp, base int) escapedRune {
	numStart := s.pos
	loc := re.FindIndex(s.buf[s.pos:])
	if loc == nil {
		if s.pos < len(s.buf) { // consume a character to make progress.
			s.pos++
		}
		return escapedRune{Pos: start}
	}
	// The ^ anchor means loc[0] == 0.
	numLen := loc[1]
	number := string(s.buf[numStart : numStart+numLen])
	s.pos += numLen
	n, err := strconv.ParseInt(number, base, 32) // rune is a 32-bit int type.
	if err != nil {
		return escapedRune{Pos: start}
	}
	return escapedRune{Pos: start, Rune: rune(n), Valid: true}
}

// Scans an escape sequence into a rune.
// The buf[s.pos] byte is assumed to be `\`.
//
// rune_escape_seq    = simple_escape_seq | hex_escape_seq | octal_escape_seq | unicode_escape_seq .
// simple_escape_seq  = `\` ( "a" | "b" | "f" | "n" | "r" | "t" | "v" | `\` | "'" | `"` | "?" ) .
func (s *scanner) escape() escapedRune {
	pos := s.pos
	s.pos++ // Skip `\`
	if s.pos >= len(s.buf) {
		return escapedRune{Pos: s.pos}
	}
	peek := s.buf[s.pos]
	if simple, ok := escapes[peek]; ok {
		s.pos++
		return escapedRune{Pos: pos, Rune: simple, Valid: true}
	}
	// Octal doesn't have a leading escape character. Check the range.
	if peek >= '0' && peek <= '7' {
		return s.number(pos, oct13Re, 8)
	}
	switch peek {
	case 'u':
		s.pos++
		return s.number(pos, hex4Re, 16)
	case 'U':
		s.pos++
		return s.number(pos, hex8Re, 16)
	case 'x':
		s.pos++
		return s.number(pos, hex12Re, 16)
	case 'X':
		s.pos++
		return s.number(pos, hex12Re, 16)
	default:
		// Unknown character. Consume it.
		s.pos++
		return escapedRune{Pos: pos + 1}
	}
}

// scanstr will scan for all non-special printable characters and escape sequences until
// it finds an unescaped quote character. The buf[s.pos] byte is assumed to be `quote`.
//
// string_literal = single_quoted_string_literal | double_quoted_string_literal .
//
// single_quoted_string_literal = "'" { !("\n" | "\x00" | "'" | `\`) | rune_escape_seq } "'" .
// double_quoted_string_literal = `"` { !("\n" | "\x00" | `"` | `\`) | rune_escape_seq } `"` .
func (s *scanner) string() *token {
	lit := bytes.NewBuffer(nil)
	start := s.pos // If the string literal closes correctly, this is the position to return.
	quote := s.buf[s.pos]
	s.pos++ // Skip quote.
	for {
		// No string end delimiter, so error.
		if s.pos >= len(s.buf) {
			return &token{Pos: s.pos, Kind: illegal, Text: string(s.buf[start:])}
		}
		peek := s.buf[s.pos]
		// Newlines and null terminators are illegal.
		if peek == '\n' || peek == 0 {
			pos := s.pos
			s.pos++
			return s.bad(pos, rune(peek))
		}
		switch peek {
		case '\\':
			// Check for string interpolation: \( in double-quoted strings.
			if quote == '"' && s.pos+1 < len(s.buf) && s.buf[s.pos+1] == '(' {
				s.pos += 2 // skip \(
				s.interpDepth = 1
				return &token{Pos: start, Kind: strbegin, Text: lit.String()}
			}
			// Normal escape sequences.
			er := s.escape()
			if !er.Valid {
				end := er.Pos + 1
				if end > len(s.buf) {
					end = len(s.buf)
				}
				return &token{Pos: er.Pos, Kind: illegal, Text: string(s.buf[start:end])}
			}
			lit.WriteRune(er.Rune)
		case quote:
			s.pos++
			return &token{Pos: start, Kind: strlit, Text: lit.String()}
		default:
			r, size := utf8.DecodeRune(s.buf[s.pos:])
			// If the rune is actually bad and not a literal U+FFFD, then the size is 1.
			if r == utf8.RuneError && size == 1 {
				return s.single(illegal)
			}
			s.pos += size
			lit.WriteRune(r)
		}
	}
}

func (s *scanner) single(t tokenKind) *token {
	start := s.pos
	s.pos++
	return &token{Pos: start, Kind: t}
}

// double consumes a two-character token (e.g. "==", "!=", "&&", "||", "<=", ">=").
func (s *scanner) double(t tokenKind) *token {
	start := s.pos
	s.pos += 2
	return &token{Pos: start, Kind: t}
}

// scanFloat scans a floating-point literal starting at the given position.
// The scanner position must be at the first character of the number (which
// may be '-', a digit, or '.' for literals like ".5").
// Recognized forms: 3.14, -0.5, .5, 100.0
func (s *scanner) scanFloat(startPos int) *token {
	pos := startPos
	// Consume optional leading '-'.
	if pos < len(s.buf) && s.buf[pos] == '-' {
		pos++
	}
	// Consume digits before the decimal point.
	for pos < len(s.buf) && s.buf[pos] >= '0' && s.buf[pos] <= '9' {
		pos++
	}
	// There must be a '.' for this to be a float.
	if pos >= len(s.buf) || s.buf[pos] != '.' {
		// Not a float — shouldn't happen if caller checked.
		s.pos++
		return &token{Pos: startPos, Kind: illegal}
	}
	pos++ // consume '.'
	// Consume digits after the decimal point.
	for pos < len(s.buf) && s.buf[pos] >= '0' && s.buf[pos] <= '9' {
		pos++
	}
	text := string(s.buf[startPos:pos])
	s.pos = pos
	return &token{Pos: startPos, Kind: floatlit, Text: text}
}

// scanStringTail resumes scanning a double-quoted string after a )
// closes an interpolated expression. Returns strmid if another \( is
// found, or strend if the closing " is reached.
func (s *scanner) scanStringTail() *token {
	lit := bytes.NewBuffer(nil)
	start := s.pos
	for {
		if s.pos >= len(s.buf) {
			return &token{Pos: s.pos, Kind: illegal, Text: string(s.buf[start:])}
		}
		peek := s.buf[s.pos]
		if peek == '\n' || peek == 0 {
			pos := s.pos
			s.pos++
			return s.bad(pos, rune(peek))
		}
		switch peek {
		case '\\':
			// Check for another interpolation: \(
			if s.pos+1 < len(s.buf) && s.buf[s.pos+1] == '(' {
				s.pos += 2 // skip \(
				s.interpDepth = 1
				return &token{Pos: start, Kind: strmid, Text: lit.String()}
			}
			// Normal escape.
			er := s.escape()
			if !er.Valid {
				end := er.Pos + 1
				if end > len(s.buf) {
					end = len(s.buf)
				}
				return &token{Pos: er.Pos, Kind: illegal, Text: string(s.buf[start:end])}
			}
			lit.WriteRune(er.Rune)
		case '"':
			s.pos++
			return &token{Pos: start, Kind: strend, Text: lit.String()}
		default:
			r, size := utf8.DecodeRune(s.buf[s.pos:])
			if r == utf8.RuneError && size == 1 {
				return s.single(illegal)
			}
			s.pos += size
			lit.WriteRune(r)
		}
	}
}

func (s *scanner) bad(pos int, r rune) *token {
	return &token{Pos: pos, Kind: illegal, Text: strconv.QuoteRune(r)}
}

func (s *scanner) scan() *token {
	// Skip whitespace — predicate expressions may contain spaces.
	for s.pos < len(s.buf) && (s.buf[s.pos] == ' ' || s.buf[s.pos] == '\t') {
		s.pos++
	}
	if s.pos >= len(s.buf) {
		return &token{Pos: s.pos, Kind: eof}
	}
	literal := func(t tokenKind, loc []int) *token {
		pos := s.pos
		// loc[0] is always 0 since the regular expressions have the ^ anchor.
		litLen := loc[1]
		s.pos += litLen
		return &token{Pos: pos, Kind: t, Text: string(s.buf[pos : pos+litLen])}
	}
	rest := s.buf[s.pos:]
	if loc := octalRe.FindIndex(rest); loc != nil {
		return literal(intlit, loc)
	}
	if loc := hexRe.FindIndex(rest); loc != nil {
		return literal(intlit, loc)
	}
	if loc := decimalRe.FindIndex(rest); loc != nil {
		// Check if this is actually a float: digits followed by '.' and another digit.
		afterInt := s.pos + loc[1]
		if afterInt < len(s.buf) && s.buf[afterInt] == '.' &&
			afterInt+1 < len(s.buf) && s.buf[afterInt+1] >= '0' && s.buf[afterInt+1] <= '9' {
			return s.scanFloat(s.pos)
		}
		return literal(intlit, loc)
	}
	if loc := identRe.FindIndex(rest); loc != nil {
		return literal(ident, loc)
	}
	switch rest[0] {
	case '(':
		if s.interpDepth > 0 {
			s.interpDepth++
		}
		return s.single(oparen)
	case ')':
		if s.interpDepth > 0 {
			s.interpDepth--
			if s.interpDepth == 0 {
				// Closing the interpolation — resume scanning the string tail.
				s.pos++ // skip ')'
				return s.scanStringTail()
			}
		}
		return s.single(cparen)
	case '[':
		return s.single(obrack)
	case ']':
		return s.single(cbrack)
	case '{':
		return s.single(obrace)
	case '}':
		return s.single(cbrace)
	case ':':
		return s.single(colon)
	case '*':
		return s.single(asterisk)
	case '.':
		return s.single(dot)
	case ',':
		return s.single(comma)
	case ';':
		return s.single(semicolon)
	case '@':
		return s.single(at)
	case '$':
		return s.single(dollar)
	case '+':
		return s.single(plus)
	case '/':
		// '//' = alternative operator; bare '/' = divide
		if s.pos+1 < len(s.buf) && s.buf[s.pos+1] == '/' {
			return s.double(slashslash)
		}
		return s.single(slash)
	case '%':
		return s.single(percent)
	case '-':
		return s.single(minus)
	case '?':
		if s.pos+1 < len(s.buf) && s.buf[s.pos+1] == '?' {
			return s.double(questionquestion)
		}
		return s.single(question)
	case '|':
		// '||' = logical or; bare '|' = pipe
		if s.pos+1 < len(s.buf) && s.buf[s.pos+1] == '|' {
			return s.double(pipepipe)
		}
		return s.single(pipe)
	case '&':
		if s.pos+1 < len(s.buf) && s.buf[s.pos+1] == '&' {
			return s.double(ampamp)
		}
		return s.single(illegal) // bare '&' is not valid
	case '!':
		if s.pos+1 < len(s.buf) && s.buf[s.pos+1] == '=' {
			return s.double(bangeq)
		}
		return s.single(bang)
	case '=':
		if s.pos+1 < len(s.buf) && s.buf[s.pos+1] == '=' {
			return s.double(eqeq)
		}
		return s.single(illegal) // bare '=' is not a valid token
	case '<':
		if s.pos+1 < len(s.buf) && s.buf[s.pos+1] == '=' {
			return s.double(langleeq)
		}
		return s.single(langle)
	case '>':
		if s.pos+1 < len(s.buf) && s.buf[s.pos+1] == '=' {
			return s.double(rangleeq)
		}
		return s.single(rangle)
	case '\'':
		return s.string()
	case '"':
		return s.string()
	default:
		// This might be a single character or it might be a multi-byte codepoint that would be easier
		// to present in an error as a whole.
		r, size := utf8.DecodeRune(rest)
		// If the rune is actually bad and not a literal U+FFFD, then the size is 1. This is fine to
		// still quote as utf8.RuneError in the token text.
		pos := s.pos
		s.pos += size
		return s.bad(pos, r)
	}
}
