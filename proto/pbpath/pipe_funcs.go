package pbpath

import (
	"encoding/base64"
	"encoding/json"
	"fmt"
	"html"
	"math"
	"net/url"
	"regexp"
	"sort"
	"strings"
	"unicode/utf8"
)

// ── Comma operator ──────────────────────────────────────────────────────

// pipeComma produces the concatenation of left's and right's outputs.
// This implements the jq comma operator: `.a, .b` → two outputs.
type pipeComma struct {
	left, right PipeExpr
}

func (p *pipeComma) exec(ctx *PipeContext, input Value) ([]Value, error) {
	lOut, err := p.left.exec(ctx, input)
	if err != nil {
		return nil, err
	}
	rOut, err := p.right.exec(ctx, input)
	if err != nil {
		return nil, err
	}
	out := make([]Value, 0, len(lOut)+len(rOut))
	out = append(out, lOut...)
	out = append(out, rOut...)
	return out, nil
}

// ── Pipeline-argument function expressions ──────────────────────────────

// pipeFuncWithPipeline wraps a function that takes one sub-pipeline argument.
// Used by map(f), sort_by(f), group_by(f), unique_by(f), min_by(f), max_by(f),
// select(f), first(f), last(f), limit(n; f), etc.
type pipeFuncWithPipeline struct {
	name  string
	inner *Pipeline
	fn    func(ctx *PipeContext, input Value, inner *Pipeline) ([]Value, error)
}

func (p *pipeFuncWithPipeline) exec(ctx *PipeContext, input Value) ([]Value, error) {
	return p.fn(ctx, input, p.inner)
}

// pipeFuncWith2Pipelines wraps a function with two sub-pipeline arguments
// separated by semicolons (e.g. gsub(re; replacement), sub(re; replacement),
// limit(n; f)).
type pipeFuncWith2Pipelines struct {
	name        string
	arg1, arg2  *Pipeline
	fn          func(ctx *PipeContext, input Value, arg1, arg2 *Pipeline) ([]Value, error)
}

func (p *pipeFuncWith2Pipelines) exec(ctx *PipeContext, input Value) ([]Value, error) {
	return p.fn(ctx, input, p.arg1, p.arg2)
}

// ── String functions ────────────────────────────────────────────────────

func builtinAsciiDowncase(_ *PipeContext, input Value) ([]Value, error) {
	s := extractString(input)
	return []Value{ScalarString(strings.ToLower(s))}, nil
}

func builtinAsciiUpcase(_ *PipeContext, input Value) ([]Value, error) {
	s := extractString(input)
	return []Value{ScalarString(strings.ToUpper(s))}, nil
}

// builtinLtrimstr is a pipeline-arg function: ltrimstr(prefix).
func execLtrimstr(_ *PipeContext, input Value, arg *Pipeline) ([]Value, error) {
	s := extractString(input)
	prefixVals, err := arg.ExecOne(input)
	if err != nil {
		return nil, err
	}
	if len(prefixVals) > 0 {
		prefix := extractString(prefixVals[0])
		return []Value{ScalarString(strings.TrimPrefix(s, prefix))}, nil
	}
	return []Value{ScalarString(s)}, nil
}

// builtinRtrimstr is a pipeline-arg function: rtrimstr(suffix).
func execRtrimstr(_ *PipeContext, input Value, arg *Pipeline) ([]Value, error) {
	s := extractString(input)
	suffixVals, err := arg.ExecOne(input)
	if err != nil {
		return nil, err
	}
	if len(suffixVals) > 0 {
		suffix := extractString(suffixVals[0])
		return []Value{ScalarString(strings.TrimSuffix(s, suffix))}, nil
	}
	return []Value{ScalarString(s)}, nil
}

func builtinStartswith(_ *PipeContext, input Value, arg *Pipeline) ([]Value, error) {
	s := extractString(input)
	prefixVals, err := arg.ExecOne(input)
	if err != nil {
		return nil, err
	}
	if len(prefixVals) > 0 {
		return []Value{ScalarBool(strings.HasPrefix(s, extractString(prefixVals[0])))}, nil
	}
	return []Value{ScalarBool(false)}, nil
}

func builtinEndswith(_ *PipeContext, input Value, arg *Pipeline) ([]Value, error) {
	s := extractString(input)
	suffixVals, err := arg.ExecOne(input)
	if err != nil {
		return nil, err
	}
	if len(suffixVals) > 0 {
		return []Value{ScalarBool(strings.HasSuffix(s, extractString(suffixVals[0])))}, nil
	}
	return []Value{ScalarBool(false)}, nil
}

func builtinSplit(_ *PipeContext, input Value, arg *Pipeline) ([]Value, error) {
	s := extractString(input)
	sepVals, err := arg.ExecOne(input)
	if err != nil {
		return nil, err
	}
	sep := ""
	if len(sepVals) > 0 {
		sep = extractString(sepVals[0])
	}
	parts := strings.Split(s, sep)
	out := make([]Value, len(parts))
	for i, p := range parts {
		out[i] = ScalarString(p)
	}
	return []Value{ListVal(out)}, nil
}

func builtinJoin(_ *PipeContext, input Value, arg *Pipeline) ([]Value, error) {
	if input.Kind() != ListKind {
		return nil, fmt.Errorf("join: input must be an array, got %s", typeNameOf(input))
	}
	sepVals, err := arg.ExecOne(input)
	if err != nil {
		return nil, err
	}
	sep := ""
	if len(sepVals) > 0 {
		sep = extractString(sepVals[0])
	}
	items := input.List()
	parts := make([]string, len(items))
	for i, item := range items {
		parts[i] = extractString(item)
	}
	return []Value{ScalarString(strings.Join(parts, sep))}, nil
}

// builtinTest checks if input matches a regex pattern: test(pattern).
func builtinTest(_ *PipeContext, input Value, arg *Pipeline) ([]Value, error) {
	s := extractString(input)
	patVals, err := arg.ExecOne(input)
	if err != nil {
		return nil, err
	}
	if len(patVals) == 0 {
		return []Value{ScalarBool(false)}, nil
	}
	pattern := extractString(patVals[0])
	re, err := regexp.Compile(pattern)
	if err != nil {
		return nil, fmt.Errorf("test: invalid regex %q: %v", pattern, err)
	}
	return []Value{ScalarBool(re.MatchString(s))}, nil
}

// builtinMatch finds regex match details: match(pattern) → {offset, length, string, captures}.
// Simplified: returns a message-like Value as a list with match info.
func builtinMatch(_ *PipeContext, input Value, arg *Pipeline) ([]Value, error) {
	s := extractString(input)
	patVals, err := arg.ExecOne(input)
	if err != nil {
		return nil, err
	}
	if len(patVals) == 0 {
		return []Value{Null()}, nil
	}
	pattern := extractString(patVals[0])
	re, err := regexp.Compile(pattern)
	if err != nil {
		return nil, fmt.Errorf("match: invalid regex %q: %v", pattern, err)
	}
	loc := re.FindStringIndex(s)
	if loc == nil {
		return []Value{Null()}, nil
	}
	// Return a list representing {offset, length, string, captures}
	// as jq does. We use a list since we don't have object construction yet.
	matched := s[loc[0]:loc[1]]
	return []Value{ListVal([]Value{
		ScalarInt64(int64(loc[0])),               // offset
		ScalarInt64(int64(loc[1] - loc[0])),      // length
		ScalarString(matched),                     // string
	})}, nil
}

// builtinCapture finds named capture groups: capture(pattern) → object-like list.
func builtinCapture(_ *PipeContext, input Value, arg *Pipeline) ([]Value, error) {
	s := extractString(input)
	patVals, err := arg.ExecOne(input)
	if err != nil {
		return nil, err
	}
	if len(patVals) == 0 {
		return []Value{Null()}, nil
	}
	pattern := extractString(patVals[0])
	re, err := regexp.Compile(pattern)
	if err != nil {
		return nil, fmt.Errorf("capture: invalid regex %q: %v", pattern, err)
	}
	names := re.SubexpNames()
	match := re.FindStringSubmatch(s)
	if match == nil {
		return []Value{Null()}, nil
	}
	// Build key-value pairs for named groups.
	var pairs []Value
	for i, name := range names {
		if i == 0 || name == "" {
			continue
		}
		val := ""
		if i < len(match) {
			val = match[i]
		}
		pairs = append(pairs, ScalarString(name), ScalarString(val))
	}
	return []Value{ListVal(pairs)}, nil
}

// execGsub replaces all matches: gsub(pattern; replacement).
func execGsub(_ *PipeContext, input Value, arg1, arg2 *Pipeline) ([]Value, error) {
	s := extractString(input)
	patVals, err := arg1.ExecOne(input)
	if err != nil {
		return nil, err
	}
	replVals, err := arg2.ExecOne(input)
	if err != nil {
		return nil, err
	}
	if len(patVals) == 0 || len(replVals) == 0 {
		return []Value{ScalarString(s)}, nil
	}
	pattern := extractString(patVals[0])
	replacement := extractString(replVals[0])
	re, err := regexp.Compile(pattern)
	if err != nil {
		return nil, fmt.Errorf("gsub: invalid regex %q: %v", pattern, err)
	}
	return []Value{ScalarString(re.ReplaceAllString(s, replacement))}, nil
}

// execSub replaces first match: sub(pattern; replacement).
func execSub(_ *PipeContext, input Value, arg1, arg2 *Pipeline) ([]Value, error) {
	s := extractString(input)
	patVals, err := arg1.ExecOne(input)
	if err != nil {
		return nil, err
	}
	replVals, err := arg2.ExecOne(input)
	if err != nil {
		return nil, err
	}
	if len(patVals) == 0 || len(replVals) == 0 {
		return []Value{ScalarString(s)}, nil
	}
	pattern := extractString(patVals[0])
	replacement := extractString(replVals[0])
	re, err := regexp.Compile(pattern)
	if err != nil {
		return nil, fmt.Errorf("sub: invalid regex %q: %v", pattern, err)
	}
	// Replace only the first match.
	loc := re.FindStringIndex(s)
	if loc == nil {
		return []Value{ScalarString(s)}, nil
	}
	result := s[:loc[0]] + re.ReplaceAllString(s[loc[0]:loc[1]], replacement) + s[loc[1]:]
	return []Value{ScalarString(result)}, nil
}

// builtinExplode converts a string to an array of Unicode code points.
func builtinExplode(_ *PipeContext, input Value) ([]Value, error) {
	s := extractString(input)
	runes := []rune(s)
	out := make([]Value, len(runes))
	for i, r := range runes {
		out[i] = ScalarInt64(int64(r))
	}
	return []Value{ListVal(out)}, nil
}

// builtinImplode converts an array of code points back to a string.
func builtinImplode(_ *PipeContext, input Value) ([]Value, error) {
	if input.Kind() != ListKind {
		return nil, fmt.Errorf("implode: input must be an array, got %s", typeNameOf(input))
	}
	items := input.List()
	runes := make([]rune, len(items))
	for i, item := range items {
		n, ok := toInt64Value(item)
		if !ok {
			return nil, fmt.Errorf("implode: element %d is not an integer", i)
		}
		if n < 0 || n > int64(utf8.MaxRune) {
			return nil, fmt.Errorf("implode: element %d (%d) is not a valid Unicode code point", i, n)
		}
		runes[i] = rune(n)
	}
	return []Value{ScalarString(string(runes))}, nil
}

// ── Collection functions ────────────────────────────────────────────────

// execMap applies a pipeline to each element: map(f).
func execMap(ctx *PipeContext, input Value, inner *Pipeline) ([]Value, error) {
	items, err := toList(input)
	if err != nil {
		return nil, fmt.Errorf("map: %v", err)
	}
	var out []Value
	for _, item := range items {
		result, err := inner.ExecOne(item)
		if err != nil {
			return nil, err
		}
		out = append(out, result...)
	}
	return []Value{ListVal(out)}, nil
}

// execSortBy sorts array elements by a key pipeline: sort_by(f).
func execSortBy(ctx *PipeContext, input Value, inner *Pipeline) ([]Value, error) {
	items, err := toList(input)
	if err != nil {
		return nil, fmt.Errorf("sort_by: %v", err)
	}
	if len(items) == 0 {
		return []Value{ListVal(nil)}, nil
	}
	// Evaluate key for each element.
	type keyed struct {
		val Value
		key Value
	}
	pairs := make([]keyed, len(items))
	for i, item := range items {
		kv, err := inner.ExecOne(item)
		if err != nil {
			return nil, err
		}
		k := Null()
		if len(kv) > 0 {
			k = kv[0]
		}
		pairs[i] = keyed{val: item, key: k}
	}
	sort.SliceStable(pairs, func(i, j int) bool {
		cmp, ok := compareValuesV(pairs[i].key, pairs[j].key)
		if !ok {
			return false
		}
		return cmp < 0
	})
	out := make([]Value, len(pairs))
	for i, p := range pairs {
		out[i] = p.val
	}
	return []Value{ListVal(out)}, nil
}

// execGroupBy groups array elements by a key pipeline: group_by(f).
func execGroupBy(ctx *PipeContext, input Value, inner *Pipeline) ([]Value, error) {
	items, err := toList(input)
	if err != nil {
		return nil, fmt.Errorf("group_by: %v", err)
	}
	if len(items) == 0 {
		return []Value{ListVal(nil)}, nil
	}
	type keyed struct {
		val Value
		key string // stringified key for grouping
	}
	pairs := make([]keyed, len(items))
	for i, item := range items {
		kv, err := inner.ExecOne(item)
		if err != nil {
			return nil, err
		}
		k := ""
		if len(kv) > 0 {
			k = kv[0].String()
		}
		pairs[i] = keyed{val: item, key: k}
	}
	// Stable sort by key, then group consecutive equal keys.
	sort.SliceStable(pairs, func(i, j int) bool {
		return pairs[i].key < pairs[j].key
	})
	var groups []Value
	var curGroup []Value
	curKey := ""
	for i, p := range pairs {
		if i == 0 || p.key != curKey {
			if len(curGroup) > 0 {
				groups = append(groups, ListVal(curGroup))
			}
			curGroup = nil
			curKey = p.key
		}
		curGroup = append(curGroup, p.val)
	}
	if len(curGroup) > 0 {
		groups = append(groups, ListVal(curGroup))
	}
	return []Value{ListVal(groups)}, nil
}

// execUniqueBy deduplicates by key: unique_by(f).
func execUniqueBy(ctx *PipeContext, input Value, inner *Pipeline) ([]Value, error) {
	items, err := toList(input)
	if err != nil {
		return nil, fmt.Errorf("unique_by: %v", err)
	}
	seen := make(map[string]bool)
	var out []Value
	for _, item := range items {
		kv, err := inner.ExecOne(item)
		if err != nil {
			return nil, err
		}
		k := "null"
		if len(kv) > 0 {
			k = kv[0].String()
		}
		if !seen[k] {
			seen[k] = true
			out = append(out, item)
		}
	}
	return []Value{ListVal(out)}, nil
}

// execMinBy returns the element with minimum key: min_by(f).
func execMinBy(ctx *PipeContext, input Value, inner *Pipeline) ([]Value, error) {
	return execMinMaxBy(ctx, input, inner, true)
}

// execMaxBy returns the element with maximum key: max_by(f).
func execMaxBy(ctx *PipeContext, input Value, inner *Pipeline) ([]Value, error) {
	return execMinMaxBy(ctx, input, inner, false)
}

func execMinMaxBy(_ *PipeContext, input Value, inner *Pipeline, isMin bool) ([]Value, error) {
	items, err := toList(input)
	if err != nil {
		name := "min_by"
		if !isMin {
			name = "max_by"
		}
		return nil, fmt.Errorf("%s: %v", name, err)
	}
	if len(items) == 0 {
		return []Value{Null()}, nil
	}
	bestVal := items[0]
	bestKey := Null()
	kv, err := inner.ExecOne(items[0])
	if err != nil {
		return nil, err
	}
	if len(kv) > 0 {
		bestKey = kv[0]
	}
	for i := 1; i < len(items); i++ {
		kv, err := inner.ExecOne(items[i])
		if err != nil {
			return nil, err
		}
		k := Null()
		if len(kv) > 0 {
			k = kv[0]
		}
		cmp, ok := compareValuesV(k, bestKey)
		if ok && ((isMin && cmp < 0) || (!isMin && cmp > 0)) {
			bestVal = items[i]
			bestKey = k
		}
	}
	return []Value{bestVal}, nil
}

// execLimit takes at most n elements from a pipeline: limit(n; f).
func execLimit(_ *PipeContext, input Value, arg1, arg2 *Pipeline) ([]Value, error) {
	nVals, err := arg1.ExecOne(input)
	if err != nil {
		return nil, err
	}
	n := int64(0)
	if len(nVals) > 0 {
		n, _ = toInt64Value(nVals[0])
	}
	results, err := arg2.ExecOne(input)
	if err != nil {
		return nil, err
	}
	if int64(len(results)) > n {
		results = results[:n]
	}
	return results, nil
}

func builtinFlatten(_ *PipeContext, input Value) ([]Value, error) {
	if input.Kind() != ListKind {
		return nil, fmt.Errorf("flatten: input must be an array, got %s", typeNameOf(input))
	}
	return []Value{ListVal(flattenList(input.List(), -1))}, nil
}

func flattenList(items []Value, depth int) []Value {
	var out []Value
	for _, item := range items {
		if item.Kind() == ListKind && depth != 0 {
			out = append(out, flattenList(item.List(), depth-1)...)
		} else {
			out = append(out, item)
		}
	}
	return out
}

func builtinReverse(_ *PipeContext, input Value) ([]Value, error) {
	switch input.Kind() {
	case ListKind:
		items := input.List()
		out := make([]Value, len(items))
		for i, item := range items {
			out[len(items)-1-i] = item
		}
		return []Value{ListVal(out)}, nil
	case ScalarKind:
		if s, ok := scalarInterface(input).(string); ok {
			runes := []rune(s)
			for i, j := 0, len(runes)-1; i < j; i, j = i+1, j-1 {
				runes[i], runes[j] = runes[j], runes[i]
			}
			return []Value{ScalarString(string(runes))}, nil
		}
		return nil, fmt.Errorf("reverse: cannot reverse %s", typeNameOf(input))
	default:
		return nil, fmt.Errorf("reverse: cannot reverse %s", typeNameOf(input))
	}
}

func builtinFirst(_ *PipeContext, input Value) ([]Value, error) {
	if input.Kind() == ListKind {
		items := input.List()
		if len(items) > 0 {
			return []Value{items[0]}, nil
		}
		return []Value{Null()}, nil
	}
	return []Value{input}, nil
}

func builtinLast(_ *PipeContext, input Value) ([]Value, error) {
	if input.Kind() == ListKind {
		items := input.List()
		if len(items) > 0 {
			return []Value{items[len(items)-1]}, nil
		}
		return []Value{Null()}, nil
	}
	return []Value{input}, nil
}

func builtinNth(_ *PipeContext, input Value, arg *Pipeline) ([]Value, error) {
	nVals, err := arg.ExecOne(input)
	if err != nil {
		return nil, err
	}
	if len(nVals) == 0 {
		return []Value{Null()}, nil
	}
	n, ok := toInt64Value(nVals[0])
	if !ok {
		return nil, fmt.Errorf("nth: argument must be a number")
	}
	if input.Kind() == ListKind {
		return []Value{input.Index(int(n))}, nil
	}
	return []Value{Null()}, nil
}

func builtinIndices(_ *PipeContext, input Value, arg *Pipeline) ([]Value, error) {
	sVals, err := arg.ExecOne(input)
	if err != nil {
		return nil, err
	}
	if len(sVals) == 0 {
		return []Value{ListVal(nil)}, nil
	}
	if input.Kind() == ScalarKind {
		s := extractString(input)
		sub := extractString(sVals[0])
		var indices []Value
		start := 0
		for {
			idx := strings.Index(s[start:], sub)
			if idx < 0 {
				break
			}
			indices = append(indices, ScalarInt64(int64(start+idx)))
			start += idx + 1
		}
		return []Value{ListVal(indices)}, nil
	}
	if input.Kind() == ListKind {
		items := input.List()
		target := sVals[0]
		var indices []Value
		for i, item := range items {
			cmp, ok := compareValuesV(item, target)
			if ok && cmp == 0 {
				indices = append(indices, ScalarInt64(int64(i)))
			}
		}
		return []Value{ListVal(indices)}, nil
	}
	return []Value{ListVal(nil)}, nil
}

func builtinIndex(_ *PipeContext, input Value, arg *Pipeline) ([]Value, error) {
	sVals, err := arg.ExecOne(input)
	if err != nil {
		return nil, err
	}
	if len(sVals) == 0 {
		return []Value{Null()}, nil
	}
	if input.Kind() == ScalarKind {
		s := extractString(input)
		sub := extractString(sVals[0])
		idx := strings.Index(s, sub)
		if idx < 0 {
			return []Value{Null()}, nil
		}
		return []Value{ScalarInt64(int64(idx))}, nil
	}
	if input.Kind() == ListKind {
		items := input.List()
		target := sVals[0]
		for i, item := range items {
			cmp, ok := compareValuesV(item, target)
			if ok && cmp == 0 {
				return []Value{ScalarInt64(int64(i))}, nil
			}
		}
		return []Value{Null()}, nil
	}
	return []Value{Null()}, nil
}

func builtinRindex(_ *PipeContext, input Value, arg *Pipeline) ([]Value, error) {
	sVals, err := arg.ExecOne(input)
	if err != nil {
		return nil, err
	}
	if len(sVals) == 0 {
		return []Value{Null()}, nil
	}
	if input.Kind() == ScalarKind {
		s := extractString(input)
		sub := extractString(sVals[0])
		idx := strings.LastIndex(s, sub)
		if idx < 0 {
			return []Value{Null()}, nil
		}
		return []Value{ScalarInt64(int64(idx))}, nil
	}
	if input.Kind() == ListKind {
		items := input.List()
		target := sVals[0]
		for i := len(items) - 1; i >= 0; i-- {
			cmp, ok := compareValuesV(items[i], target)
			if ok && cmp == 0 {
				return []Value{ScalarInt64(int64(i))}, nil
			}
		}
		return []Value{Null()}, nil
	}
	return []Value{Null()}, nil
}

func builtinContains(_ *PipeContext, input Value, arg *Pipeline) ([]Value, error) {
	tgtVals, err := arg.ExecOne(input)
	if err != nil {
		return nil, err
	}
	if len(tgtVals) == 0 {
		return []Value{ScalarBool(false)}, nil
	}
	return []Value{ScalarBool(valueContains(input, tgtVals[0]))}, nil
}

func builtinInside(_ *PipeContext, input Value, arg *Pipeline) ([]Value, error) {
	tgtVals, err := arg.ExecOne(input)
	if err != nil {
		return nil, err
	}
	if len(tgtVals) == 0 {
		return []Value{ScalarBool(false)}, nil
	}
	return []Value{ScalarBool(valueContains(tgtVals[0], input))}, nil
}

// valueContains checks if a contains b (jq semantics).
func valueContains(a, b Value) bool {
	if b.IsNull() {
		return true
	}
	if a.Kind() == ScalarKind && b.Kind() == ScalarKind {
		// String containment.
		as, aOk := scalarInterface(a).(string)
		bs, bOk := scalarInterface(b).(string)
		if aOk && bOk {
			return strings.Contains(as, bs)
		}
		// Exact equality for other types.
		cmp, ok := compareValuesV(a, b)
		return ok && cmp == 0
	}
	if a.Kind() == ListKind && b.Kind() == ListKind {
		// Every element in b must be contained in some element of a.
		for _, bItem := range b.List() {
			found := false
			for _, aItem := range a.List() {
				if valueContains(aItem, bItem) {
					found = true
					break
				}
			}
			if !found {
				return false
			}
		}
		return true
	}
	cmp, ok := compareValuesV(a, b)
	return ok && cmp == 0
}

// ── Numeric functions ───────────────────────────────────────────────────

func builtinFabs(_ *PipeContext, input Value) ([]Value, error) {
	return numericUnary(input, "fabs", math.Abs)
}

func builtinSqrt(_ *PipeContext, input Value) ([]Value, error) {
	return numericUnary(input, "sqrt", math.Sqrt)
}

func builtinLog(_ *PipeContext, input Value) ([]Value, error) {
	return numericUnary(input, "log", math.Log)
}

func builtinPow(_ *PipeContext, input Value) ([]Value, error) {
	// pow takes a list [base, exponent]
	if input.Kind() != ListKind || input.Len() != 2 {
		return nil, fmt.Errorf("pow: input must be [base, exponent]")
	}
	items := input.List()
	base, ok1 := toFloat64FromValue(items[0])
	exp, ok2 := toFloat64FromValue(items[1])
	if !ok1 || !ok2 {
		return nil, fmt.Errorf("pow: arguments must be numeric")
	}
	return []Value{ScalarFloat64(math.Pow(base, exp))}, nil
}

func builtinNan(_ *PipeContext, _ Value) ([]Value, error) {
	return []Value{ScalarFloat64(math.NaN())}, nil
}

func builtinInfinite(_ *PipeContext, _ Value) ([]Value, error) {
	return []Value{ScalarFloat64(math.Inf(1))}, nil
}

func builtinIsnan(_ *PipeContext, input Value) ([]Value, error) {
	f, ok := toFloat64FromValue(input)
	if !ok {
		return []Value{ScalarBool(false)}, nil
	}
	return []Value{ScalarBool(math.IsNaN(f))}, nil
}

func builtinIsinfinite(_ *PipeContext, input Value) ([]Value, error) {
	f, ok := toFloat64FromValue(input)
	if !ok {
		return []Value{ScalarBool(false)}, nil
	}
	return []Value{ScalarBool(math.IsInf(f, 0))}, nil
}

func builtinIsnormal(_ *PipeContext, input Value) ([]Value, error) {
	f, ok := toFloat64FromValue(input)
	if !ok {
		return []Value{ScalarBool(false)}, nil
	}
	return []Value{ScalarBool(!math.IsNaN(f) && !math.IsInf(f, 0) && f != 0)}, nil
}

// ── Serialization functions ─────────────────────────────────────────────

func builtinTojson(_ *PipeContext, input Value) ([]Value, error) {
	v := valueToJSON(input)
	b, err := json.Marshal(v)
	if err != nil {
		return nil, fmt.Errorf("tojson: %v", err)
	}
	return []Value{ScalarString(string(b))}, nil
}

func builtinFromjson(_ *PipeContext, input Value) ([]Value, error) {
	s := extractString(input)
	var raw any
	if err := json.Unmarshal([]byte(s), &raw); err != nil {
		return nil, fmt.Errorf("fromjson: %v", err)
	}
	return []Value{jsonToValue(raw)}, nil
}

// Format string builtins: @base64, @base64d, @uri, @csv, @tsv, @html, @json.
func builtinBase64(_ *PipeContext, input Value) ([]Value, error) {
	s := extractString(input)
	return []Value{ScalarString(base64.StdEncoding.EncodeToString([]byte(s)))}, nil
}

func builtinBase64d(_ *PipeContext, input Value) ([]Value, error) {
	s := extractString(input)
	b, err := base64.StdEncoding.DecodeString(s)
	if err != nil {
		// Try URL-safe encoding.
		b, err = base64.URLEncoding.DecodeString(s)
		if err != nil {
			return nil, fmt.Errorf("@base64d: invalid base64: %v", err)
		}
	}
	return []Value{ScalarString(string(b))}, nil
}

func builtinURI(_ *PipeContext, input Value) ([]Value, error) {
	s := extractString(input)
	return []Value{ScalarString(url.QueryEscape(s))}, nil
}

func builtinCSV(_ *PipeContext, input Value) ([]Value, error) {
	if input.Kind() != ListKind {
		return []Value{ScalarString(extractString(input))}, nil
	}
	items := input.List()
	parts := make([]string, len(items))
	for i, item := range items {
		s := extractString(item)
		if strings.ContainsAny(s, ",\"\n") {
			s = `"` + strings.ReplaceAll(s, `"`, `""`) + `"`
		}
		parts[i] = s
	}
	return []Value{ScalarString(strings.Join(parts, ","))}, nil
}

func builtinTSV(_ *PipeContext, input Value) ([]Value, error) {
	if input.Kind() != ListKind {
		return []Value{ScalarString(extractString(input))}, nil
	}
	items := input.List()
	parts := make([]string, len(items))
	for i, item := range items {
		s := extractString(item)
		s = strings.ReplaceAll(s, "\t", "\\t")
		s = strings.ReplaceAll(s, "\n", "\\n")
		s = strings.ReplaceAll(s, "\r", "\\r")
		s = strings.ReplaceAll(s, "\\", "\\\\")
		parts[i] = s
	}
	return []Value{ScalarString(strings.Join(parts, "\t"))}, nil
}

func builtinHTML(_ *PipeContext, input Value) ([]Value, error) {
	s := extractString(input)
	return []Value{ScalarString(html.EscapeString(s))}, nil
}

func builtinJSON(_ *PipeContext, input Value) ([]Value, error) {
	return builtinTojson(nil, input)
}

// ── Extended built-in registry ──────────────────────────────────────────

// pipeBuiltins3b extends the Phase 3a registry with Phase 3b functions.
func init() {
	// String functions (no args).
	pipeBuiltins["ascii_downcase"] = builtinAsciiDowncase
	pipeBuiltins["ascii_upcase"] = builtinAsciiUpcase
	pipeBuiltins["explode"] = builtinExplode
	pipeBuiltins["implode"] = builtinImplode

	// Collection functions (no args).
	pipeBuiltins["flatten"] = builtinFlatten
	pipeBuiltins["reverse"] = builtinReverse
	pipeBuiltins["first"] = builtinFirst
	pipeBuiltins["last"] = builtinLast

	// Numeric functions (no args).
	pipeBuiltins["fabs"] = builtinFabs
	pipeBuiltins["sqrt"] = builtinSqrt
	pipeBuiltins["pow"] = builtinPow
	pipeBuiltins["log"] = builtinLog
	pipeBuiltins["nan"] = builtinNan
	pipeBuiltins["infinite"] = builtinInfinite
	pipeBuiltins["isnan"] = builtinIsnan
	pipeBuiltins["isinfinite"] = builtinIsinfinite
	pipeBuiltins["isnormal"] = builtinIsnormal

	// Serialization (no args).
	pipeBuiltins["tojson"] = builtinTojson
	pipeBuiltins["fromjson"] = builtinFromjson
}

// pipeFuncsWith1Arg maps function names to implementations that take one
// sub-pipeline argument: name(pipeline).
var pipeFuncsWith1Arg = map[string]func(*PipeContext, Value, *Pipeline) ([]Value, error){
	"ltrimstr":  execLtrimstr,
	"rtrimstr":  execRtrimstr,
	"startswith": builtinStartswith,
	"endswith":  builtinEndswith,
	"split":     builtinSplit,
	"join":      builtinJoin,
	"test":      builtinTest,
	"match":     builtinMatch,
	"capture":   builtinCapture,
	"map":       execMap,
	"sort_by":   execSortBy,
	"group_by":  execGroupBy,
	"unique_by": execUniqueBy,
	"min_by":    execMinBy,
	"max_by":    execMaxBy,
	"nth":       builtinNth,
	"indices":   builtinIndices,
	"index":     builtinIndex,
	"rindex":    builtinRindex,
	"contains":  builtinContains,
	"inside":    builtinInside,
}

// pipeFuncsWith2Args maps function names to implementations that take two
// semicolon-separated pipeline arguments: name(arg1; arg2).
var pipeFuncsWith2Args = map[string]func(*PipeContext, Value, *Pipeline, *Pipeline) ([]Value, error){
	"gsub":  execGsub,
	"sub":   execSub,
	"limit": execLimit,
}

// pipeFormatStrings maps @format names to their implementations.
var pipeFormatStrings = map[string]func(*PipeContext, Value) ([]Value, error){
	"base64":  builtinBase64,
	"base64d": builtinBase64d,
	"uri":     builtinURI,
	"csv":     builtinCSV,
	"tsv":     builtinTSV,
	"html":    builtinHTML,
	"json":    builtinJSON,
}

// ── Helpers ─────────────────────────────────────────────────────────────

// extractString extracts a string from a Value, converting if necessary.
func extractString(v Value) string {
	if v.Kind() == NullKind {
		return "null"
	}
	if v.Kind() == ScalarKind {
		if s, ok := scalarInterface(v).(string); ok {
			return s
		}
	}
	return valueToStringValue(v)
}

// toList extracts list items from a Value. ListKind returns items directly.
// Other kinds return an error.
func toList(v Value) ([]Value, error) {
	if v.Kind() == ListKind {
		return v.List(), nil
	}
	return nil, fmt.Errorf("input must be an array, got %s", typeNameOf(v))
}

// toFloat64FromValue extracts a float64 from a Value.
func toFloat64FromValue(v Value) (float64, bool) {
	if i, ok := toInt64Value(v); ok {
		return float64(i), true
	}
	return toFloat64Value(v, 0, false)
}

// numericUnary applies a float64→float64 function to the input.
func numericUnary(input Value, name string, fn func(float64) float64) ([]Value, error) {
	f, ok := toFloat64FromValue(input)
	if !ok {
		return nil, fmt.Errorf("%s: input must be numeric, got %s", name, typeNameOf(input))
	}
	return []Value{ScalarFloat64(fn(f))}, nil
}

// valueToJSON converts a Value to a Go interface{} suitable for json.Marshal.
func valueToJSON(v Value) any {
	switch v.Kind() {
	case NullKind:
		return nil
	case ScalarKind:
		return scalarInterface(v)
	case ListKind:
		items := v.List()
		out := make([]any, len(items))
		for i, item := range items {
			out[i] = valueToJSON(item)
		}
		return out
	case MessageKind:
		return "<object>"
	case ObjectKind:
		entries := v.Entries()
		out := make(map[string]any, len(entries))
		for _, e := range entries {
			out[e.Key] = valueToJSON(e.Value)
		}
		return out
	default:
		return nil
	}
}

// jsonToValue converts a Go interface{} (from json.Unmarshal) to a Value.
func jsonToValue(v any) Value {
	if v == nil {
		return Null()
	}
	switch val := v.(type) {
	case bool:
		return ScalarBool(val)
	case float64:
		// JSON numbers are always float64.
		if val == math.Trunc(val) && val >= math.MinInt64 && val <= math.MaxInt64 {
			return ScalarInt64(int64(val))
		}
		return ScalarFloat64(val)
	case string:
		return ScalarString(val)
	case []any:
		items := make([]Value, len(val))
		for i, item := range val {
			items[i] = jsonToValue(item)
		}
		return ListVal(items)
	case map[string]any:
		entries := make([]ObjectEntry, 0, len(val))
		for k, v := range val {
			entries = append(entries, ObjectEntry{Key: k, Value: jsonToValue(v)})
		}
		return ObjectVal(entries)
	default:
		return Null()
	}
}
