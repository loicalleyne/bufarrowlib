package pbpath

import (
	"fmt"
	"regexp"
	"sort"
	"strings"

	"google.golang.org/protobuf/reflect/protoreflect"
)

// pipe_extra.go — Phase 5 additional builtins:
//   - paths / leaf_paths / paths(f)
//   - path(f) — output path expression
//   - builtins — list all builtin names
//   - utf8bytelength — byte length of string
//   - scan(re) / splits(re) — regex scanning
//   - first(f) / last(f) — 1-arg variants
//   - @text — format string
//   - sort / unique / keys_unsorted / sort_by keys / group_by keys
//   - def f(args): body; expr — user-defined functions
//   - input / inputs — multiple-input support
//   - limit(n; f) — already in pipe_funcs.go
//   - indices / index / rindex — already in pipe_funcs.go
//   - getpath / setpath / delpaths — already in pipe_object.go

// ── paths / leaf_paths / paths(f) ───────────────────────────────────────

// builtinPaths enumerates all paths in an object/array structure.
func builtinPaths(_ *PipeContext, input Value) ([]Value, error) {
	var out []Value
	enumeratePaths(input, nil, &out, false)
	return out, nil
}

// builtinLeafPaths enumerates only leaf (non-container) paths.
func builtinLeafPaths(_ *PipeContext, input Value) ([]Value, error) {
	var out []Value
	enumeratePaths(input, nil, &out, true)
	return out, nil
}

// execPathsFilter enumerates paths whose values match a filter.
// paths(f) — outputs paths for which f applied to the value is truthy.
func execPathsFilter(ctx *PipeContext, input Value, filter *Pipeline) ([]Value, error) {
	var allPaths []pathAndValue
	collectPathsAndValues(input, nil, &allPaths)
	var out []Value
	for _, pv := range allPaths {
		fVals, err := filter.execWith(ctx, []Value{pv.val})
		if err != nil {
			continue // skip errors
		}
		if isTruthyResults(fVals) {
			out = append(out, ListVal(pv.path))
		}
	}
	return out, nil
}

type pathAndValue struct {
	path []Value
	val  Value
}

func collectPathsAndValues(v Value, prefix []Value, out *[]pathAndValue) {
	*out = append(*out, pathAndValue{path: copyPath(prefix), val: v})
	switch v.Kind() {
	case ListKind:
		for i, item := range v.List() {
			next := append(copyPath(prefix), ScalarInt64(int64(i)))
			collectPathsAndValues(item, next, out)
		}
	case ObjectKind:
		for _, e := range v.Entries() {
			next := append(copyPath(prefix), ScalarString(e.Key))
			collectPathsAndValues(e.Value, next, out)
		}
	case MessageKind:
		if v.Message() != nil {
			v.Message().Range(func(fd protoreflect.FieldDescriptor, val protoreflect.Value) bool {
				next := append(copyPath(prefix), ScalarString(string(fd.Name())))
				collectPathsAndValues(FromProtoValue(val), next, out)
				return true
			})
		}
	}
}

func enumeratePaths(v Value, prefix []Value, out *[]Value, leafOnly bool) {
	isContainer := false
	switch v.Kind() {
	case ListKind:
		isContainer = len(v.List()) > 0
		for i, item := range v.List() {
			next := append(copyPath(prefix), ScalarInt64(int64(i)))
			enumeratePaths(item, next, out, leafOnly)
		}
	case ObjectKind:
		isContainer = len(v.Entries()) > 0
		for _, e := range v.Entries() {
			next := append(copyPath(prefix), ScalarString(e.Key))
			enumeratePaths(e.Value, next, out, leafOnly)
		}
	case MessageKind:
		if v.Message() != nil {
			v.Message().Range(func(fd protoreflect.FieldDescriptor, val protoreflect.Value) bool {
				isContainer = true
				next := append(copyPath(prefix), ScalarString(string(fd.Name())))
				enumeratePaths(FromProtoValue(val), next, out, leafOnly)
				return true
			})
		}
	}
	if !leafOnly || !isContainer {
		if len(prefix) > 0 { // skip root empty path
			*out = append(*out, ListVal(copyPath(prefix)))
		}
	}
}

func copyPath(p []Value) []Value {
	cp := make([]Value, len(p))
	copy(cp, p)
	return cp
}

// ── path(f) — output path expression ────────────────────────────────────

// pipePath evaluates its filter and outputs the paths that the filter
// would access. For simple field access chains (e.g., path(.a.b)),
// it reconstructs the path as an array. For more complex filters,
// it outputs paths to all matching values.
type pipePath struct {
	filter *Pipeline
}

func (p *pipePath) exec(ctx *PipeContext, input Value) ([]Value, error) {
	// Collect all paths and their values, then run filter on root
	// to find which values it produces, and output matching paths.
	var allPV []pathAndValue
	collectPathsAndValues(input, nil, &allPV)

	filterVals, err := p.filter.execWith(ctx, []Value{input})
	if err != nil {
		return nil, fmt.Errorf("path: %w", err)
	}

	// For each filter result, find matching paths by value identity.
	// This is a heuristic: if the filter produces a value that matches
	// a value at a known path, output that path.
	var out []Value
	for _, fv := range filterVals {
		for _, pv := range allPV {
			if valuesEqual(fv, pv.val) && len(pv.path) > 0 {
				out = append(out, ListVal(pv.path))
				break // first match only
			}
		}
	}
	return out, nil
}

// valuesEqual checks if two values are structurally equal.
func valuesEqual(a, b Value) bool {
	if a.Kind() != b.Kind() {
		return false
	}
	switch a.Kind() {
	case NullKind:
		return true
	case ScalarKind:
		return a.String() == b.String()
	case ListKind:
		al, bl := a.List(), b.List()
		if len(al) != len(bl) {
			return false
		}
		for i := range al {
			if !valuesEqual(al[i], bl[i]) {
				return false
			}
		}
		return true
	case ObjectKind:
		ae, be := a.Entries(), b.Entries()
		if len(ae) != len(be) {
			return false
		}
		for i := range ae {
			if ae[i].Key != be[i].Key || !valuesEqual(ae[i].Value, be[i].Value) {
				return false
			}
		}
		return true
	case MessageKind:
		// For messages, compare by identity (same proto message pointer).
		return a.Message() == b.Message()
	}
	return false
}

// ── builtins — list all builtin names ───────────────────────────────────

func builtinBuiltins(_ *PipeContext, _ Value) ([]Value, error) {
	seen := make(map[string]bool)
	var names []string

	// Collect from all registries.
	for name := range pipeBuiltins {
		if !seen[name] {
			seen[name] = true
			names = append(names, name+"/0")
		}
	}
	for name := range pipeFuncsWith1Arg {
		key := name + "/1"
		if !seen[key] {
			seen[key] = true
			names = append(names, key)
		}
		// Also include 0-arg version if it exists.
		key0 := name + "/0"
		if _, ok := pipeBuiltins[name]; ok && !seen[key0] {
			seen[key0] = true
			names = append(names, key0)
		}
	}
	for name := range pipeFuncsWith2Args {
		key := name + "/2"
		if !seen[key] {
			seen[key] = true
			names = append(names, key)
		}
	}

	// Add special builtins not in the registries.
	for _, special := range []string{
		"not/0", "empty/0", "select/1", "path/1",
		"reduce/0", "foreach/0", "if/0", "try/0",
		"label/0", "break/0", "def/0",
	} {
		if !seen[special] {
			seen[special] = true
			names = append(names, special)
		}
	}

	// Add format strings.
	for name := range pipeFormatStrings {
		key := "@" + name + "/0"
		if !seen[key] {
			seen[key] = true
			names = append(names, key)
		}
	}

	sort.Strings(names)
	out := make([]Value, len(names))
	for i, n := range names {
		out[i] = ScalarString(n)
	}
	return out, nil
}

// ── utf8bytelength ──────────────────────────────────────────────────────

func builtinUtf8bytelength(_ *PipeContext, input Value) ([]Value, error) {
	s, ok := scalarInterface(input).(string)
	if !ok {
		return nil, fmt.Errorf("utf8bytelength requires string input, got %s", typeNameOf(input))
	}
	return []Value{ScalarInt64(int64(len(s)))}, nil
}

// ── scan(re) — all non-overlapping regex matches ────────────────────────

func execScan(ctx *PipeContext, input Value, arg *Pipeline) ([]Value, error) {
	s, ok := scalarInterface(input).(string)
	if !ok {
		return nil, fmt.Errorf("scan requires string input, got %s", typeNameOf(input))
	}
	argVals, err := arg.execWith(ctx, []Value{input})
	if err != nil {
		return nil, err
	}
	if len(argVals) == 0 {
		return nil, fmt.Errorf("scan: expected regex argument")
	}
	pattern, ok := scalarInterface(argVals[0]).(string)
	if !ok {
		return nil, fmt.Errorf("scan: expected string regex argument")
	}
	re, err := regexp.Compile(pattern)
	if err != nil {
		return nil, fmt.Errorf("scan: invalid regex: %w", err)
	}
	matches := re.FindAllStringSubmatch(s, -1)
	var out []Value
	for _, m := range matches {
		if len(m) == 1 {
			// No capture groups: output the full match as a single-element array.
			out = append(out, ListVal([]Value{ScalarString(m[0])}))
		} else {
			// With capture groups: output the groups as an array.
			items := make([]Value, len(m)-1)
			for i, g := range m[1:] {
				items[i] = ScalarString(g)
			}
			out = append(out, ListVal(items))
		}
	}
	return out, nil
}

// ── splits(re) — split on regex, generating multiple values ─────────────

func execSplits(ctx *PipeContext, input Value, arg *Pipeline) ([]Value, error) {
	s, ok := scalarInterface(input).(string)
	if !ok {
		return nil, fmt.Errorf("splits requires string input, got %s", typeNameOf(input))
	}
	argVals, err := arg.execWith(ctx, []Value{input})
	if err != nil {
		return nil, err
	}
	if len(argVals) == 0 {
		return nil, fmt.Errorf("splits: expected regex argument")
	}
	pattern, ok := scalarInterface(argVals[0]).(string)
	if !ok {
		return nil, fmt.Errorf("splits: expected string regex argument")
	}
	re, err := regexp.Compile(pattern)
	if err != nil {
		return nil, fmt.Errorf("splits: invalid regex: %w", err)
	}
	parts := re.Split(s, -1)
	out := make([]Value, len(parts))
	for i, p := range parts {
		out[i] = ScalarString(p)
	}
	return out, nil
}

// ── first(f) / last(f) — 1-arg variants ────────────────────────────────

func execFirstWith(ctx *PipeContext, input Value, arg *Pipeline) ([]Value, error) {
	vals, err := arg.execWith(ctx, []Value{input})
	if err != nil {
		return nil, err
	}
	if len(vals) == 0 {
		return nil, nil // empty
	}
	return []Value{vals[0]}, nil
}

func execLastWith(ctx *PipeContext, input Value, arg *Pipeline) ([]Value, error) {
	vals, err := arg.execWith(ctx, []Value{input})
	if err != nil {
		return nil, err
	}
	if len(vals) == 0 {
		return nil, nil // empty
	}
	return []Value{vals[len(vals)-1]}, nil
}

// ── @text format string ─────────────────────────────────────────────────

func builtinText(_ *PipeContext, input Value) ([]Value, error) {
	return []Value{ScalarString(extractString(input))}, nil
}

// ── sort / unique / keys_unsorted ───────────────────────────────────────

func builtinSort(_ *PipeContext, input Value) ([]Value, error) {
	if input.Kind() != ListKind {
		return nil, fmt.Errorf("sort requires array input, got %s", typeNameOf(input))
	}
	items := input.List()
	sorted := make([]Value, len(items))
	copy(sorted, items)
	sort.SliceStable(sorted, func(i, j int) bool {
		return compareValues(sorted[i], sorted[j]) < 0
	})
	return []Value{ListVal(sorted)}, nil
}

func builtinUnique(_ *PipeContext, input Value) ([]Value, error) {
	if input.Kind() != ListKind {
		return nil, fmt.Errorf("unique requires array input, got %s", typeNameOf(input))
	}
	items := input.List()
	sorted := make([]Value, len(items))
	copy(sorted, items)
	sort.SliceStable(sorted, func(i, j int) bool {
		return compareValues(sorted[i], sorted[j]) < 0
	})
	var out []Value
	for i, v := range sorted {
		if i == 0 || !valuesEqual(v, sorted[i-1]) {
			out = append(out, v)
		}
	}
	return []Value{ListVal(out)}, nil
}

func builtinKeysUnsorted(_ *PipeContext, input Value) ([]Value, error) {
	switch input.Kind() {
	case ObjectKind:
		entries := input.Entries()
		out := make([]Value, len(entries))
		for i, e := range entries {
			out[i] = ScalarString(e.Key)
		}
		return []Value{ListVal(out)}, nil
	case MessageKind:
		if input.Message() == nil {
			return []Value{ListVal(nil)}, nil
		}
		var out []Value
		input.Message().Range(func(fd protoreflect.FieldDescriptor, _ protoreflect.Value) bool {
			out = append(out, ScalarString(string(fd.Name())))
			return true
		})
		return []Value{ListVal(out)}, nil
	case ListKind:
		out := make([]Value, len(input.List()))
		for i := range out {
			out[i] = ScalarInt64(int64(i))
		}
		return []Value{ListVal(out)}, nil
	default:
		return nil, fmt.Errorf("keys_unsorted requires object or array input, got %s", typeNameOf(input))
	}
}

// compareValues returns <0, 0, >0 following jq ordering:
// null < false < true < numbers < strings < arrays < objects
func compareValues(a, b Value) int {
	oa, ob := valueOrder(a), valueOrder(b)
	if oa != ob {
		return oa - ob
	}
	switch a.Kind() {
	case NullKind:
		return 0
	case ScalarKind:
		ai := scalarInterface(a)
		bi := scalarInterface(b)
		switch av := ai.(type) {
		case bool:
			bv, _ := bi.(bool)
			if av == bv {
				return 0
			}
			if !av {
				return -1
			}
			return 1
		case string:
			bv, _ := bi.(string)
			return strings.Compare(av, bv)
		default:
			af, _ := toFloat64FromValue(a)
			bf, _ := toFloat64FromValue(b)
			if af < bf {
				return -1
			}
			if af > bf {
				return 1
			}
			return 0
		}
	case ListKind:
		al, bl := a.List(), b.List()
		for i := 0; i < len(al) && i < len(bl); i++ {
			c := compareValues(al[i], bl[i])
			if c != 0 {
				return c
			}
		}
		return len(al) - len(bl)
	case ObjectKind:
		ae, be := a.Entries(), b.Entries()
		for i := 0; i < len(ae) && i < len(be); i++ {
			if ae[i].Key != be[i].Key {
				return strings.Compare(ae[i].Key, be[i].Key)
			}
			c := compareValues(ae[i].Value, be[i].Value)
			if c != 0 {
				return c
			}
		}
		return len(ae) - len(be)
	}
	return 0
}

func valueOrder(v Value) int {
	switch v.Kind() {
	case NullKind:
		return 0
	case ScalarKind:
		switch scalarInterface(v).(type) {
		case bool:
			b, _ := scalarInterface(v).(bool)
			if !b {
				return 1 // false
			}
			return 2 // true
		case string:
			return 4
		default:
			return 3 // number
		}
	case ListKind:
		return 5
	case ObjectKind, MessageKind:
		return 6
	}
	return 7
}

// ── User-defined functions (def) ────────────────────────────────────────

// pipeUserFunc is a user-defined function that captures its definition.
type pipeUserFunc struct {
	name   string
	params []string // parameter names (without $)
	body   *Pipeline
}

// pipeUserFuncCall calls a user-defined function at runtime.
type pipeUserFuncCall struct {
	name  string       // function name for runtime lookup
	arity int          // number of arguments (for overload resolution)
	args  []*Pipeline  // argument pipelines, one per parameter
}

func (p *pipeUserFuncCall) exec(ctx *PipeContext, input Value) ([]Value, error) {
	// Look up the function in the runtime context (not the compile-time def).
	// This is essential because parameter bindings replace the compile-time
	// pseudo-definitions with actual argument pipelines.
	var def *pipeUserFunc
	for _, fn := range ctx.userFuncs {
		if fn.name == p.name && len(fn.params) == p.arity {
			def = fn
			break
		}
	}
	if def == nil {
		return nil, fmt.Errorf("undefined function %s/%d", p.name, p.arity)
	}

	if def.body == nil {
		return nil, fmt.Errorf("function %s has nil body", p.name)
	}

	// Create child context with parameter functions bound.
	// In jq, function parameters are themselves zero-arg functions.
	// def addN(n): . + n;  addN(3)  →  n evaluates to 3
	childFuncs := make([]*pipeUserFunc, 0, len(ctx.userFuncs)+len(def.params)+1)
	// Prepend the function itself for recursive calls.
	childFuncs = append(childFuncs, def)
	for i, paramName := range def.params {
		if i < len(p.args) {
			// Create a zero-arg function that evaluates the argument pipeline.
			childFuncs = append(childFuncs, &pipeUserFunc{
				name:   paramName,
				params: nil, // zero-arg
				body:   p.args[i],
			})
		}
	}
	// Then existing funcs.
	childFuncs = append(childFuncs, ctx.userFuncs...)

	childCtx := &PipeContext{md: ctx.md, vars: ctx.vars, userFuncs: childFuncs, inputFn: ctx.inputFn}
	return def.body.execWith(childCtx, []Value{input})
}

// pipeDefBind introduces a user-defined function and evaluates the body.
type pipeDefBind struct {
	def  *pipeUserFunc
	body *Pipeline // the expression after the semicolon
}

func (p *pipeDefBind) exec(ctx *PipeContext, input Value) ([]Value, error) {
	childCtx := &PipeContext{
		md:        ctx.md,
		vars:      ctx.vars,
		userFuncs: appendUserFunc(ctx, p.def),
		inputFn:   ctx.inputFn,
	}
	return p.body.execWith(childCtx, []Value{input})
}

// appendUserFunc creates a new userFuncs slice with the new function prepended.
func appendUserFunc(ctx *PipeContext, fn *pipeUserFunc) []*pipeUserFunc {
	fns := make([]*pipeUserFunc, 0, len(ctx.userFuncs)+1)
	fns = append(fns, fn)
	fns = append(fns, ctx.userFuncs...)
	return fns
}

// lookupUserFunc searches the context for a user-defined function by name and arity.
func lookupUserFunc(ctx *PipeContext, name string, arity int) *pipeUserFunc {
	for _, fn := range ctx.userFuncs {
		if fn.name == name && len(fn.params) == arity {
			return fn
		}
	}
	return nil
}

// ── inputs — generate all remaining inputs ──────────────────────────────

func builtinInputs(ctx *PipeContext, _ Value) ([]Value, error) {
	if ctx.inputFn == nil {
		return nil, nil // no input function, produce empty
	}
	var out []Value
	for {
		v, ok := ctx.inputFn()
		if !ok {
			break
		}
		out = append(out, v)
	}
	return out, nil
}

// ── not_null — filter out null values ───────────────────────────────────

func builtinNotNull(_ *PipeContext, input Value) ([]Value, error) {
	if input.Kind() == NullKind {
		return nil, nil // empty
	}
	return []Value{input}, nil
}

// ── objects / arrays / strings / numbers / booleans / nulls / iterables / scalars ─

func builtinTypeFilter(typeName string) func(*PipeContext, Value) ([]Value, error) {
	return func(_ *PipeContext, input Value) ([]Value, error) {
		tn := typeNameOf(input)
		if tn == typeName {
			return []Value{input}, nil
		}
		return nil, nil // empty — not matching type
	}
}

func builtinIterables(_ *PipeContext, input Value) ([]Value, error) {
	switch input.Kind() {
	case ListKind, ObjectKind, MessageKind:
		return []Value{input}, nil
	}
	return nil, nil
}

func builtinScalars(_ *PipeContext, input Value) ([]Value, error) {
	switch input.Kind() {
	case NullKind, ScalarKind:
		return []Value{input}, nil
	}
	return nil, nil
}

// ── transpose ───────────────────────────────────────────────────────────

func builtinTranspose(_ *PipeContext, input Value) ([]Value, error) {
	if input.Kind() != ListKind {
		return nil, fmt.Errorf("transpose requires array of arrays, got %s", typeNameOf(input))
	}
	rows := input.List()
	if len(rows) == 0 {
		return []Value{ListVal(nil)}, nil
	}
	// Find max length.
	maxLen := 0
	for _, row := range rows {
		if row.Kind() == ListKind && len(row.List()) > maxLen {
			maxLen = len(row.List())
		}
	}
	out := make([]Value, maxLen)
	for col := 0; col < maxLen; col++ {
		colItems := make([]Value, len(rows))
		for row, rv := range rows {
			if rv.Kind() == ListKind && col < len(rv.List()) {
				colItems[row] = rv.List()[col]
			} else {
				colItems[row] = Null()
			}
		}
		out[col] = ListVal(colItems)
	}
	return []Value{ListVal(out)}, nil
}

// ── range overloads ─────────────────────────────────────────────────────
// range is already a builtin in pipe_control.go but we need to make sure
// it handles all jq overloads: range(n), range(a;b), range(a;b;step).
// The existing implementation should handle this via the parsing.

// ── ascii_downcase / ascii_upcase for non-string types ──────────────────
// Already in pipe_funcs.go.

// ── limit(n; f) — already in pipe_funcs.go ──────────────────────────────

// ── Registration ────────────────────────────────────────────────────────

func init() {
	// Zero-arg builtins.
	pipeBuiltins["paths"] = builtinPaths
	pipeBuiltins["leaf_paths"] = builtinLeafPaths
	pipeBuiltins["builtins"] = builtinBuiltins
	pipeBuiltins["utf8bytelength"] = builtinUtf8bytelength
	pipeBuiltins["sort"] = builtinSort
	pipeBuiltins["unique"] = builtinUnique
	pipeBuiltins["keys_unsorted"] = builtinKeysUnsorted
	pipeBuiltins["not_null"] = builtinNotNull
	pipeBuiltins["transpose"] = builtinTranspose
	pipeBuiltins["inputs"] = builtinInputs

	// Type-selection filters.
	pipeBuiltins["objects"] = builtinTypeFilter("object")
	pipeBuiltins["arrays"] = builtinTypeFilter("array")
	pipeBuiltins["strings"] = builtinTypeFilter("string")
	pipeBuiltins["numbers"] = builtinTypeFilter("number")
	pipeBuiltins["booleans"] = builtinTypeFilter("boolean")
	pipeBuiltins["nulls"] = builtinTypeFilter("null")
	pipeBuiltins["iterables"] = builtinIterables
	pipeBuiltins["scalars"] = builtinScalars

	// 1-arg builtins.
	pipeFuncsWith1Arg["scan"] = execScan
	pipeFuncsWith1Arg["splits"] = execSplits
	pipeFuncsWith1Arg["first"] = execFirstWith
	pipeFuncsWith1Arg["last"] = execLastWith
	pipeFuncsWith1Arg["paths"] = execPathsFilter

	// Format strings.
	pipeFormatStrings["text"] = builtinText
}
