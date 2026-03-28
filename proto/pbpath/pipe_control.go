package pbpath

import (
	"fmt"
	"math"
	"strings"

	"google.golang.org/protobuf/reflect/protoreflect"
)

// ── Variables ───────────────────────────────────────────────────────────

// pipeDynamicAccess resolves a field name at runtime against the actual
// message, rather than using a statically resolved field descriptor.
// Used when the schema context is unknown (e.g., field access on a variable).
type pipeDynamicAccess struct {
	field string
}

func (p *pipeDynamicAccess) exec(_ *PipeContext, input Value) ([]Value, error) {
	switch input.Kind() {
	case ObjectKind:
		for _, e := range input.Entries() {
			if e.Key == p.field {
				return []Value{e.Value}, nil
			}
		}
		return []Value{Null()}, nil
	case MessageKind:
		if input.Message() == nil {
			return nil, fmt.Errorf("cannot access field %q on %s", p.field, typeNameOf(input))
		}
	default:
		return nil, fmt.Errorf("cannot access field %q on %s", p.field, typeNameOf(input))
	}
	msg := input.Message()
	fd := msg.Descriptor().Fields().ByTextName(p.field)
	if fd == nil {
		return nil, fmt.Errorf("field %q not found in %s", p.field, msg.Descriptor().FullName())
	}
	val := msg.Get(fd)
	if fd.IsList() {
		list := val.List()
		out := make([]Value, list.Len())
		for j := 0; j < list.Len(); j++ {
			out[j] = FromProtoValue(list.Get(j))
		}
		return []Value{ListVal(out)}, nil
	}
	if fd.IsMap() {
		var items []Value
		val.Map().Range(func(_ protoreflect.MapKey, v protoreflect.Value) bool {
			items = append(items, FromProtoValue(v))
			return true
		})
		return []Value{ListVal(items)}, nil
	}
	return []Value{FromProtoValue(val)}, nil
}

// pipeVarRef reads a variable from the PipeContext.
type pipeVarRef struct {
	name string // without the '$' prefix
}

func (p *pipeVarRef) exec(ctx *PipeContext, _ Value) ([]Value, error) {
	if ctx.vars != nil {
		if vals, ok := ctx.vars[p.name]; ok {
			return vals, nil
		}
	}
	return nil, fmt.Errorf("undefined variable $%s", p.name)
}

// pipeVarBind evaluates body with $name bound to the result of expr.
// jq syntax: expr as $name | body
type pipeVarBind struct {
	name string    // variable name (without $)
	expr PipeExpr  // expression that produces the value(s) to bind
	body *Pipeline // rest of the pipeline evaluated with the binding
}

func (p *pipeVarBind) exec(ctx *PipeContext, input Value) ([]Value, error) {
	vals, err := p.expr.exec(ctx, input)
	if err != nil {
		return nil, err
	}
	// Clone vars map to avoid mutating parent scope.
	newVars := make(map[string][]Value, len(ctx.vars)+1)
	for k, v := range ctx.vars {
		newVars[k] = v
	}
	newVars[p.name] = vals
	childCtx := &PipeContext{md: ctx.md, vars: newVars}
	// Execute body with the new binding.
	return p.body.execWith(childCtx, []Value{input})
}

// ── If-then-else ────────────────────────────────────────────────────────

// pipeIfThenElse implements if cond then body (elif cond then body)* [else body] end.
type pipeIfThenElse struct {
	cond     PipeExpr
	thenBody *Pipeline
	elifs    []pipeElif // optional elif chains
	elseBody *Pipeline  // nil if no else clause
}

type pipeElif struct {
	cond PipeExpr
	body *Pipeline
}

func (p *pipeIfThenElse) exec(ctx *PipeContext, input Value) ([]Value, error) {
	// Evaluate primary condition.
	condVals, err := p.cond.exec(ctx, input)
	if err != nil {
		return nil, err
	}
	if isTruthyResults(condVals) {
		return p.thenBody.execWith(ctx, []Value{input})
	}
	// Evaluate elif chains.
	for _, elif := range p.elifs {
		condVals, err := elif.cond.exec(ctx, input)
		if err != nil {
			return nil, err
		}
		if isTruthyResults(condVals) {
			return elif.body.execWith(ctx, []Value{input})
		}
	}
	// Else clause.
	if p.elseBody != nil {
		return p.elseBody.execWith(ctx, []Value{input})
	}
	// No else → identity (pass input through), matching jq.
	return []Value{input}, nil
}

// ── Try-catch ───────────────────────────────────────────────────────────

// pipeTryCatch evaluates tryBody; on error evaluates catchBody (or empty).
type pipeTryCatch struct {
	tryBody   PipeExpr
	catchBody *Pipeline // nil means catch errors silently (empty output)
}

func (p *pipeTryCatch) exec(ctx *PipeContext, input Value) ([]Value, error) {
	vals, err := p.tryBody.exec(ctx, input)
	if err != nil {
		if p.catchBody != nil {
			// Pass the error message as a string to the catch body.
			return p.catchBody.execWith(ctx, []Value{ScalarString(err.Error())})
		}
		return nil, nil // try without catch: silently suppress errors
	}
	return vals, nil
}

// ── Alternative operator // ─────────────────────────────────────────────

// pipeAlternative evaluates left; if null or false, evaluates right.
// This matches jq's `//` operator.
type pipeAlternative struct {
	left, right PipeExpr
}

func (p *pipeAlternative) exec(ctx *PipeContext, input Value) ([]Value, error) {
	lVals, err := p.left.exec(ctx, input)
	if err != nil {
		return nil, err
	}
	// Check if any result is truthy.
	for _, v := range lVals {
		if isNonZeroValue(v) {
			return lVals, nil
		}
	}
	return p.right.exec(ctx, input)
}

// ── Optional operator ? ─────────────────────────────────────────────────

// pipeOptional suppresses errors from inner, returning empty on error.
type pipeOptional struct {
	inner PipeExpr
}

func (p *pipeOptional) exec(ctx *PipeContext, input Value) ([]Value, error) {
	vals, err := p.inner.exec(ctx, input)
	if err != nil {
		return nil, nil // suppress error, produce no output
	}
	return vals, nil
}

// ── Arithmetic operators ────────────────────────────────────────────────

// pipeArith applies a binary arithmetic operation.
type pipeArith struct {
	op    tokenKind // plus, minus, asterisk, slash, percent
	left  PipeExpr
	right PipeExpr
}

func (p *pipeArith) exec(ctx *PipeContext, input Value) ([]Value, error) {
	lVals, err := p.left.exec(ctx, input)
	if err != nil {
		return nil, err
	}
	rVals, err := p.right.exec(ctx, input)
	if err != nil {
		return nil, err
	}
	lv := Null()
	rv := Null()
	if len(lVals) > 0 {
		lv = lVals[0]
	}
	if len(rVals) > 0 {
		rv = rVals[0]
	}

	// Special cases for + and * on non-numeric types (matching jq).
	if p.op == plus {
		// String concatenation.
		if lv.Kind() == ScalarKind && rv.Kind() == ScalarKind {
			ls, lOk := scalarInterface(lv).(string)
			rs, rOk := scalarInterface(rv).(string)
			if lOk && rOk {
				return []Value{ScalarString(ls + rs)}, nil
			}
		}
		// Array concatenation.
		if lv.Kind() == ListKind && rv.Kind() == ListKind {
			combined := make([]Value, 0, len(lv.List())+len(rv.List()))
			combined = append(combined, lv.List()...)
			combined = append(combined, rv.List()...)
			return []Value{ListVal(combined)}, nil
		}
		// Object merge: {a:1} + {b:2} → {a:1,b:2}.
		if lv.Kind() == ObjectKind && rv.Kind() == ObjectKind {
			return []Value{objMerge(lv, rv)}, nil
		}
		// null + x = x, x + null = x
		if lv.IsNull() {
			return []Value{rv}, nil
		}
		if rv.IsNull() {
			return []Value{lv}, nil
		}
	}

	if p.op == asterisk {
		// Object recursive merge: {a:{x:1}} * {a:{y:2}} → {a:{x:1,y:2}}.
		if lv.Kind() == ObjectKind && rv.Kind() == ObjectKind {
			return []Value{objRecursiveMerge(lv, rv)}, nil
		}
	}

	// Numeric arithmetic.
	li, lIsInt := toInt64Value(lv)
	ri, rIsInt := toInt64Value(rv)

	if lIsInt && rIsInt {
		switch p.op {
		case plus:
			return []Value{ScalarInt64(li + ri)}, nil
		case minus:
			return []Value{ScalarInt64(li - ri)}, nil
		case asterisk:
			return []Value{ScalarInt64(li * ri)}, nil
		case slash:
			if ri == 0 {
				return nil, fmt.Errorf("division by zero")
			}
			return []Value{ScalarInt64(li / ri)}, nil
		case percent:
			if ri == 0 {
				return nil, fmt.Errorf("modulo by zero")
			}
			return []Value{ScalarInt64(li % ri)}, nil
		}
	}

	// Float promotion.
	lf, lIsFloat := toFloat64FromValue(lv)
	rf, rIsFloat := toFloat64FromValue(rv)
	if (lIsFloat || lIsInt) && (rIsFloat || rIsInt) {
		if lIsInt {
			lf = float64(li)
		}
		if rIsInt {
			rf = float64(ri)
		}
		switch p.op {
		case plus:
			return []Value{ScalarFloat64(lf + rf)}, nil
		case minus:
			return []Value{ScalarFloat64(lf - rf)}, nil
		case asterisk:
			return []Value{ScalarFloat64(lf * rf)}, nil
		case slash:
			if rf == 0 {
				return nil, fmt.Errorf("division by zero")
			}
			return []Value{ScalarFloat64(lf / rf)}, nil
		case percent:
			if rf == 0 {
				return nil, fmt.Errorf("modulo by zero")
			}
			return []Value{ScalarFloat64(math.Mod(lf, rf))}, nil
		}
	}

	// Subtraction on strings is not defined. Match jq error.
	opStr := "+"
	switch p.op {
	case minus:
		opStr = "-"
	case asterisk:
		opStr = "*"
	case slash:
		opStr = "/"
	case percent:
		opStr = "%"
	}
	return nil, fmt.Errorf("cannot apply %s to %s and %s", opStr, typeNameOf(lv), typeNameOf(rv))
}

// ── Unary negation ──────────────────────────────────────────────────────

// pipeNegate negates a numeric value.
type pipeNegate struct {
	inner PipeExpr
}

func (p *pipeNegate) exec(ctx *PipeContext, input Value) ([]Value, error) {
	vals, err := p.inner.exec(ctx, input)
	if err != nil {
		return nil, err
	}
	if len(vals) == 0 {
		return nil, nil
	}
	v := vals[0]
	if i, ok := toInt64Value(v); ok {
		return []Value{ScalarInt64(-i)}, nil
	}
	if f, ok := toFloat64FromValue(v); ok {
		return []Value{ScalarFloat64(-f)}, nil
	}
	return nil, fmt.Errorf("cannot negate %s", typeNameOf(v))
}

// ── Reduce ──────────────────────────────────────────────────────────────

// pipeReduce implements: reduce expr as $name (init; update)
type pipeReduce struct {
	expr   PipeExpr  // expression that generates values
	varN   string    // variable name (without $)
	init   *Pipeline // initial accumulator expression
	update *Pipeline // update expression (has $name and $acc available)
}

func (p *pipeReduce) exec(ctx *PipeContext, input Value) ([]Value, error) {
	// Evaluate the stream expression.
	stream, err := p.expr.exec(ctx, input)
	if err != nil {
		return nil, err
	}
	// Evaluate init.
	accVals, err := p.init.execWith(ctx, []Value{input})
	if err != nil {
		return nil, err
	}
	acc := Null()
	if len(accVals) > 0 {
		acc = accVals[0]
	}
	// Iterate over stream values.
	for _, sv := range stream {
		// Bind $name to current stream value, input is the accumulator.
		newVars := make(map[string][]Value, len(ctx.vars)+1)
		for k, v := range ctx.vars {
			newVars[k] = v
		}
		newVars[p.varN] = []Value{sv}
		childCtx := &PipeContext{md: ctx.md, vars: newVars}
		result, err := p.update.execWith(childCtx, []Value{acc})
		if err != nil {
			return nil, err
		}
		if len(result) > 0 {
			acc = result[0]
		}
	}
	return []Value{acc}, nil
}

// ── Foreach ─────────────────────────────────────────────────────────────

// pipeForeach implements: foreach expr as $name (init; update; extract)
// Emits extract result for each iteration.
type pipeForeach struct {
	expr    PipeExpr
	varN    string
	init    *Pipeline
	update  *Pipeline
	extract *Pipeline // nil means emit accumulator after each step
}

func (p *pipeForeach) exec(ctx *PipeContext, input Value) ([]Value, error) {
	stream, err := p.expr.exec(ctx, input)
	if err != nil {
		return nil, err
	}
	accVals, err := p.init.execWith(ctx, []Value{input})
	if err != nil {
		return nil, err
	}
	acc := Null()
	if len(accVals) > 0 {
		acc = accVals[0]
	}
	var out []Value
	for _, sv := range stream {
		newVars := make(map[string][]Value, len(ctx.vars)+1)
		for k, v := range ctx.vars {
			newVars[k] = v
		}
		newVars[p.varN] = []Value{sv}
		childCtx := &PipeContext{md: ctx.md, vars: newVars}
		result, err := p.update.execWith(childCtx, []Value{acc})
		if err != nil {
			return nil, err
		}
		if len(result) > 0 {
			acc = result[0]
		}
		if p.extract != nil {
			extracted, err := p.extract.execWith(childCtx, []Value{acc})
			if err != nil {
				return nil, err
			}
			out = append(out, extracted...)
		} else {
			out = append(out, acc)
		}
	}
	return out, nil
}

// ── Recurse ─────────────────────────────────────────────────────────────

// builtinRecurse implements `recurse` and `recurse(f)`.
// It recursively applies f (default: .[]) and emits all values.
func execRecurse(ctx *PipeContext, input Value, inner *Pipeline) ([]Value, error) {
	var out []Value
	seen := make(map[string]bool) // prevent infinite loops on scalars
	var walk func(v Value) error
	walk = func(v Value) error {
		key := v.String()
		if seen[key] {
			return nil
		}
		seen[key] = true
		out = append(out, v)
		children, err := inner.execWith(ctx, []Value{v})
		if err != nil {
			return nil // recurse silently stops on errors
		}
		for _, child := range children {
			if err := walk(child); err != nil {
				return err
			}
		}
		return nil
	}
	if err := walk(input); err != nil {
		return nil, err
	}
	return out, nil
}

// builtinRecurseDefault is the zero-arg `recurse` (recurse(.[])).
func builtinRecurseDefault(ctx *PipeContext, input Value) ([]Value, error) {
	defaultPipe := &Pipeline{exprs: []PipeExpr{pipeIterate{}}, md: ctx.md}
	return execRecurse(ctx, input, defaultPipe)
}

// ── While / Until / Repeat ──────────────────────────────────────────────

// execWhile implements: while(cond; update) — emit values while cond is truthy.
func execWhile(_ *PipeContext, input Value, arg1, arg2 *Pipeline) ([]Value, error) {
	var out []Value
	current := input
	for i := 0; i < 10000; i++ { // safety limit
		condVals, err := arg1.ExecOne(current)
		if err != nil {
			return out, nil
		}
		if !isTruthyResults(condVals) {
			break
		}
		out = append(out, current)
		next, err := arg2.ExecOne(current)
		if err != nil {
			return out, nil
		}
		if len(next) == 0 {
			break
		}
		current = next[0]
	}
	return out, nil
}

// execUntil implements: until(cond; update) — apply update until cond is truthy.
func execUntil(_ *PipeContext, input Value, arg1, arg2 *Pipeline) ([]Value, error) {
	current := input
	for i := 0; i < 10000; i++ { // safety limit
		condVals, err := arg1.ExecOne(current)
		if err != nil {
			return []Value{current}, nil
		}
		if isTruthyResults(condVals) {
			return []Value{current}, nil
		}
		next, err := arg2.ExecOne(current)
		if err != nil {
			return []Value{current}, nil
		}
		if len(next) == 0 {
			break
		}
		current = next[0]
	}
	return []Value{current}, nil
}

// execRepeat implements: repeat(f) — apply f repeatedly, emitting each result.
func execRepeat(ctx *PipeContext, input Value, inner *Pipeline) ([]Value, error) {
	var out []Value
	current := input
	for i := 0; i < 10000; i++ { // safety limit
		out = append(out, current)
		next, err := inner.execWith(ctx, []Value{current})
		if err != nil {
			break
		}
		if len(next) == 0 {
			break
		}
		current = next[0]
	}
	return out, nil
}

// ── Label-break ─────────────────────────────────────────────────────────

// labelBreakError is used to implement label-break out via panic/recover.
type labelBreakError struct {
	label string
	value Value
}

// pipeLabel implements: label $name | body
// Body can use `break $name` to exit early.
type pipeLabel struct {
	name string
	body *Pipeline
}

func (p *pipeLabel) exec(ctx *PipeContext, input Value) (result []Value, retErr error) {
	defer func() {
		if r := recover(); r != nil {
			if lb, ok := r.(labelBreakError); ok && lb.label == p.name {
				result = []Value{lb.value}
				retErr = nil
				return
			}
			panic(r) // re-panic for unrelated panics
		}
	}()
	return p.body.execWith(ctx, []Value{input})
}

// pipeBreak implements: break $name
type pipeBreak struct {
	name string
}

func (p *pipeBreak) exec(_ *PipeContext, input Value) ([]Value, error) {
	panic(labelBreakError{label: p.name, value: input})
}

// ── Helpers ─────────────────────────────────────────────────────────────

// isTruthyResults returns true if any result value is truthy (non-zero).
func isTruthyResults(vals []Value) bool {
	for _, v := range vals {
		if isNonZeroValue(v) {
			return true
		}
	}
	return false
}

// ── Register Phase 3c built-ins ─────────────────────────────────────────

func init() {
	pipeBuiltins["recurse"] = builtinRecurseDefault
	pipeBuiltins["env"] = builtinEnv
	pipeBuiltins["debug"] = builtinDebug
	pipeBuiltins["error"] = builtinError
	pipeBuiltins["ascii"] = builtinAscii
	pipeBuiltins["min"] = builtinMin
	pipeBuiltins["max"] = builtinMax
	pipeBuiltins["any"] = builtinAny
	pipeBuiltins["all"] = builtinAll
	pipeBuiltins["range"] = builtinRange
	pipeBuiltins["floor"] = builtinFloor
	pipeBuiltins["ceil"] = builtinCeil
	pipeBuiltins["round"] = builtinRound
	pipeBuiltins["input"] = builtinInput
	pipeBuiltins["null"] = func(_ *PipeContext, _ Value) ([]Value, error) {
		return []Value{Null()}, nil
	}

	// 1-arg functions
	pipeFuncsWith1Arg["recurse"] = execRecurse
	pipeFuncsWith1Arg["repeat"] = execRepeat
	pipeFuncsWith1Arg["any"] = execAnyWith
	pipeFuncsWith1Arg["all"] = execAllWith
	pipeFuncsWith1Arg["error"] = execErrorWith

	// 2-arg functions
	pipeFuncsWith2Args["while"] = execWhile
	pipeFuncsWith2Args["until"] = execUntil
}

// builtinEnv returns null (no environment variables in protobuf context).
func builtinEnv(_ *PipeContext, _ Value) ([]Value, error) {
	return []Value{Null()}, nil
}

// builtinDebug passes the input through (in a full jq this would log).
func builtinDebug(_ *PipeContext, input Value) ([]Value, error) {
	return []Value{input}, nil
}

// builtinError raises an error with the input as message.
func builtinError(_ *PipeContext, input Value) ([]Value, error) {
	return nil, fmt.Errorf("%s", extractString(input))
}

// execErrorWith raises an error with the pipeline argument as message: error(msg).
func execErrorWith(_ *PipeContext, input Value, inner *Pipeline) ([]Value, error) {
	result, err := inner.ExecOne(input)
	if err != nil {
		return nil, err
	}
	msg := ""
	if len(result) > 0 {
		msg = extractString(result[0])
	}
	return nil, fmt.Errorf("%s", msg)
}

// builtinAscii returns the ASCII code point of the first character.
func builtinAscii(_ *PipeContext, input Value) ([]Value, error) {
	s := extractString(input)
	if len(s) == 0 {
		return []Value{Null()}, nil
	}
	return []Value{ScalarInt64(int64(s[0]))}, nil
}

// builtinMin returns the minimum of an array.
func builtinMin(_ *PipeContext, input Value) ([]Value, error) {
	if input.Kind() != ListKind {
		return nil, fmt.Errorf("min: input must be an array")
	}
	items := input.List()
	if len(items) == 0 {
		return []Value{Null()}, nil
	}
	best := items[0]
	for _, item := range items[1:] {
		cmp, ok := compareValuesV(item, best)
		if ok && cmp < 0 {
			best = item
		}
	}
	return []Value{best}, nil
}

// builtinMax returns the maximum of an array.
func builtinMax(_ *PipeContext, input Value) ([]Value, error) {
	if input.Kind() != ListKind {
		return nil, fmt.Errorf("max: input must be an array")
	}
	items := input.List()
	if len(items) == 0 {
		return []Value{Null()}, nil
	}
	best := items[0]
	for _, item := range items[1:] {
		cmp, ok := compareValuesV(item, best)
		if ok && cmp > 0 {
			best = item
		}
	}
	return []Value{best}, nil
}

// builtinAny returns true if any array element is truthy.
func builtinAny(_ *PipeContext, input Value) ([]Value, error) {
	if input.Kind() != ListKind {
		return []Value{ScalarBool(isNonZeroValue(input))}, nil
	}
	for _, item := range input.List() {
		if isNonZeroValue(item) {
			return []Value{ScalarBool(true)}, nil
		}
	}
	return []Value{ScalarBool(false)}, nil
}

// builtinAll returns true if all array elements are truthy.
func builtinAll(_ *PipeContext, input Value) ([]Value, error) {
	if input.Kind() != ListKind {
		return []Value{ScalarBool(isNonZeroValue(input))}, nil
	}
	for _, item := range input.List() {
		if !isNonZeroValue(item) {
			return []Value{ScalarBool(false)}, nil
		}
	}
	return []Value{ScalarBool(true)}, nil
}

// execAnyWith: any(f) — true if f produces truthy for any element.
func execAnyWith(ctx *PipeContext, input Value, inner *Pipeline) ([]Value, error) {
	items, err := toList(input)
	if err != nil {
		return nil, fmt.Errorf("any: %v", err)
	}
	for _, item := range items {
		result, err := inner.execWith(ctx, []Value{item})
		if err != nil {
			return nil, err
		}
		if isTruthyResults(result) {
			return []Value{ScalarBool(true)}, nil
		}
	}
	return []Value{ScalarBool(false)}, nil
}

// execAllWith: all(f) — true if f produces truthy for all elements.
func execAllWith(ctx *PipeContext, input Value, inner *Pipeline) ([]Value, error) {
	items, err := toList(input)
	if err != nil {
		return nil, fmt.Errorf("all: %v", err)
	}
	for _, item := range items {
		result, err := inner.execWith(ctx, []Value{item})
		if err != nil {
			return nil, err
		}
		if !isTruthyResults(result) {
			return []Value{ScalarBool(false)}, nil
		}
	}
	return []Value{ScalarBool(true)}, nil
}

// builtinRange generates a sequence of numbers: range produces 0..n-1 from input n.
func builtinRange(_ *PipeContext, input Value) ([]Value, error) {
	n, ok := toInt64Value(input)
	if !ok {
		return nil, fmt.Errorf("range: input must be a number")
	}
	if n < 0 || n > 10000 {
		return nil, fmt.Errorf("range: count %d out of bounds", n)
	}
	out := make([]Value, n)
	for i := int64(0); i < n; i++ {
		out[i] = ScalarInt64(i)
	}
	return out, nil
}

// builtinFloor returns the floor of a number.
func builtinFloor(_ *PipeContext, input Value) ([]Value, error) {
	f, ok := toFloat64FromValue(input)
	if !ok {
		if i, ok := toInt64Value(input); ok {
			return []Value{ScalarInt64(i)}, nil
		}
		return nil, fmt.Errorf("floor: input must be numeric")
	}
	return []Value{ScalarInt64(int64(math.Floor(f)))}, nil
}

// builtinCeil returns the ceiling of a number.
func builtinCeil(_ *PipeContext, input Value) ([]Value, error) {
	f, ok := toFloat64FromValue(input)
	if !ok {
		if i, ok := toInt64Value(input); ok {
			return []Value{ScalarInt64(i)}, nil
		}
		return nil, fmt.Errorf("ceil: input must be numeric")
	}
	return []Value{ScalarInt64(int64(math.Ceil(f)))}, nil
}

// builtinRound returns the rounded value of a number.
func builtinRound(_ *PipeContext, input Value) ([]Value, error) {
	f, ok := toFloat64FromValue(input)
	if !ok {
		if i, ok := toInt64Value(input); ok {
			return []Value{ScalarInt64(i)}, nil
		}
		return nil, fmt.Errorf("round: input must be numeric")
	}
	return []Value{ScalarInt64(int64(math.Round(f)))}, nil
}

// builtinInput is a no-op placeholder (jq reads from stdin; not applicable here).
func builtinInput(_ *PipeContext, _ Value) ([]Value, error) {
	return []Value{Null()}, nil
}

// ── String interpolation helper ─────────────────────────────────────────

// pipeStringInterp implements jq's string interpolation: "hello \(expr)".
// For now this is a future placeholder. The parser handles it.
type pipeStringInterp struct {
	parts []PipeExpr // alternating literal strings and interpolated expressions
}

func (p *pipeStringInterp) exec(ctx *PipeContext, input Value) ([]Value, error) {
	var sb strings.Builder
	for _, part := range p.parts {
		vals, err := part.exec(ctx, input)
		if err != nil {
			return nil, err
		}
		for _, v := range vals {
			sb.WriteString(extractString(v))
		}
	}
	return []Value{ScalarString(sb.String())}, nil
}
