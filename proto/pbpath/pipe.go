package pbpath

import (
	"fmt"
	"strconv"
	"strings"

	"google.golang.org/protobuf/reflect/protoreflect"
)

// ── Pipeline context ────────────────────────────────────────────────────

// PipeContext carries execution state through a pipeline.
// It holds the root message descriptor for schema-aware operations
// and variable bindings from `as $name` expressions.
type PipeContext struct {
	md        protoreflect.MessageDescriptor
	vars      map[string][]Value // variable bindings ($name → values)
	userFuncs []*pipeUserFunc    // user-defined functions from `def`
	inputFn   func() (Value, bool) // optional input source for `input`/`inputs`
}

// ── Pipeline ────────────────────────────────────────────────────────────

// Pipeline is a compiled sequence of pipe-separated expressions.
// Each expression receives every value from the current stream and produces
// zero or more output values for the next expression.
//
// Create a Pipeline via [ParsePipeline].
type Pipeline struct {
	exprs []PipeExpr
	md    protoreflect.MessageDescriptor
}

// Exec runs the pipeline against the given input stream.
// Each expression is applied to every value in the current stream; results
// are concatenated to form the input for the next expression.
func (p *Pipeline) Exec(input []Value) ([]Value, error) {
	ctx := &PipeContext{md: p.md}
	current := input
	for _, expr := range p.exprs {
		var next []Value
		for _, v := range current {
			out, err := expr.exec(ctx, v)
			if err != nil {
				return nil, err
			}
			next = append(next, out...)
		}
		current = next
	}
	return current, nil
}

// ExecOne runs the pipeline with a single input [Value].
func (p *Pipeline) ExecOne(input Value) ([]Value, error) {
	return p.Exec([]Value{input})
}

// ExecMessage runs the pipeline against a protobuf message.
func (p *Pipeline) ExecMessage(msg protoreflect.Message) ([]Value, error) {
	return p.Exec([]Value{MessageVal(msg)})
}

// execWith runs the pipeline with a caller-provided PipeContext.
// Used internally for variable bindings and control flow.
func (p *Pipeline) execWith(ctx *PipeContext, input []Value) ([]Value, error) {
	current := input
	for _, expr := range p.exprs {
		var next []Value
		for _, v := range current {
			out, err := expr.exec(ctx, v)
			if err != nil {
				return nil, err
			}
			next = append(next, out...)
		}
		current = next
	}
	return current, nil
}

// ── PipeExpr interface ──────────────────────────────────────────────────

// PipeExpr is a node in a pipeline expression tree. Each node receives one
// input [Value] and produces zero or more output Values.
//
// Implementations include path access, iteration, collection, comparisons,
// boolean logic, built-in functions, and literal constants.
type PipeExpr interface {
	exec(ctx *PipeContext, input Value) ([]Value, error)
}

// ── Identity: "." ───────────────────────────────────────────────────────

// pipeIdentity passes the input through unchanged.
type pipeIdentity struct{}

func (pipeIdentity) exec(_ *PipeContext, input Value) ([]Value, error) {
	return []Value{input}, nil
}

// ── Path access: ".field", ".field.sub.deep" ────────────────────────────

// pipePathAccess navigates into message fields using pre-resolved field
// descriptors. For a chain like ".a.b.c", fields contains [fd_a, fd_b, fd_c].
type pipePathAccess struct {
	fields []protoreflect.FieldDescriptor
}

func (p *pipePathAccess) exec(_ *PipeContext, input Value) ([]Value, error) {
	if input.Kind() != MessageKind || input.Message() == nil {
		return []Value{Null()}, nil
	}
	msg := input.Message()
	for i, fd := range p.fields {
		val := msg.Get(fd)
		if i == len(p.fields)-1 {
			// Last field — return the value.
			// For repeated fields, wrap the proto list as a ListKind Value.
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
		// Intermediate field — must be a message to continue.
		if fd.Message() == nil {
			return []Value{Null()}, nil
		}
		msg = val.Message()
	}
	return []Value{Null()}, nil
}

// ── Iterate: ".[]" ──────────────────────────────────────────────────────

// pipeIterate expands a list into its elements, or a message into its
// field values (matching jq's .[] on arrays/objects).
type pipeIterate struct{}

func (pipeIterate) exec(_ *PipeContext, input Value) ([]Value, error) {
	switch input.Kind() {
	case ListKind:
		items := input.List()
		if len(items) == 0 {
			return nil, nil
		}
		return items, nil
	case ObjectKind:
		entries := input.Entries()
		if len(entries) == 0 {
			return nil, nil
		}
		out := make([]Value, len(entries))
		for i, e := range entries {
			out[i] = e.Value
		}
		return out, nil
	case ScalarKind:
		// Check for proto List or Map wrapped as scalar.
		if pv := input.ProtoValue(); pv.IsValid() {
			switch iface := pv.Interface().(type) {
			case protoreflect.List:
				out := make([]Value, iface.Len())
				for i := 0; i < iface.Len(); i++ {
					out[i] = FromProtoValue(iface.Get(i))
				}
				return out, nil
			case protoreflect.Map:
				var out []Value
				iface.Range(func(_ protoreflect.MapKey, v protoreflect.Value) bool {
					out = append(out, FromProtoValue(v))
					return true
				})
				return out, nil
			}
		}
		return nil, fmt.Errorf("cannot iterate over scalar")
	case MessageKind:
		if input.Message() == nil {
			return nil, nil
		}
		var out []Value
		input.Message().Range(func(_ protoreflect.FieldDescriptor, val protoreflect.Value) bool {
			out = append(out, FromProtoValue(val))
			return true
		})
		return out, nil
	default:
		return nil, fmt.Errorf("cannot iterate over null")
	}
}

// ── Index: ".[n]" ───────────────────────────────────────────────────────

// pipeIndex selects the nth element from a list. Negative indices are
// supported (Python-style: -1 = last).
type pipeIndex struct {
	index int
}

func (p *pipeIndex) exec(_ *PipeContext, input Value) ([]Value, error) {
	if input.Kind() == ListKind {
		v := input.Index(p.index)
		return []Value{v}, nil
	}
	return []Value{Null()}, nil
}

// ── Collect: "[pipeline]" ───────────────────────────────────────────────

// pipeCollect runs an inner pipeline and gathers all outputs into a single
// [ListKind] value.
type pipeCollect struct {
	inner *Pipeline
}

func (p *pipeCollect) exec(ctx *PipeContext, input Value) ([]Value, error) {
	out, err := p.inner.execWith(ctx, []Value{input})
	if err != nil {
		return nil, err
	}
	return []Value{ListVal(out)}, nil
}

// ── Select: "select(expr)" ──────────────────────────────────────────────

// pipeSelectExpr keeps the input if the predicate pipeline produces a
// truthy result; otherwise it drops the input (produces no output).
type pipeSelectExpr struct {
	pred PipeExpr
}

func (p *pipeSelectExpr) exec(ctx *PipeContext, input Value) ([]Value, error) {
	result, err := p.pred.exec(ctx, input)
	if err != nil {
		return nil, err
	}
	for _, v := range result {
		if isNonZeroValue(v) {
			return []Value{input}, nil
		}
	}
	return nil, nil // drop
}

// ── Comparison: expr == expr, expr != expr, etc. ────────────────────────

// pipeCompare evaluates left and right sub-expressions against the input
// and compares their first results using the given operator.
type pipeCompare struct {
	op    tokenKind // eqeq, bangeq, langle, langleeq, rangle, rangleeq
	left  PipeExpr
	right PipeExpr
}

func (p *pipeCompare) exec(ctx *PipeContext, input Value) ([]Value, error) {
	lVals, err := p.left.exec(ctx, input)
	if err != nil {
		return nil, err
	}
	rVals, err := p.right.exec(ctx, input)
	if err != nil {
		return nil, err
	}
	var lv, rv Value
	if len(lVals) > 0 {
		lv = lVals[0]
	}
	if len(rVals) > 0 {
		rv = rVals[0]
	}

	cmp, ok := compareValuesV(lv, rv)
	if !ok {
		// Non-comparable: equality checks return false, order checks return false.
		return []Value{ScalarBool(false)}, nil
	}
	var result bool
	switch p.op {
	case eqeq:
		result = cmp == 0
	case bangeq:
		result = cmp != 0
	case langle:
		result = cmp < 0
	case langleeq:
		result = cmp <= 0
	case rangle:
		result = cmp > 0
	case rangleeq:
		result = cmp >= 0
	}
	return []Value{ScalarBool(result)}, nil
}

// ── Boolean operators: and, or, not ─────────────────────────────────────

// pipeBoolAnd evaluates left and right; returns true if both are truthy.
type pipeBoolAnd struct {
	left, right PipeExpr
}

func (p *pipeBoolAnd) exec(ctx *PipeContext, input Value) ([]Value, error) {
	lVals, err := p.left.exec(ctx, input)
	if err != nil {
		return nil, err
	}
	lTruthy := false
	for _, v := range lVals {
		if isNonZeroValue(v) {
			lTruthy = true
			break
		}
	}
	if !lTruthy {
		return []Value{ScalarBool(false)}, nil
	}
	rVals, err := p.right.exec(ctx, input)
	if err != nil {
		return nil, err
	}
	for _, v := range rVals {
		if isNonZeroValue(v) {
			return []Value{ScalarBool(true)}, nil
		}
	}
	return []Value{ScalarBool(false)}, nil
}

// pipeBoolOr evaluates left and right; returns true if either is truthy.
type pipeBoolOr struct {
	left, right PipeExpr
}

func (p *pipeBoolOr) exec(ctx *PipeContext, input Value) ([]Value, error) {
	lVals, err := p.left.exec(ctx, input)
	if err != nil {
		return nil, err
	}
	for _, v := range lVals {
		if isNonZeroValue(v) {
			return []Value{ScalarBool(true)}, nil
		}
	}
	rVals, err := p.right.exec(ctx, input)
	if err != nil {
		return nil, err
	}
	for _, v := range rVals {
		if isNonZeroValue(v) {
			return []Value{ScalarBool(true)}, nil
		}
	}
	return []Value{ScalarBool(false)}, nil
}

// pipeNot produces true when the input is falsy, false when truthy.
// In jq, "not" is a filter (not an operator), applied via pipe: `. | not`.
type pipeNot struct{}

func (pipeNot) exec(_ *PipeContext, input Value) ([]Value, error) {
	return []Value{ScalarBool(!isNonZeroValue(input))}, nil
}

// ── Literal: string, number, bool, null ─────────────────────────────────

// pipeLiteral produces a constant value, ignoring the input.
type pipeLiteral struct {
	val Value
}

func (p *pipeLiteral) exec(_ *PipeContext, _ Value) ([]Value, error) {
	return []Value{p.val}, nil
}

// ── Empty: produces no output ───────────────────────────────────────────

// pipeEmpty produces zero output values, discarding the input.
type pipeEmpty struct{}

func (pipeEmpty) exec(_ *PipeContext, _ Value) ([]Value, error) {
	return nil, nil
}

// ── Built-in functions ──────────────────────────────────────────────────

// pipeBuiltin wraps a named built-in function.
type pipeBuiltin struct {
	name string
	fn   func(*PipeContext, Value) ([]Value, error)
}

func (p *pipeBuiltin) exec(ctx *PipeContext, input Value) ([]Value, error) {
	return p.fn(ctx, input)
}

// builtinLength returns the length of lists, strings, maps, and messages.
// Null returns 0 (matching jq's `null | length` → 0).
func builtinLength(_ *PipeContext, input Value) ([]Value, error) {
	switch input.Kind() {
	case NullKind:
		return []Value{ScalarInt64(0)}, nil
	case ListKind:
		return []Value{ScalarInt64(int64(len(input.List())))}, nil
	case MessageKind:
		if input.Message() == nil {
			return []Value{ScalarInt64(0)}, nil
		}
		// Count populated fields.
		n := 0
		input.Message().Range(func(_ protoreflect.FieldDescriptor, _ protoreflect.Value) bool {
			n++
			return true
		})
		return []Value{ScalarInt64(int64(n))}, nil
	case ScalarKind:
		iface := scalarInterface(input)
		if iface == nil {
			return []Value{ScalarInt64(0)}, nil
		}
		switch v := iface.(type) {
		case string:
			return []Value{ScalarInt64(int64(len(v)))}, nil
		case []byte:
			return []Value{ScalarInt64(int64(len(v)))}, nil
		default:
			// For numbers, jq returns absolute value; we return 1 for non-zero, 0 for zero.
			// Actually jq `length` on numbers returns abs value. Let's match that.
			if i, ok := toInt64Value(input); ok {
				if i < 0 {
					i = -i
				}
				return []Value{ScalarInt64(i)}, nil
			}
			if f, ok := toFloat64Value(input, 0, false); ok {
				if f < 0 {
					f = -f
				}
				return []Value{ScalarFloat64(f)}, nil
			}
			return []Value{ScalarInt64(0)}, nil
		}
	default:
		return []Value{ScalarInt64(0)}, nil
	}
}

// builtinType returns the type name of the value as a string.
// Returns "null", "boolean", "number", "string", "array", "object".
func builtinType(_ *PipeContext, input Value) ([]Value, error) {
	switch input.Kind() {
	case NullKind:
		return []Value{ScalarString("null")}, nil
	case ListKind:
		return []Value{ScalarString("array")}, nil
	case MessageKind:
		return []Value{ScalarString("object")}, nil
	case ScalarKind:
		iface := scalarInterface(input)
		if iface == nil {
			return []Value{ScalarString("null")}, nil
		}
		switch iface.(type) {
		case bool:
			return []Value{ScalarString("boolean")}, nil
		case string:
			return []Value{ScalarString("string")}, nil
		case []byte:
			return []Value{ScalarString("string")}, nil
		default:
			return []Value{ScalarString("number")}, nil
		}
	default:
		return []Value{ScalarString("null")}, nil
	}
}

// builtinKeys returns the keys of a message (field names) or list (indices).
func builtinKeys(_ *PipeContext, input Value) ([]Value, error) {
	switch input.Kind() {
	case ListKind:
		items := input.List()
		out := make([]Value, len(items))
		for i := range items {
			out[i] = ScalarInt64(int64(i))
		}
		return []Value{ListVal(out)}, nil
	case MessageKind:
		if input.Message() == nil {
			return []Value{ListVal(nil)}, nil
		}
		var keys []Value
		input.Message().Range(func(fd protoreflect.FieldDescriptor, _ protoreflect.Value) bool {
			keys = append(keys, ScalarString(string(fd.Name())))
			return true
		})
		return []Value{ListVal(keys)}, nil
	default:
		return nil, fmt.Errorf("keys: cannot get keys of %s", typeNameOf(input))
	}
}

// builtinValues returns the values of a message or list.
func builtinValues(_ *PipeContext, input Value) ([]Value, error) {
	switch input.Kind() {
	case ListKind:
		// values on an array is identity in jq.
		return []Value{input}, nil
	case MessageKind:
		if input.Message() == nil {
			return []Value{ListVal(nil)}, nil
		}
		var vals []Value
		input.Message().Range(func(_ protoreflect.FieldDescriptor, val protoreflect.Value) bool {
			vals = append(vals, FromProtoValue(val))
			return true
		})
		return []Value{ListVal(vals)}, nil
	default:
		return nil, fmt.Errorf("values: cannot get values of %s", typeNameOf(input))
	}
}

// builtinAdd reduces a list by "adding" its elements. For numbers, sums;
// for strings, concatenates; for lists, flattens one level. Null is the
// identity element (like jq: `null | add` → null, `[] | add` → null).
func builtinAdd(_ *PipeContext, input Value) ([]Value, error) {
	items := input.List()
	if input.Kind() != ListKind || len(items) == 0 {
		return []Value{Null()}, nil
	}

	// Determine the type from the first non-null element.
	var firstIface any
	for _, item := range items {
		if item.IsNull() {
			continue
		}
		firstIface = scalarInterface(item)
		if firstIface != nil {
			break
		}
		// If first non-null is a list, do list flattening.
		if item.Kind() == ListKind {
			var flat []Value
			for _, it := range items {
				if it.Kind() == ListKind {
					flat = append(flat, it.List()...)
				}
			}
			return []Value{ListVal(flat)}, nil
		}
	}
	if firstIface == nil {
		return []Value{Null()}, nil
	}

	switch firstIface.(type) {
	case string:
		var sb strings.Builder
		for _, item := range items {
			if item.IsNull() {
				continue
			}
			s, ok := scalarInterface(item).(string)
			if ok {
				sb.WriteString(s)
			} else {
				sb.WriteString(valueToStringValue(item))
			}
		}
		return []Value{ScalarString(sb.String())}, nil
	default:
		// Numeric sum.
		var sumI int64
		var sumF float64
		isFloat := false
		for _, item := range items {
			if item.IsNull() {
				continue
			}
			if i, ok := toInt64Value(item); ok {
				sumI += i
				sumF += float64(i)
			} else if f, ok := toFloat64Value(item, 0, false); ok {
				isFloat = true
				sumF += f
			}
		}
		if isFloat {
			return []Value{ScalarFloat64(sumF)}, nil
		}
		return []Value{ScalarInt64(sumI)}, nil
	}
}

// builtinTostring converts any value to its string representation.
func builtinTostring(_ *PipeContext, input Value) ([]Value, error) {
	if input.Kind() == ScalarKind {
		if s, ok := scalarInterface(input).(string); ok {
			return []Value{ScalarString(s)}, nil
		}
	}
	return []Value{ScalarString(valueToStringValue(input))}, nil
}

// builtinTonumber converts a string to a number. Strings containing '.' are
// parsed as float64; otherwise as int64. Non-string inputs that are already
// numeric pass through; other types produce an error.
func builtinTonumber(_ *PipeContext, input Value) ([]Value, error) {
	if input.Kind() == ScalarKind {
		iface := scalarInterface(input)
		// Already numeric → pass through.
		switch iface.(type) {
		case int32, int64, uint32, uint64, float32, float64:
			return []Value{input}, nil
		case string:
			s := iface.(string)
			if strings.ContainsAny(s, ".eE") {
				f, err := strconv.ParseFloat(s, 64)
				if err != nil {
					return nil, fmt.Errorf("tonumber: %q is not a number", s)
				}
				return []Value{ScalarFloat64(f)}, nil
			}
			i, err := strconv.ParseInt(s, 10, 64)
			if err != nil {
				return nil, fmt.Errorf("tonumber: %q is not a number", s)
				}
			return []Value{ScalarInt64(i)}, nil
		case bool:
			b := iface.(bool)
			if b {
				return []Value{ScalarInt64(1)}, nil
			}
			return []Value{ScalarInt64(0)}, nil
		}
	}
	return nil, fmt.Errorf("tonumber: cannot convert %s to number", typeNameOf(input))
}

// ── Built-in registry ───────────────────────────────────────────────────

// pipeBuiltins maps function names to their implementations.
var pipeBuiltins = map[string]func(*PipeContext, Value) ([]Value, error){
	"length":   builtinLength,
	"type":     builtinType,
	"keys":     builtinKeys,
	"values":   builtinValues,
	"add":      builtinAdd,
	"tostring": builtinTostring,
	"tonumber": builtinTonumber,
	// "not" and "empty" are handled as dedicated PipeExpr types
	// (pipeNot, pipeEmpty) since they have special semantics.
}

// lookupBuiltin returns the PipeExpr for a named built-in, or nil if unknown.
func lookupBuiltin(name string) PipeExpr {
	if name == "not" {
		return pipeNot{}
	}
	if name == "empty" {
		return pipeEmpty{}
	}
	if name == "null" {
		return &pipeLiteral{val: Null()}
	}
	if fn, ok := pipeBuiltins[name]; ok {
		return &pipeBuiltin{name: name, fn: fn}
	}
	return nil
}

// ── Helpers ──────────────────────────────────────────────────────────────

// typeNameOf returns a human-readable type name for error messages.
func typeNameOf(v Value) string {
	switch v.Kind() {
	case NullKind:
		return "null"
	case ScalarKind:
		iface := scalarInterface(v)
		if iface == nil {
			return "null"
		}
		switch iface.(type) {
		case bool:
			return "boolean"
		case string:
			return "string"
		case []byte:
			return "bytes"
		default:
			return "number"
		}
	case ListKind:
		return "array"
	case MessageKind:
		return "object"
	case ObjectKind:
		return "object"
	default:
		return "unknown"
	}
}
