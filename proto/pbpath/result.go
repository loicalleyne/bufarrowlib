package pbpath

import (
	"google.golang.org/protobuf/reflect/protoreflect"
)

// Result wraps a slice of [Value] representing the fan-out output of a
// single path entry. It provides typed accessor methods for convenient
// consumption of query results.
//
// The zero value is an empty result (no values).
//
// Accessor naming convention:
//   - Plural methods (Float64s, Strings, …) return a slice of all branches.
//   - Singular methods (Float64, String, …) return the first branch value
//     or the Go zero value when the result is empty.
type Result struct {
	vals []Value
}

// NewResult creates a [Result] from a slice of [Value].
// The slice is referenced, not copied.
func NewResult(vals []Value) Result {
	return Result{vals: vals}
}

// newResultFromProto creates a [Result] from a legacy []protoreflect.Value
// slice, converting each element via [Scalar]. This bridges the existing
// EvalLeaves output to the new typed accessor API.
func newResultFromProto(pvs []protoreflect.Value) Result {
	if len(pvs) == 0 {
		return Result{}
	}
	vals := make([]Value, len(pvs))
	for i, pv := range pvs {
		vals[i] = Scalar(pv)
	}
	return Result{vals: vals}
}

// ── Metadata ────────────────────────────────────────────────────────────

// Len returns the number of fan-out branches in this result.
func (r Result) Len() int { return len(r.vals) }

// IsEmpty reports whether the result contains no values.
func (r Result) IsEmpty() bool { return len(r.vals) == 0 }

// Values returns the raw [Value] slice.
func (r Result) Values() []Value { return r.vals }

// ── Singular accessors (first branch) ───────────────────────────────────

// Value returns the first [Value] in the result, or null if empty.
func (r Result) Value() Value {
	if len(r.vals) == 0 {
		return Null()
	}
	return r.vals[0]
}

// Float64 returns the first branch as float64.
// Returns 0 if the result is empty or the value is not numeric.
func (r Result) Float64() float64 {
	if len(r.vals) == 0 {
		return 0
	}
	v := r.vals[0]
	if i, ok := toInt64Value(v); ok {
		return float64(i)
	}
	if f, ok := toFloat64Value(v, 0, false); ok {
		return f
	}
	return 0
}

// Int64 returns the first branch as int64.
// Returns 0 if the result is empty or the value is not an integer type.
func (r Result) Int64() int64 {
	if len(r.vals) == 0 {
		return 0
	}
	i, _ := toInt64Value(r.vals[0])
	return i
}

// Uint64 returns the first branch as uint64.
// Returns 0 if the result is empty or the value is not an unsigned integer type.
func (r Result) Uint64() uint64 {
	if len(r.vals) == 0 {
		return 0
	}
	v := r.vals[0]
	if v.kind != ScalarKind {
		return 0
	}
	switch iface := v.scalar.Interface().(type) {
	case uint32:
		return uint64(iface)
	case uint64:
		return iface
	default:
		// Fall back to int64 extraction for signed types.
		i, ok := toInt64(v.scalar)
		if ok && i >= 0 {
			return uint64(i)
		}
		return 0
	}
}

// String returns the first branch as string.
// Returns "" if the result is empty. Non-string values are converted via
// [valueToStringValue].
func (r Result) String() string {
	if len(r.vals) == 0 {
		return ""
	}
	return valueToStringValue(r.vals[0])
}

// Bool returns the first branch as bool.
// Returns false if the result is empty or the value is not boolean.
func (r Result) Bool() bool {
	if len(r.vals) == 0 {
		return false
	}
	v := r.vals[0]
	if v.kind != ScalarKind {
		return false
	}
	b, ok := v.scalar.Interface().(bool)
	return ok && b
}

// Bytes returns the first branch as []byte.
// Returns nil if the result is empty or the value is not bytes.
func (r Result) Bytes() []byte {
	if len(r.vals) == 0 {
		return nil
	}
	v := r.vals[0]
	if v.kind != ScalarKind {
		return nil
	}
	bs, ok := v.scalar.Interface().([]byte)
	if !ok {
		return nil
	}
	return bs
}

// Message returns the first branch as [protoreflect.Message].
// Returns nil if the result is empty or the value is not a message.
func (r Result) Message() protoreflect.Message {
	if len(r.vals) == 0 {
		return nil
	}
	v := r.vals[0]
	if v.kind == MessageKind {
		return v.msg
	}
	// Scalar wrapping a message (from proto traversal).
	if v.kind == ScalarKind {
		if m, ok := v.scalar.Interface().(protoreflect.Message); ok {
			return m
		}
	}
	return nil
}

// ── Plural accessors (all branches) ─────────────────────────────────────

// Float64s returns all branches as float64 values.
// Non-numeric branches are silently skipped. The returned slice has
// length ≤ [Result.Len].
func (r Result) Float64s() []float64 {
	if len(r.vals) == 0 {
		return nil
	}
	out := make([]float64, 0, len(r.vals))
	for _, v := range r.vals {
		if i, ok := toInt64Value(v); ok {
			out = append(out, float64(i))
			continue
		}
		if f, ok := toFloat64Value(v, 0, false); ok {
			out = append(out, f)
			continue
		}
		// Skip non-numeric values.
	}
	return out
}

// Int64s returns all branches as int64 values.
// Non-integer branches are silently skipped.
func (r Result) Int64s() []int64 {
	if len(r.vals) == 0 {
		return nil
	}
	out := make([]int64, 0, len(r.vals))
	for _, v := range r.vals {
		if i, ok := toInt64Value(v); ok {
			out = append(out, i)
		}
	}
	return out
}

// Uint64s returns all branches as uint64 values.
// Non-unsigned-integer branches are silently skipped.
func (r Result) Uint64s() []uint64 {
	if len(r.vals) == 0 {
		return nil
	}
	out := make([]uint64, 0, len(r.vals))
	for _, v := range r.vals {
		if v.kind != ScalarKind {
			continue
		}
		switch iface := v.scalar.Interface().(type) {
		case uint32:
			out = append(out, uint64(iface))
		case uint64:
			out = append(out, iface)
		default:
			i, ok := toInt64(v.scalar)
			if ok && i >= 0 {
				out = append(out, uint64(i))
			}
		}
	}
	return out
}

// Strings returns all branches as string values.
// Non-string values are converted via [valueToStringValue].
func (r Result) Strings() []string {
	if len(r.vals) == 0 {
		return nil
	}
	out := make([]string, len(r.vals))
	for i, v := range r.vals {
		out[i] = valueToStringValue(v)
	}
	return out
}

// Bools returns all branches as bool values.
// Non-boolean branches are silently skipped.
func (r Result) Bools() []bool {
	if len(r.vals) == 0 {
		return nil
	}
	out := make([]bool, 0, len(r.vals))
	for _, v := range r.vals {
		if v.kind != ScalarKind {
			continue
		}
		if b, ok := v.scalar.Interface().(bool); ok {
			out = append(out, b)
		}
	}
	return out
}

// BytesSlice returns all branches as []byte values.
// Non-bytes branches are silently skipped.
func (r Result) BytesSlice() [][]byte {
	if len(r.vals) == 0 {
		return nil
	}
	out := make([][]byte, 0, len(r.vals))
	for _, v := range r.vals {
		if v.kind != ScalarKind {
			continue
		}
		if bs, ok := v.scalar.Interface().([]byte); ok {
			out = append(out, bs)
		}
	}
	return out
}

// Messages returns all branches as [protoreflect.Message] values.
// Non-message branches are silently skipped.
func (r Result) Messages() []protoreflect.Message {
	if len(r.vals) == 0 {
		return nil
	}
	out := make([]protoreflect.Message, 0, len(r.vals))
	for _, v := range r.vals {
		switch v.kind {
		case MessageKind:
			if v.msg != nil {
				out = append(out, v.msg)
			}
		case ScalarKind:
			if m, ok := v.scalar.Interface().(protoreflect.Message); ok {
				out = append(out, m)
			}
		}
	}
	return out
}

// ProtoValues converts all branches back to [protoreflect.Value] for
// interoperability with existing code that expects the legacy representation.
// Null values become the zero [protoreflect.Value].
func (r Result) ProtoValues() []protoreflect.Value {
	if len(r.vals) == 0 {
		return nil
	}
	out := make([]protoreflect.Value, len(r.vals))
	for i, v := range r.vals {
		out[i] = v.ToProtoValue()
	}
	return out
}
