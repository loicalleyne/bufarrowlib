// Package pbpath provides functionality for
// representing a sequence of protobuf reflection operations on a message,
// including parsing human-readable path strings and traversing messages
// along a path to collect values.
//
// pbpath extends the standard [protopath.Step] with additional step kinds
// for list range slicing ([start:end], [start:], [:end]) and list wildcards
// ([*], [:]) that fan out during value traversal.
package pbpath

import (
	"encoding/hex"
	"fmt"
	"strconv"
	"strings"

	"google.golang.org/protobuf/reflect/protopath"
	"google.golang.org/protobuf/reflect/protoreflect"
)

// StepKind enumerates the kinds of steps in a [Path].
// The first six values mirror [protopath.StepKind].
type StepKind int

const (
	// RootStep identifies a [Step] as a root message.
	RootStep StepKind = iota
	// FieldAccessStep identifies a [Step] as accessing a message field by name.
	FieldAccessStep
	// UnknownAccessStep identifies a [Step] as accessing unknown fields.
	UnknownAccessStep
	// ListIndexStep identifies a [Step] as indexing into a list by position.
	// Negative indices are allowed and resolved at traversal time.
	ListIndexStep
	// MapIndexStep identifies a [Step] as indexing into a map by key.
	MapIndexStep
	// AnyExpandStep identifies a [Step] as expanding an Any message.
	AnyExpandStep
	// ListRangeStep identifies a [Step] as a Python-style slice
	// [start:end:step] applied to a repeated field during value traversal.
	// Any of start, end, step may be omitted; omitted bounds are resolved
	// at traversal time based on the step sign (see Python slice semantics).
	ListRangeStep
	// ListWildcardStep identifies a [Step] as selecting all elements of a
	// repeated field. Equivalent to [*], [:], or [::] in the path syntax.
	ListWildcardStep
)

// Step is a single operation in a [Path]. It wraps a [protopath.Step] for the
// standard step kinds and adds [ListRangeStep] and [ListWildcardStep].
type Step struct {
	kind StepKind
	// For standard step kinds (Root..AnyExpand) the wrapped protopath.Step.
	// For ListIndexStep with a negative index, proto is zero-valued and
	// listIndex stores the raw value.
	proto protopath.Step
	// listIndex stores the raw index for ListIndexStep, including negative values.
	listIndex int
	// For ListRangeStep — Python-style [start:end:step].
	rangeStart   int
	rangeEnd     int
	rangeStep    int  // stride; 0 is invalid and rejected at parse/construct time
	startOmitted bool // true when start was not explicitly provided
	endOmitted   bool // true when end was not explicitly provided
}

// Kind returns the step's kind.
func (s Step) Kind() StepKind { return s.kind }

// ProtoStep returns the underlying [protopath.Step].
// It panics if the step kind is [ListRangeStep] or [ListWildcardStep].
func (s Step) ProtoStep() protopath.Step {
	if s.kind == ListRangeStep || s.kind == ListWildcardStep {
		panic(fmt.Sprintf("pbpath.Step.ProtoStep: not a standard step kind (%d)", s.kind))
	}
	return s.proto
}

// MessageDescriptor returns the message descriptor for [RootStep] and [AnyExpandStep].
func (s Step) MessageDescriptor() protoreflect.MessageDescriptor {
	return s.proto.MessageDescriptor()
}

// FieldDescriptor returns the field descriptor for a [FieldAccessStep].
func (s Step) FieldDescriptor() protoreflect.FieldDescriptor {
	return s.proto.FieldDescriptor()
}

// ListIndex returns the list index for a [ListIndexStep].
// May be negative (resolved at traversal time).
func (s Step) ListIndex() int { return s.listIndex }

// MapIndex returns the map key for a [MapIndexStep].
func (s Step) MapIndex() protoreflect.MapKey { return s.proto.MapIndex() }

// RangeStart returns the start bound of a [ListRangeStep].
// The value is 0 when [Step.StartOmitted] is true.
func (s Step) RangeStart() int { return s.rangeStart }

// RangeEnd returns the end bound of a [ListRangeStep].
// The value is 0 when [Step.EndOmitted] is true.
func (s Step) RangeEnd() int { return s.rangeEnd }

// RangeStep returns the stride of a [ListRangeStep].
// Defaults to 1 when not explicitly provided. Never 0.
func (s Step) RangeStep() int { return s.rangeStep }

// StartOmitted reports whether the start bound was omitted in the source syntax.
// When true, the effective start is computed at traversal time based on the
// step sign: 0 for positive step, len-1 for negative step.
func (s Step) StartOmitted() bool { return s.startOmitted }

// EndOmitted reports whether the end bound was omitted in the source syntax.
// When true, the effective end is computed at traversal time based on the
// step sign: len for positive step, -(len+1) for negative step.
func (s Step) EndOmitted() bool { return s.endOmitted }

// RangeOpen reports whether the range has no explicit end bound.
// Deprecated: use [Step.EndOmitted] instead.
func (s Step) RangeOpen() bool { return s.endOmitted }

// NOTE: The Path and Values are separate types here since there are use cases
// where you would like to "address" some value in a message with just the path
// and don't have the value information available.
//
// This is different from how github.com/google/go-cmp/cmp.Path operates,
// which combines both path and value information together.
// Since the cmp package itself is the only one ever constructing a cmp.Path,
// it will always have the value available.

// Path is a sequence of protobuf reflection steps applied to some root
// protobuf message value to arrive at the current value.
// The first step must be a [Root] step.
type Path []Step

// Index returns the ith step in the path and supports negative indexing.
// A negative index starts counting from the tail of the Path such that -1
// refers to the last step, -2 refers to the second-to-last step, and so on.
// It returns a zero Step value if the index is out-of-bounds.
func (p Path) Index(i int) Step {
	if i < 0 {
		i = len(p) + i
	}
	if i < 0 || i >= len(p) {
		return Step{}
	}
	return p[i]
}

// String returns a structured representation of the path
// by concatenating the string representation of every path step.
func (p Path) String() string {
	var b []byte
	for _, s := range p {
		b = stepAppendString(s, b)
	}
	return string(b)
}

// Values is a Path paired with a sequence of values at each step.
// The lengths of [Values.Path] and [Values.Values] must be identical.
// The first step must be a [Root] step and
// the first value must be a concrete message value.
type Values struct {
	Path   Path
	Values []protoreflect.Value
	// clamped is set to true when any step along this branch was clamped
	// (e.g. a range end exceeded the list length). Used by [Plan.Eval]
	// to defer strict checking to leaf nodes.
	clamped bool
}

// Len reports the length of the path and values.
// If the path and values have differing length, it returns the minimum length.
func (p Values) Len() int {
	n := len(p.Path)
	if n > len(p.Values) {
		n = len(p.Values)
	}
	return n
}

// Index returns the ith step and value and supports negative indexing.
// A negative index starts counting from the tail of the Values such that -1
// refers to the last pair, -2 refers to the second-to-last pair, and so on.
func (p Values) Index(i int) (out struct {
	Step  Step
	Value protoreflect.Value
},
) {
	// NOTE: This returns a single struct instead of two return values so that
	// callers can make use of the value in an expression:
	//	vs.Index(i).Value.Interface()
	n := p.Len()
	if i < 0 {
		i = n + i
	}
	if i < 0 || i >= n {
		return out
	}
	out.Step = p.Path[i]
	out.Value = p.Values[i]
	return out
}

// String returns a humanly readable representation of the path and last value.
// Do not depend on the output being stable.
//
// For example:
//
//	(path.to.MyMessage).list_field[5].map_field["hello"] = {hello: "world"}
func (p Values) String() string {
	n := p.Len()
	if n == 0 {
		return ""
	}
	// Determine the field descriptor associated with the last step.
	var fd protoreflect.FieldDescriptor
	last := p.Index(-1)
	switch last.Step.Kind() {
	case FieldAccessStep:
		fd = last.Step.FieldDescriptor()
	case MapIndexStep, ListIndexStep:
		fd = p.Index(-2).Step.FieldDescriptor()
	}
	// Format the full path with the last value.
	return fmt.Sprintf("%v = %v", p.Path[:n], FormatValue(last.Value, fd))
}

// ListIndices returns the concrete list indices visited along this Values path.
// It collects the index from every [ListIndexStep] in order.
func (p Values) ListIndices() []int {
	var out []int
	for _, s := range p.Path {
		if s.Kind() == ListIndexStep {
			out = append(out, s.ListIndex())
		}
	}
	return out
}

// FormatValue returns a human-readable string representation of a protoreflect.Value.
func FormatValue(v protoreflect.Value, fd protoreflect.FieldDescriptor) string {
	return string(appendValue(nil, v, fd))
}

// appendValue appends a human-readable representation of v to b.
func appendValue(b []byte, v protoreflect.Value, fd protoreflect.FieldDescriptor) []byte {
	if !v.IsValid() {
		return append(b, "<nil>"...)
	}
	switch val := v.Interface().(type) {
	case protoreflect.Message:
		b = append(b, '{')
		fields := val.Descriptor().Fields()
		first := true
		for i := 0; i < fields.Len(); i++ {
			f := fields.Get(i)
			if !val.Has(f) {
				continue
			}
			if !first {
				b = append(b, ", "...)
			}
			first = false
			b = append(b, f.TextName()...)
			b = append(b, ": "...)
			b = appendValue(b, val.Get(f), f)
		}
		b = append(b, '}')
	case protoreflect.List:
		b = append(b, '[')
		for i := 0; i < val.Len(); i++ {
			if i > 0 {
				b = append(b, ", "...)
			}
			b = appendValue(b, val.Get(i), fd)
		}
		b = append(b, ']')
	case protoreflect.Map:
		b = append(b, '{')
		first := true
		val.Range(func(k protoreflect.MapKey, v protoreflect.Value) bool {
			if !first {
				b = append(b, ", "...)
			}
			first = false
			b = appendValue(b, k.Value(), nil)
			b = append(b, ": "...)
			var valFD protoreflect.FieldDescriptor
			if fd != nil && fd.IsMap() {
				valFD = fd.MapValue()
			}
			b = appendValue(b, v, valFD)
			return true
		})
		b = append(b, '}')
	case protoreflect.EnumNumber:
		if fd != nil && fd.Enum() != nil {
			if evd := fd.Enum().Values().ByNumber(val); evd != nil {
				return append(b, evd.Name()...)
			}
		}
		b = strconv.AppendInt(b, int64(val), 10)
	case bool:
		b = strconv.AppendBool(b, val)
	case int32:
		b = strconv.AppendInt(b, int64(val), 10)
	case int64:
		b = strconv.AppendInt(b, val, 10)
	case uint32:
		b = strconv.AppendUint(b, uint64(val), 10)
	case uint64:
		b = strconv.AppendUint(b, val, 10)
	case float32:
		b = strconv.AppendFloat(b, float64(val), 'g', -1, 32)
	case float64:
		b = strconv.AppendFloat(b, val, 'g', -1, 64)
	case string:
		b = strconv.AppendQuote(b, val)
	case []byte:
		b = append(b, '"')
		b = append(b, hex.EncodeToString(val)...)
		b = append(b, '"')
	default:
		b = append(b, fmt.Sprintf("%v", val)...)
	}
	return b
}

// stepAppendString appends the string representation of a [Step] to b.
func stepAppendString(s Step, b []byte) []byte {
	switch s.Kind() {
	case ListRangeStep:
		b = append(b, '[')
		if !s.startOmitted {
			b = strconv.AppendInt(b, int64(s.rangeStart), 10)
		}
		b = append(b, ':')
		if !s.endOmitted {
			b = strconv.AppendInt(b, int64(s.rangeEnd), 10)
		}
		if s.rangeStep != 1 {
			b = append(b, ':')
			b = strconv.AppendInt(b, int64(s.rangeStep), 10)
		}
		b = append(b, ']')
		return b
	case ListWildcardStep:
		return append(b, "[*]"...)
	case ListIndexStep:
		b = append(b, '[')
		b = strconv.AppendInt(b, int64(s.listIndex), 10)
		b = append(b, ']')
		return b
	}
	// Standard protopath step kinds with a valid underlying proto step.
	ps := s.proto
	switch ps.Kind() {
	case protopath.RootStep:
		b = append(b, '(')
		b = append(b, ps.MessageDescriptor().FullName()...)
		b = append(b, ')')
	case protopath.FieldAccessStep:
		b = append(b, '.')
		fd := ps.FieldDescriptor()
		if fd.IsExtension() {
			b = append(b, '(')
			b = append(b, strings.Trim(fd.TextName(), "[]")...)
			b = append(b, ')')
		} else {
			b = append(b, fd.TextName()...)
		}
	case protopath.UnknownAccessStep:
		b = append(b, '.')
		b = append(b, '?')
	case protopath.MapIndexStep:
		b = append(b, '[')
		switch k := ps.MapIndex().Interface().(type) {
		case bool:
			b = strconv.AppendBool(b, k)
		case int32:
			b = strconv.AppendInt(b, int64(k), 10)
		case int64:
			b = strconv.AppendInt(b, int64(k), 10)
		case uint32:
			b = strconv.AppendUint(b, uint64(k), 10)
		case uint64:
			b = strconv.AppendUint(b, uint64(k), 10)
		case string:
			b = strconv.AppendQuote(b, k)
		}
		b = append(b, ']')
	case protopath.AnyExpandStep:
		b = append(b, '.')
		b = append(b, '(')
		b = append(b, ps.MessageDescriptor().FullName()...)
		b = append(b, ')')
	default:
		b = append(b, "<invalid>"...)
	}
	return b
}

// ---- Convenience constructors ----

// wrapStep wraps a [protopath.Step] in a [Step] with the corresponding kind.
func wrapStep(ps protopath.Step) Step {
	var k StepKind
	switch ps.Kind() {
	case protopath.RootStep:
		k = RootStep
	case protopath.FieldAccessStep:
		k = FieldAccessStep
	case protopath.UnknownAccessStep:
		k = UnknownAccessStep
	case protopath.ListIndexStep:
		k = ListIndexStep
		return Step{kind: k, proto: ps, listIndex: ps.ListIndex()}
	case protopath.MapIndexStep:
		k = MapIndexStep
	case protopath.AnyExpandStep:
		k = AnyExpandStep
	}
	return Step{kind: k, proto: ps}
}

// Root returns a [RootStep] for the given message descriptor.
func Root(md protoreflect.MessageDescriptor) Step {
	return wrapStep(protopath.Root(md))
}

// FieldAccess returns a [FieldAccessStep] for the given field descriptor.
func FieldAccess(fd protoreflect.FieldDescriptor) Step {
	return wrapStep(protopath.FieldAccess(fd))
}

// ListIndex returns a [ListIndexStep] for the given index.
// Negative indices are allowed and resolved at traversal time.
// For non-negative indices, the underlying [protopath.ListIndex] is used.
// For negative indices, only the raw value is stored (since protopath panics on negatives).
func ListIndex(i int) Step {
	if i >= 0 {
		return wrapStep(protopath.ListIndex(i))
	}
	return Step{kind: ListIndexStep, listIndex: i}
}

// MapIndex returns a [MapIndexStep] for the given map key.
func MapIndex(k protoreflect.MapKey) Step {
	return wrapStep(protopath.MapIndex(k))
}

// AnyExpand returns an [AnyExpandStep] for the given message descriptor.
func AnyExpand(md protoreflect.MessageDescriptor) Step {
	return wrapStep(protopath.AnyExpand(md))
}

// ListRange returns a [ListRangeStep] representing the half-open range [start, end)
// with stride 1. Both start and end may be negative (resolved at traversal time).
func ListRange(start, end int) Step {
	return Step{kind: ListRangeStep, rangeStart: start, rangeEnd: end, rangeStep: 1}
}

// ListRangeFrom returns a [ListRangeStep] with only a start bound (open end)
// and stride 1. This represents [start:] syntax.
func ListRangeFrom(start int) Step {
	return Step{kind: ListRangeStep, rangeStart: start, rangeStep: 1, endOmitted: true}
}

// ListRangeStep3 returns a [ListRangeStep] with explicit start, end, and step.
// Panics if step is 0.
//
// Use startOmitted/endOmitted to indicate that the respective bound should be
// defaulted at traversal time based on the step sign (Python semantics).
func ListRangeStep3(start, end, step int, startOmitted, endOmitted bool) Step {
	if step == 0 {
		panic("pbpath.ListRangeStep3: step must not be zero")
	}
	return Step{
		kind:         ListRangeStep,
		rangeStart:   start,
		rangeEnd:     end,
		rangeStep:    step,
		startOmitted: startOmitted,
		endOmitted:   endOmitted,
	}
}

// ListWildcard returns a [ListWildcardStep] that selects every element.
func ListWildcard() Step {
	return Step{kind: ListWildcardStep}
}
