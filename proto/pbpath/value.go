package pbpath

import (
	"google.golang.org/protobuf/reflect/protoreflect"
)

// ValueKind identifies the category of data held by a [Value].
// A Value is always exactly one of these kinds.
type ValueKind int

const (
	// NullKind indicates the [Value] carries no data.
	// This is the zero-value kind for [Value].
	NullKind ValueKind = iota

	// ScalarKind indicates the [Value] wraps a single [protoreflect.Value].
	ScalarKind

	// ListKind indicates the [Value] holds an ordered collection of child
	// [Value] elements — the result of a collect or fan-out operation.
	ListKind

	// MessageKind indicates the [Value] wraps a live [protoreflect.Message]
	// that can be traversed further by subsequent path steps.
	MessageKind

	// ObjectKind indicates the [Value] holds a constructed object — an
	// ordered sequence of key-value pairs produced by {key: expr, ...}
	// syntax. Unlike [MessageKind], objects are schema-free and can hold
	// arbitrary string keys and [Value] values.
	ObjectKind
)

// ObjectEntry is a single key-value pair in an [ObjectKind] [Value].
type ObjectEntry struct {
	Key   string
	Value Value
}

// Value is the universal intermediate representation for pbpath expressions.
//
// It is intentionally a small struct (≤ 48 bytes on 64-bit) that can be
// passed by value without heap allocation in hot-path expression evaluation.
// The zero value is a null (kind == [NullKind]).
//
// Design rationale:
//   - scalar is stored as [protoreflect.Value] (interface{} wrapper) so
//     every proto primitive type is supported without boxing again.
//   - list is a slice header (24 bytes) stored directly in the struct.
//     For small fan-out counts, Go 1.26 stack-allocated append backing
//     stores keep these off the heap entirely.
//   - msg is a [protoreflect.Message] interface — one word + one pointer.
//   - kind occupies a single int. Keeping it first enables branch
//     prediction on the hot-path switch.
type Value struct {
	kind   ValueKind
	scalar protoreflect.Value   // valid when kind == ScalarKind
	list   []Value              // valid when kind == ListKind
	msg    protoreflect.Message // valid when kind == MessageKind
	obj    []ObjectEntry        // valid when kind == ObjectKind
}

// ── Constructors ────────────────────────────────────────────────────────

// Null returns the null [Value]. Equivalent to the zero value.
func Null() Value { return Value{} }

// Scalar wraps a single [protoreflect.Value] into a [Value] with
// kind [ScalarKind]. If pv is not valid (zero), the result is null.
func Scalar(pv protoreflect.Value) Value {
	if !pv.IsValid() {
		return Value{}
	}
	return Value{kind: ScalarKind, scalar: pv}
}

// ScalarBool creates a [ScalarKind] [Value] holding a bool.
func ScalarBool(b bool) Value {
	return Value{kind: ScalarKind, scalar: protoreflect.ValueOfBool(b)}
}

// ScalarInt64 creates a [ScalarKind] [Value] holding an int64.
func ScalarInt64(n int64) Value {
	return Value{kind: ScalarKind, scalar: protoreflect.ValueOfInt64(n)}
}

// ScalarInt32 creates a [ScalarKind] [Value] holding an int32.
func ScalarInt32(n int32) Value {
	return Value{kind: ScalarKind, scalar: protoreflect.ValueOfInt32(n)}
}

// ScalarFloat64 creates a [ScalarKind] [Value] holding a float64.
func ScalarFloat64(f float64) Value {
	return Value{kind: ScalarKind, scalar: protoreflect.ValueOfFloat64(f)}
}

// ScalarString creates a [ScalarKind] [Value] holding a string.
func ScalarString(s string) Value {
	return Value{kind: ScalarKind, scalar: protoreflect.ValueOfString(s)}
}

// ListVal creates a [ListKind] [Value] from an existing slice of [Value].
// If items is nil or empty, the result is an empty list (not null).
func ListVal(items []Value) Value {
	if items == nil {
		items = []Value{}
	}
	return Value{kind: ListKind, list: items}
}

// MessageVal wraps a [protoreflect.Message] into a [Value] with kind
// [MessageKind]. If m is nil, the result is null.
func MessageVal(m protoreflect.Message) Value {
	if m == nil {
		return Value{}
	}
	return Value{kind: MessageKind, msg: m}
}

// ObjectVal creates an [ObjectKind] [Value] from key-value pairs.
// If entries is nil, the result is an empty object (not null).
func ObjectVal(entries []ObjectEntry) Value {
	if entries == nil {
		entries = []ObjectEntry{}
	}
	return Value{kind: ObjectKind, obj: entries}
}

// ── Accessors ───────────────────────────────────────────────────────────

// Kind returns the [ValueKind] of this value.
func (v Value) Kind() ValueKind { return v.kind }

// IsNull reports whether the value is null (kind == [NullKind]).
func (v Value) IsNull() bool { return v.kind == NullKind }

// ProtoValue returns the underlying [protoreflect.Value] for a scalar.
// For non-scalar kinds, it returns the zero [protoreflect.Value].
func (v Value) ProtoValue() protoreflect.Value {
	if v.kind == ScalarKind {
		return v.scalar
	}
	return protoreflect.Value{}
}

// List returns the child elements for a [ListKind] value.
// Returns nil for non-list kinds.
func (v Value) List() []Value {
	if v.kind == ListKind {
		return v.list
	}
	return nil
}

// Message returns the [protoreflect.Message] for a [MessageKind] value.
// Returns nil for non-message kinds.
func (v Value) Message() protoreflect.Message {
	if v.kind == MessageKind {
		return v.msg
	}
	return nil
}

// Entries returns the key-value pairs for an [ObjectKind] value.
// Returns nil for non-object kinds.
func (v Value) Entries() []ObjectEntry {
	if v.kind == ObjectKind {
		return v.obj
	}
	return nil
}

// Len returns the number of elements for a [ListKind] value,
// or the number of entries for an [ObjectKind] value, or 0 otherwise.
func (v Value) Len() int {
	switch v.kind {
	case ListKind:
		return len(v.list)
	case ObjectKind:
		return len(v.obj)
	default:
		return 0
	}
}

// Index returns the i-th element of a [ListKind] value.
// Returns null for non-list kinds or out-of-bounds indices.
// Negative indices are supported (Python-style).
func (v Value) Index(i int) Value {
	if v.kind != ListKind {
		return Value{}
	}
	if i < 0 {
		i = len(v.list) + i
	}
	if i < 0 || i >= len(v.list) {
		return Value{}
	}
	return v.list[i]
}

// IsNonZero reports whether the value is non-null and not the protobuf
// zero value for its kind. For scalars this means non-zero/non-empty;
// for lists, non-empty; for messages, non-nil.
func (v Value) IsNonZero() bool { return isNonZeroValue(v) }

// Get returns the value of field fd on a [MessageKind] value.
// Returns null for non-message values.
func (v Value) Get(fd protoreflect.FieldDescriptor) Value {
	if v.kind != MessageKind || v.msg == nil {
		return Value{}
	}
	return FromProtoValue(v.msg.Get(fd))
}

// String returns a human-readable representation of the value for debugging.
func (v Value) String() string {
	switch v.kind {
	case NullKind:
		return "null"
	case ScalarKind:
		return valueToString(v.scalar)
	case ListKind:
		if len(v.list) == 0 {
			return "[]"
		}
		b := []byte{'['}
		for i, elem := range v.list {
			if i > 0 {
				b = append(b, ',')
			}
			b = append(b, jsonEncodeValue(elem)...)
		}
		b = append(b, ']')
		return string(b)
	case MessageKind:
		if v.msg == nil {
			return "null"
		}
		return "<message>"
	case ObjectKind:
		if len(v.obj) == 0 {
			return "{}"
		}
		b := []byte{'{'}
		for i, e := range v.obj {
			if i > 0 {
				b = append(b, ',')
			}
			b = append(b, '"')
			b = append(b, e.Key...)
			b = append(b, '"')
			b = append(b, ':')
			b = append(b, jsonEncodeValue(e.Value)...)
		}
		b = append(b, '}')
		return string(b)
	default:
		return "null"
	}
}

// jsonEncodeValue returns a JSON-style string representation of a value
// for use inside object String() output. Strings are double-quoted;
// other types use their normal String() representation.
func jsonEncodeValue(v Value) string {
	if v.kind == ScalarKind {
		iface := scalarInterface(v)
		if s, ok := iface.(string); ok {
			return `"` + s + `"`
		}
	}
	return v.String()
}

// ── Conversion helpers (bridging to/from protoreflect.Value) ────────────

// FromProtoValue converts a [protoreflect.Value] to a [Value].
// Messages are wrapped as [MessageKind]; Lists/Maps are wrapped as
// [ScalarKind] (preserving the proto List/Map interface for downstream
// expression functions). Invalid values become null.
func FromProtoValue(pv protoreflect.Value) Value {
	if !pv.IsValid() {
		return Value{}
	}
	// Detect messages and wrap as MessageKind for further traversal.
	if m, ok := pv.Interface().(protoreflect.Message); ok {
		return Value{kind: MessageKind, msg: m}
	}
	return Value{kind: ScalarKind, scalar: pv}
}

// ToProtoValue converts a [Value] back to a [protoreflect.Value].
// For [ScalarKind], the wrapped value is returned directly.
// For [MessageKind], the message is wrapped via [protoreflect.ValueOfMessage].
// For [ListKind] and [NullKind], the zero [protoreflect.Value] is returned.
func (v Value) ToProtoValue() protoreflect.Value {
	switch v.kind {
	case ScalarKind:
		return v.scalar
	case MessageKind:
		if v.msg != nil {
			return protoreflect.ValueOfMessage(v.msg)
		}
		return protoreflect.Value{}
	default:
		return protoreflect.Value{}
	}
}

// ── Predicate helpers ───────────────────────────────────────────────────

// isNonZeroValue reports whether v is non-null and not the protobuf zero
// value for its kind. This is the [Value]-based equivalent of [isNonZero].
func isNonZeroValue(v Value) bool {
	switch v.kind {
	case NullKind:
		return false
	case ScalarKind:
		return isNonZero(v.scalar)
	case ListKind:
		return len(v.list) > 0
	case MessageKind:
		return v.msg != nil
	case ObjectKind:
		return len(v.obj) > 0
	default:
		return false
	}
}

// ── Type-extraction helpers (operate on the scalar payload) ─────────────

// toInt64Value extracts an integer from a scalar [Value].
// Returns (0, false) for null, list, message, or non-integer scalar types.
func toInt64Value(v Value) (int64, bool) {
	if v.kind != ScalarKind {
		return 0, false
	}
	return toInt64(v.scalar)
}

// toFloat64Value extracts or promotes a scalar [Value] to float64.
// If the scalar was already extracted as int64, pass it via asInt/wasInt
// to enable int→float promotion without a second type-switch.
func toFloat64Value(v Value, asInt int64, wasInt bool) (float64, bool) {
	if v.kind != ScalarKind {
		return 0, false
	}
	return toFloat64(v.scalar, asInt, wasInt)
}

// valueToStringValue converts a [Value] to its string representation.
// Null values return "". List and message values return placeholder strings.
func valueToStringValue(v Value) string {
	switch v.kind {
	case NullKind:
		return ""
	case ScalarKind:
		return valueToString(v.scalar)
	case ListKind:
		return "<list>"
	case MessageKind:
		return "<message>"
	case ObjectKind:
		return v.String()
	default:
		return ""
	}
}

// scalarInterface returns the Go interface value of the scalar payload,
// or nil if the value is not a scalar or is invalid.
// This avoids the need to call v.ProtoValue().Interface() with its
// intermediate allocation on the proto value.
func scalarInterface(v Value) any {
	if v.kind != ScalarKind || !v.scalar.IsValid() {
		return nil
	}
	return v.scalar.Interface()
}
