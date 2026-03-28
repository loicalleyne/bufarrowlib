package pbpath

import (
	"testing"

	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/types/dynamicpb"
)

// ── Constructor: Scalar ─────────────────────────────────────────────────

func TestScalarInvalid(t *testing.T) {
	// Zero proto value → null
	v := Scalar(protoreflect.Value{})
	if !v.IsNull() {
		t.Fatal("Scalar(zero) should be null")
	}
}

func TestScalarValid(t *testing.T) {
	v := Scalar(protoreflect.ValueOfInt64(42))
	if v.Kind() != ScalarKind {
		t.Fatalf("Kind() = %v; want ScalarKind", v.Kind())
	}
	if v.ProtoValue().Int() != 42 {
		t.Fatalf("ProtoValue().Int() = %d; want 42", v.ProtoValue().Int())
	}
}

// ── Constructor: MessageVal with nil ────────────────────────────────────

func TestMessageValNil(t *testing.T) {
	v := MessageVal(nil)
	if !v.IsNull() {
		t.Fatal("MessageVal(nil) should be null")
	}
}

// ── Accessors on wrong kind ─────────────────────────────────────────────

func TestValueAccessorsWrongKind(t *testing.T) {
	s := ScalarString("test")
	if s.List() != nil {
		t.Fatal("List() on scalar should be nil")
	}
	if s.Message() != nil {
		t.Fatal("Message() on scalar should be nil")
	}
	if s.Entries() != nil {
		t.Fatal("Entries() on scalar should be nil")
	}

	l := ListVal(nil)
	if l.ProtoValue().IsValid() {
		t.Fatal("ProtoValue() on list should be invalid")
	}
	if l.Message() != nil {
		t.Fatal("Message() on list should be nil")
	}

	m := MessageVal(nil) // null
	if m.List() != nil {
		t.Fatal("List() on null should be nil")
	}
}

// ── Len on different kinds ──────────────────────────────────────────────

func TestValueLen(t *testing.T) {
	// Null → 0
	if Null().Len() != 0 {
		t.Fatal("Null.Len() should be 0")
	}
	// Scalar → 0
	if ScalarInt64(1).Len() != 0 {
		t.Fatal("Scalar.Len() should be 0")
	}
	// List → count
	l := ListVal([]Value{ScalarInt64(1), ScalarInt64(2)})
	if l.Len() != 2 {
		t.Fatalf("List.Len() = %d; want 2", l.Len())
	}
	// Object → count
	o := ObjectVal([]ObjectEntry{{Key: "a", Value: ScalarInt64(1)}})
	if o.Len() != 1 {
		t.Fatalf("Object.Len() = %d; want 1", o.Len())
	}
}

// ── Index ───────────────────────────────────────────────────────────────

func TestValueIndex(t *testing.T) {
	l := ListVal([]Value{ScalarString("a"), ScalarString("b"), ScalarString("c")})

	// Normal index
	if l.Index(0).String() != "a" {
		t.Fatal("Index(0) should be a")
	}

	// Negative index
	if l.Index(-1).String() != "c" {
		t.Fatal("Index(-1) should be c")
	}

	// Out of bounds
	if !l.Index(10).IsNull() {
		t.Fatal("Index(10) should be null")
	}
	if !l.Index(-10).IsNull() {
		t.Fatal("Index(-10) should be null")
	}

	// Non-list
	if !ScalarString("x").Index(0).IsNull() {
		t.Fatal("Index on scalar should be null")
	}
}

// ── IsNonZero ───────────────────────────────────────────────────────────

func TestValueIsNonZero(t *testing.T) {
	tcs := []struct {
		name string
		val  Value
		want bool
	}{
		{"null", Null(), false},
		{"zero int", ScalarInt64(0), false},
		{"non-zero int", ScalarInt64(42), true},
		{"empty string", ScalarString(""), false},
		{"non-empty string", ScalarString("x"), true},
		{"false bool", ScalarBool(false), false},
		{"true bool", ScalarBool(true), true},
		{"empty list", ListVal(nil), false},
		{"non-empty list", ListVal([]Value{ScalarInt64(1)}), true},
		{"empty object", ObjectVal(nil), false},
		{"non-empty object", ObjectVal([]ObjectEntry{{Key: "a", Value: ScalarInt64(1)}}), true},
	}
	for _, tc := range tcs {
		t.Run(tc.name, func(t *testing.T) {
			if got := tc.val.IsNonZero(); got != tc.want {
				t.Fatalf("IsNonZero() = %v; want %v", got, tc.want)
			}
		})
	}
}

// ── Get on MessageKind ──────────────────────────────────────────────────

func TestValueGetField(t *testing.T) {
	_, nestedMD := buildTestDescriptors(t)

	nested := newNested(nestedMD, "hello")
	v := MessageVal(nested)

	fd := nestedMD.Fields().ByName("stringfield")
	got := v.Get(fd)
	if got.Kind() != ScalarKind {
		t.Fatalf("Get(stringfield).Kind() = %v; want ScalarKind", got.Kind())
	}
	if got.String() != "hello" {
		t.Fatalf("Get(stringfield) = %q; want %q", got.String(), "hello")
	}

	// Get on null/non-message returns null
	null := Null()
	if !null.Get(fd).IsNull() {
		t.Fatal("Get on null should return null")
	}

	scalar := ScalarString("x")
	if !scalar.Get(fd).IsNull() {
		t.Fatal("Get on scalar should return null")
	}
}

// ── String representation ───────────────────────────────────────────────

func TestValueStringRepr(t *testing.T) {
	tcs := []struct {
		name string
		val  Value
		want string
	}{
		{"null", Null(), "null"},
		{"int", ScalarInt64(42), "42"},
		{"string", ScalarString("hello"), "hello"},
		{"empty list", ListVal(nil), "[]"},
		{"list", ListVal([]Value{ScalarInt64(1), ScalarString("a")}), `[1,"a"]`},
		{"empty object", ObjectVal(nil), "{}"},
		{"object", ObjectVal([]ObjectEntry{
			{Key: "k", Value: ScalarInt64(1)},
		}), `{"k":1}`},
		{"nil message", MessageVal(nil), "null"}, // MessageVal(nil) → Null
	}
	for _, tc := range tcs {
		t.Run(tc.name, func(t *testing.T) {
			if got := tc.val.String(); got != tc.want {
				t.Fatalf("String() = %q; want %q", got, tc.want)
			}
		})
	}

	// Non-nil message
	_, nestedMD := buildTestDescriptors(t)
	msgVal := MessageVal(newNested(nestedMD, "test"))
	if got := msgVal.String(); got != "<message>" {
		t.Fatalf("message String() = %q; want %q", got, "<message>")
	}
}

// ── FromProtoValue ──────────────────────────────────────────────────────

func TestFromProtoValue(t *testing.T) {
	// Invalid → null
	v := FromProtoValue(protoreflect.Value{})
	if !v.IsNull() {
		t.Fatal("FromProtoValue(invalid) should be null")
	}

	// Message → MessageKind
	_, nestedMD := buildTestDescriptors(t)
	nested := newNested(nestedMD, "test")
	v = FromProtoValue(protoreflect.ValueOfMessage(nested))
	if v.Kind() != MessageKind {
		t.Fatalf("Kind() = %v; want MessageKind", v.Kind())
	}

	// Scalar → ScalarKind
	v = FromProtoValue(protoreflect.ValueOfString("hello"))
	if v.Kind() != ScalarKind {
		t.Fatalf("Kind() = %v; want ScalarKind", v.Kind())
	}
}

// ── ToProtoValue ────────────────────────────────────────────────────────

func TestToProtoValue(t *testing.T) {
	// Scalar
	v := ScalarInt64(42)
	pv := v.ToProtoValue()
	if pv.Int() != 42 {
		t.Fatalf("ToProtoValue().Int() = %d; want 42", pv.Int())
	}

	// MessageKind with non-nil msg
	_, nestedMD := buildTestDescriptors(t)
	nested := newNested(nestedMD, "test")
	v = MessageVal(nested)
	pv = v.ToProtoValue()
	if !pv.IsValid() {
		t.Fatal("ToProtoValue() for message should be valid")
	}

	// MessageKind wrapping nil msg won't happen since MessageVal(nil) → NullKind
	// but construct it directly for coverage
	v = Value{kind: MessageKind, msg: nil}
	pv = v.ToProtoValue()
	if pv.IsValid() {
		t.Fatal("ToProtoValue() for nil message should be invalid")
	}

	// NullKind → zero value
	v = Null()
	pv = v.ToProtoValue()
	if pv.IsValid() {
		t.Fatal("ToProtoValue() for null should be invalid")
	}

	// ListKind → zero value
	v = ListVal(nil)
	pv = v.ToProtoValue()
	if pv.IsValid() {
		t.Fatal("ToProtoValue() for list should be invalid")
	}
}

// ── valueToStringValue ──────────────────────────────────────────────────

func TestValueToStringValue(t *testing.T) {
	tcs := []struct {
		name string
		val  Value
		want string
	}{
		{"null", Null(), ""},
		{"string", ScalarString("hello"), "hello"},
		{"int", ScalarInt64(42), "42"},
		{"list", ListVal(nil), "<list>"},
		{"message", Value{kind: MessageKind, msg: dynamicpb.NewMessage(nil)}, "<message>"},
		{"object", ObjectVal([]ObjectEntry{{Key: "k", Value: ScalarInt64(1)}}), `{"k":1}`},
	}
	for _, tc := range tcs {
		t.Run(tc.name, func(t *testing.T) {
			if got := valueToStringValue(tc.val); got != tc.want {
				t.Fatalf("valueToStringValue() = %q; want %q", got, tc.want)
			}
		})
	}
}

// ── scalarInterface ─────────────────────────────────────────────────────

func TestScalarInterface(t *testing.T) {
	// Non-scalar returns nil
	if scalarInterface(Null()) != nil {
		t.Fatal("scalarInterface(null) should be nil")
	}
	if scalarInterface(ListVal(nil)) != nil {
		t.Fatal("scalarInterface(list) should be nil")
	}

	// Valid scalar
	v := ScalarString("test")
	if scalarInterface(v).(string) != "test" {
		t.Fatal("scalarInterface mismatch")
	}
}

// ── isNonZeroValue comprehensive ────────────────────────────────────────

func TestIsNonZeroValue(t *testing.T) {
	// Default kind (unknown, covered by default case)
	v := Value{kind: ValueKind(99)} // invalid kind
	if isNonZeroValue(v) {
		t.Fatal("unknown kind should not be non-zero")
	}
}

// ── toInt64Value / toFloat64Value ───────────────────────────────────────

func TestToInt64ValueNonScalar(t *testing.T) {
	_, ok := toInt64Value(ListVal(nil))
	if ok {
		t.Fatal("toInt64Value on list should return false")
	}
}

func TestToFloat64ValueNonScalar(t *testing.T) {
	_, ok := toFloat64Value(ListVal(nil), 0, false)
	if ok {
		t.Fatal("toFloat64Value on list should return false")
	}
}
