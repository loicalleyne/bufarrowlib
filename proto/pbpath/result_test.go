package pbpath

import (
	"testing"

	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/types/dynamicpb"
)

// ── NewResult / IsEmpty / Len / Values ──────────────────────────────────

func TestNewResultEmpty(t *testing.T) {
	r := NewResult(nil)
	if !r.IsEmpty() {
		t.Fatal("NewResult(nil) should be empty")
	}
	if r.Len() != 0 {
		t.Fatalf("Len() = %d; want 0", r.Len())
	}
	if r.Values() != nil {
		t.Fatal("Values() should be nil for empty result")
	}
}

func TestNewResultSingleValue(t *testing.T) {
	vals := []Value{ScalarString("abc")}
	r := NewResult(vals)
	if r.IsEmpty() {
		t.Fatal("should not be empty")
	}
	if r.Len() != 1 {
		t.Fatalf("Len() = %d; want 1", r.Len())
	}
	if len(r.Values()) != 1 {
		t.Fatal("Values() length mismatch")
	}
}

// ── Value() accessor ────────────────────────────────────────────────────

func TestResultValue(t *testing.T) {
	// Empty result returns null
	empty := NewResult(nil)
	v := empty.Value()
	if !v.IsNull() {
		t.Fatal("Value() of empty result should be null")
	}

	// Non-empty result returns first element
	vals := []Value{ScalarInt64(42), ScalarInt64(99)}
	r := NewResult(vals)
	if r.Value().Kind() != ScalarKind {
		t.Fatal("Value() should be scalar")
	}
}

// ── Float64 accessor ────────────────────────────────────────────────────

func TestResultFloat64(t *testing.T) {
	tcs := []struct {
		name string
		vals []Value
		want float64
	}{
		{"empty", nil, 0},
		{"int value", []Value{ScalarInt64(42)}, 42.0},
		{"float value", []Value{ScalarFloat64(3.14)}, 3.14},
		{"string value (non-numeric)", []Value{ScalarString("abc")}, 0},
		{"null value", []Value{Null()}, 0},
	}
	for _, tc := range tcs {
		t.Run(tc.name, func(t *testing.T) {
			r := NewResult(tc.vals)
			if got := r.Float64(); got != tc.want {
				t.Fatalf("Float64() = %v; want %v", got, tc.want)
			}
		})
	}
}

// ── Int64 accessor ──────────────────────────────────────────────────────

func TestResultInt64(t *testing.T) {
	tcs := []struct {
		name string
		vals []Value
		want int64
	}{
		{"empty", nil, 0},
		{"int value", []Value{ScalarInt64(42)}, 42},
		{"string value", []Value{ScalarString("abc")}, 0},
		{"null value", []Value{Null()}, 0},
	}
	for _, tc := range tcs {
		t.Run(tc.name, func(t *testing.T) {
			r := NewResult(tc.vals)
			if got := r.Int64(); got != tc.want {
				t.Fatalf("Int64() = %v; want %v", got, tc.want)
			}
		})
	}
}

// ── Uint64 accessor ─────────────────────────────────────────────────────

func TestResultUint64(t *testing.T) {
	tcs := []struct {
		name string
		vals []Value
		want uint64
	}{
		{"empty", nil, 0},
		{"uint64 value", []Value{Scalar(protoreflect.ValueOfUint64(100))}, 100},
		{"uint32 value", []Value{Scalar(protoreflect.ValueOfUint32(50))}, 50},
		{"positive int64", []Value{ScalarInt64(42)}, 42},
		{"negative int64", []Value{ScalarInt64(-1)}, 0},
		{"null value", []Value{Null()}, 0},
		{"non-scalar", []Value{ListVal(nil)}, 0},
		{"string value", []Value{ScalarString("abc")}, 0},
	}
	for _, tc := range tcs {
		t.Run(tc.name, func(t *testing.T) {
			r := NewResult(tc.vals)
			if got := r.Uint64(); got != tc.want {
				t.Fatalf("Uint64() = %v; want %v", got, tc.want)
			}
		})
	}
}

// ── String accessor ─────────────────────────────────────────────────────

func TestResultString(t *testing.T) {
	tcs := []struct {
		name string
		vals []Value
		want string
	}{
		{"empty", nil, ""},
		{"string value", []Value{ScalarString("hello")}, "hello"},
		{"int value", []Value{ScalarInt64(42)}, "42"},
	}
	for _, tc := range tcs {
		t.Run(tc.name, func(t *testing.T) {
			r := NewResult(tc.vals)
			if got := r.String(); got != tc.want {
				t.Fatalf("String() = %q; want %q", got, tc.want)
			}
		})
	}
}

// ── Bool accessor ───────────────────────────────────────────────────────

func TestResultBool(t *testing.T) {
	tcs := []struct {
		name string
		vals []Value
		want bool
	}{
		{"empty", nil, false},
		{"true", []Value{ScalarBool(true)}, true},
		{"false", []Value{ScalarBool(false)}, false},
		{"non-bool", []Value{ScalarString("abc")}, false},
		{"null", []Value{Null()}, false},
		{"non-scalar", []Value{ListVal(nil)}, false},
	}
	for _, tc := range tcs {
		t.Run(tc.name, func(t *testing.T) {
			r := NewResult(tc.vals)
			if got := r.Bool(); got != tc.want {
				t.Fatalf("Bool() = %v; want %v", got, tc.want)
			}
		})
	}
}

// ── Bytes accessor ──────────────────────────────────────────────────────

func TestResultBytes(t *testing.T) {
	data := []byte("hello bytes")

	// Empty
	r := NewResult(nil)
	if r.Bytes() != nil {
		t.Fatal("Bytes() of empty result should be nil")
	}

	// Non-scalar
	r = NewResult([]Value{ListVal(nil)})
	if r.Bytes() != nil {
		t.Fatal("Bytes() of non-scalar should be nil")
	}

	// Non-bytes scalar
	r = NewResult([]Value{ScalarString("not bytes")})
	if r.Bytes() != nil {
		t.Fatal("Bytes() of string should be nil")
	}

	// Actual bytes
	r = NewResult([]Value{Scalar(protoreflect.ValueOfBytes(data))})
	got := r.Bytes()
	if string(got) != "hello bytes" {
		t.Fatalf("Bytes() = %q; want %q", got, data)
	}
}

// ── Message accessor ────────────────────────────────────────────────────

func TestResultMessage(t *testing.T) {
	testMD, nestedMD := buildTestDescriptors(t)

	// Empty
	r := NewResult(nil)
	if r.Message() != nil {
		t.Fatal("Message() of empty result should be nil")
	}

	// Non-message scalar
	r = NewResult([]Value{ScalarString("abc")})
	if r.Message() != nil {
		t.Fatal("Message() of string scalar should be nil")
	}

	// MessageKind value
	nested := newNested(nestedMD, "inner")
	r = NewResult([]Value{MessageVal(nested)})
	msg := r.Message()
	if msg == nil {
		t.Fatal("Message() should not be nil")
	}

	// Scalar wrapping a message
	wrapped := dynamicpb.NewMessage(testMD)
	r = NewResult([]Value{Scalar(protoreflect.ValueOfMessage(wrapped))})
	if r.Message() == nil {
		t.Fatal("Message() of scalar-wrapped message should not be nil")
	}
}

// ── Plural accessors: Float64s ──────────────────────────────────────────

func TestResultFloat64s(t *testing.T) {
	// Empty
	r := NewResult(nil)
	if r.Float64s() != nil {
		t.Fatal("Float64s() of empty result should be nil")
	}

	// Mix of numeric and non-numeric
	vals := []Value{
		ScalarInt64(10),
		ScalarFloat64(2.5),
		ScalarString("skip"),
		ScalarInt64(30),
	}
	r = NewResult(vals)
	got := r.Float64s()
	want := []float64{10, 2.5, 30}
	if len(got) != len(want) {
		t.Fatalf("Float64s() len = %d; want %d", len(got), len(want))
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("Float64s()[%d] = %v; want %v", i, got[i], want[i])
		}
	}
}

// ── Plural accessors: Int64s ────────────────────────────────────────────

func TestResultInt64s(t *testing.T) {
	r := NewResult(nil)
	if r.Int64s() != nil {
		t.Fatal("Int64s() of empty should be nil")
	}

	vals := []Value{ScalarInt64(1), ScalarString("skip"), ScalarInt64(3)}
	r = NewResult(vals)
	got := r.Int64s()
	if len(got) != 2 || got[0] != 1 || got[1] != 3 {
		t.Fatalf("Int64s() = %v; want [1 3]", got)
	}
}

// ── Plural accessors: Uint64s ───────────────────────────────────────────

func TestResultUint64s(t *testing.T) {
	r := NewResult(nil)
	if r.Uint64s() != nil {
		t.Fatal("Uint64s() of empty should be nil")
	}

	vals := []Value{
		Scalar(protoreflect.ValueOfUint64(100)),
		Scalar(protoreflect.ValueOfUint32(50)),
		ScalarInt64(42),  // positive int64 should be included
		ScalarInt64(-1),  // negative should be skipped
		ListVal(nil),     // non-scalar skipped
		ScalarString("x"), // non-numeric skipped
	}
	r = NewResult(vals)
	got := r.Uint64s()
	want := []uint64{100, 50, 42}
	if len(got) != len(want) {
		t.Fatalf("Uint64s() len = %d; want %d", len(got), len(want))
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("Uint64s()[%d] = %v; want %v", i, got[i], want[i])
		}
	}
}

// ── Plural accessors: Strings ───────────────────────────────────────────

func TestResultStrings(t *testing.T) {
	r := NewResult(nil)
	if r.Strings() != nil {
		t.Fatal("Strings() of empty should be nil")
	}

	vals := []Value{ScalarString("a"), ScalarInt64(42), ScalarString("b")}
	r = NewResult(vals)
	got := r.Strings()
	if len(got) != 3 {
		t.Fatalf("Strings() len = %d; want 3", len(got))
	}
	if got[0] != "a" || got[2] != "b" {
		t.Fatalf("Strings() = %v", got)
	}
}

// ── Plural accessors: Bools ─────────────────────────────────────────────

func TestResultBools(t *testing.T) {
	r := NewResult(nil)
	if r.Bools() != nil {
		t.Fatal("Bools() of empty should be nil")
	}

	vals := []Value{ScalarBool(true), ScalarString("skip"), ScalarBool(false), ListVal(nil)}
	r = NewResult(vals)
	got := r.Bools()
	if len(got) != 2 || got[0] != true || got[1] != false {
		t.Fatalf("Bools() = %v; want [true false]", got)
	}
}

// ── Plural accessors: BytesSlice ────────────────────────────────────────

func TestResultBytesSlice(t *testing.T) {
	r := NewResult(nil)
	if r.BytesSlice() != nil {
		t.Fatal("BytesSlice() of empty should be nil")
	}

	vals := []Value{
		Scalar(protoreflect.ValueOfBytes([]byte("a"))),
		ScalarString("skip"),
		Scalar(protoreflect.ValueOfBytes([]byte("b"))),
		ListVal(nil),
	}
	r = NewResult(vals)
	got := r.BytesSlice()
	if len(got) != 2 || string(got[0]) != "a" || string(got[1]) != "b" {
		t.Fatalf("BytesSlice() = %v; want [[a] [b]]", got)
	}
}

// ── Plural accessors: Messages ──────────────────────────────────────────

func TestResultMessages(t *testing.T) {
	_, nestedMD := buildTestDescriptors(t)

	r := NewResult(nil)
	if r.Messages() != nil {
		t.Fatal("Messages() of empty should be nil")
	}

	n1 := newNested(nestedMD, "one")
	n2 := newNested(nestedMD, "two")
	vals := []Value{
		MessageVal(n1),
		ScalarString("skip"),
		MessageVal(n2),
		MessageVal(nil), // nil message → null, should be skipped
		Scalar(protoreflect.ValueOfMessage(newNested(nestedMD, "three"))), // scalar-wrapped message
	}
	r = NewResult(vals)
	got := r.Messages()
	if len(got) != 3 {
		t.Fatalf("Messages() len = %d; want 3", len(got))
	}
}

// ── ProtoValues ─────────────────────────────────────────────────────────

func TestResultProtoValues(t *testing.T) {
	r := NewResult(nil)
	if r.ProtoValues() != nil {
		t.Fatal("ProtoValues() of empty should be nil")
	}

	vals := []Value{ScalarInt64(42), ScalarString("hello"), Null()}
	r = NewResult(vals)
	got := r.ProtoValues()
	if len(got) != 3 {
		t.Fatalf("ProtoValues() len = %d; want 3", len(got))
	}
	// First should be valid int
	if got[0].Int() != 42 {
		t.Fatalf("ProtoValues()[0].Int() = %d; want 42", got[0].Int())
	}
}

// ── newResultFromProto ──────────────────────────────────────────────────

func TestNewResultFromProto(t *testing.T) {
	// Empty
	r := newResultFromProto(nil)
	if !r.IsEmpty() {
		t.Fatal("newResultFromProto(nil) should be empty")
	}

	// With values
	pvs := []protoreflect.Value{
		protoreflect.ValueOfString("hello"),
		protoreflect.ValueOfInt64(42),
	}
	r = newResultFromProto(pvs)
	if r.Len() != 2 {
		t.Fatalf("Len() = %d; want 2", r.Len())
	}
	if got := r.String(); got != "hello" {
		t.Fatalf("String() = %q; want %q", got, "hello")
	}
}
