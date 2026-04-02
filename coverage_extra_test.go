package bufarrowlib

import (
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/loicalleyne/bufarrowlib/gen/go/samples"
	"google.golang.org/protobuf/reflect/protoreflect"
)

// ── getPath / dotPath ───────────────────────────────────────────────────

func TestGetPath(t *testing.T) {
	// Build a node tree from a known message.
	m := &samples.ScalarTypes{}
	msg := build(m.ProtoReflect())
	root := msg.root

	// Successful lookup of a top-level field (proto name "double").
	n, err := root.getPath([]string{"double"})
	if err != nil {
		t.Fatalf("getPath(double) error: %v", err)
	}
	if n == nil {
		t.Fatal("getPath returned nil node")
	}

	// Field not found.
	_, err = root.getPath([]string{"nonexistent_field"})
	if err != ErrPathNotFound {
		t.Fatalf("expected ErrPathNotFound, got: %v", err)
	}

	// Empty path.
	_, err = root.getPath([]string{})
	if err == nil {
		t.Fatal("expected error for empty path, got nil")
	}
}

func TestGetPathNested(t *testing.T) {
	m := &samples.Nested{}
	msg := build(m.ProtoReflect())
	root := msg.root

	// Nested path: nested_scalar should be found in children.
	n, err := root.getPath([]string{"nested_scalar"})
	if err != nil {
		t.Fatalf("getPath(nested_scalar) error: %v", err)
	}
	if n == nil {
		t.Fatal("getPath(nested_scalar) returned nil")
	}
}

func TestDotPath(t *testing.T) {
	m := &samples.ScalarTypes{}
	msg := build(m.ProtoReflect())
	root := msg.root

	// The root's dotPath should be the fully-qualified message name.
	dp := root.dotPath()
	if dp == "" {
		t.Fatal("dotPath() returned empty string")
	}

	// A child's dotPath should include the package + message + field name.
	child, err := root.getPath([]string{"double"})
	if err != nil {
		t.Fatalf("getPath: %v", err)
	}
	childDP := child.dotPath()
	// The field descriptor's FullName includes the parent message.
	if childDP == "" {
		t.Fatal("child dotPath() returned empty string")
	}
	// It should contain "double"
	if childDP != "ScalarTypes.double" {
		t.Fatalf("child dotPath() = %q; want ScalarTypes.double", childDP)
	}
}

// ── isScalarLeaf ────────────────────────────────────────────────────────

func TestIsScalarLeaf(t *testing.T) {
	// Get a primitive field descriptor.
	md := (&samples.ScalarTypes{}).ProtoReflect().Descriptor()

	// All fields on ScalarTypes should be scalar leaves.
	fields := md.Fields()
	for i := 0; i < fields.Len(); i++ {
		fd := fields.Get(i)
		if !isScalarLeaf(fd) {
			t.Errorf("isScalarLeaf(%s) = false; want true", fd.Name())
		}
	}

	// A message-typed field should NOT be a scalar leaf.
	nestedMD := (&samples.Nested{}).ProtoReflect().Descriptor()
	nestedField := nestedMD.Fields().ByName("nested")
	if nestedField != nil && isScalarLeaf(nestedField) {
		t.Error("isScalarLeaf(message field) should be false")
	}
}

// ── ExprKindToArrowType ─────────────────────────────────────────────────

func TestExprKindToArrowType(t *testing.T) {
	tcs := []struct {
		kind protoreflect.Kind
		want arrow.DataType
	}{
		{protoreflect.BoolKind, arrow.FixedWidthTypes.Boolean},
		{protoreflect.EnumKind, arrow.PrimitiveTypes.Int32},
		{protoreflect.Int32Kind, arrow.PrimitiveTypes.Int32},
		{protoreflect.Sint32Kind, arrow.PrimitiveTypes.Int32},
		{protoreflect.Sfixed32Kind, arrow.PrimitiveTypes.Int32},
		{protoreflect.Uint32Kind, arrow.PrimitiveTypes.Uint32},
		{protoreflect.Fixed32Kind, arrow.PrimitiveTypes.Uint32},
		{protoreflect.Int64Kind, arrow.PrimitiveTypes.Int64},
		{protoreflect.Sint64Kind, arrow.PrimitiveTypes.Int64},
		{protoreflect.Sfixed64Kind, arrow.PrimitiveTypes.Int64},
		{protoreflect.Uint64Kind, arrow.PrimitiveTypes.Uint64},
		{protoreflect.Fixed64Kind, arrow.PrimitiveTypes.Uint64},
		{protoreflect.FloatKind, arrow.PrimitiveTypes.Float32},
		{protoreflect.DoubleKind, arrow.PrimitiveTypes.Float64},
		{protoreflect.StringKind, arrow.BinaryTypes.String},
		{protoreflect.BytesKind, arrow.BinaryTypes.Binary},
		{protoreflect.MessageKind, nil},
		{protoreflect.GroupKind, nil},
	}
	for _, tc := range tcs {
		t.Run(tc.kind.String(), func(t *testing.T) {
			got := ExprKindToArrowType(tc.kind)
			if got != tc.want {
				t.Fatalf("ExprKindToArrowType(%v) = %v; want %v", tc.kind, got, tc.want)
			}
		})
	}
}

// ── ExprKindToAppendFunc ────────────────────────────────────────────────

func TestExprKindToAppendFunc(t *testing.T) {
	alloc := memory.DefaultAllocator

	tcs := []struct {
		kind    protoreflect.Kind
		builder func() array.Builder
		value   protoreflect.Value
	}{
		{protoreflect.BoolKind, func() array.Builder { return array.NewBooleanBuilder(alloc) }, protoreflect.ValueOfBool(true)},
		{protoreflect.EnumKind, func() array.Builder { return array.NewInt32Builder(alloc) }, protoreflect.ValueOfEnum(1)},
		{protoreflect.Int32Kind, func() array.Builder { return array.NewInt32Builder(alloc) }, protoreflect.ValueOfInt32(42)},
		{protoreflect.Uint32Kind, func() array.Builder { return array.NewUint32Builder(alloc) }, protoreflect.ValueOfUint32(42)},
		{protoreflect.Int64Kind, func() array.Builder { return array.NewInt64Builder(alloc) }, protoreflect.ValueOfInt64(42)},
		{protoreflect.Uint64Kind, func() array.Builder { return array.NewUint64Builder(alloc) }, protoreflect.ValueOfUint64(42)},
		{protoreflect.FloatKind, func() array.Builder { return array.NewFloat32Builder(alloc) }, protoreflect.ValueOfFloat32(3.14)},
		{protoreflect.DoubleKind, func() array.Builder { return array.NewFloat64Builder(alloc) }, protoreflect.ValueOfFloat64(3.14)},
		{protoreflect.StringKind, func() array.Builder { return array.NewStringBuilder(alloc) }, protoreflect.ValueOfString("hello")},
		{protoreflect.BytesKind, func() array.Builder { return array.NewBinaryBuilder(alloc, arrow.BinaryTypes.Binary) }, protoreflect.ValueOfBytes([]byte("data"))},
	}
	for _, tc := range tcs {
		t.Run(tc.kind.String(), func(t *testing.T) {
			b := tc.builder()
			defer b.Release()

			fn := ExprKindToAppendFunc(tc.kind, b)
			if fn == nil {
				t.Fatalf("ExprKindToAppendFunc(%v) = nil", tc.kind)
			}
			fn(tc.value)
			if b.Len() != 1 {
				t.Fatalf("builder.Len() = %d; want 1", b.Len())
			}
		})
	}

	// Unsupported kind returns nil.
	t.Run("unsupported", func(t *testing.T) {
		b := array.NewBooleanBuilder(alloc)
		defer b.Release()
		if ExprKindToAppendFunc(protoreflect.MessageKind, b) != nil {
			t.Fatal("ExprKindToAppendFunc(MessageKind) should be nil")
		}
	})
}

// ── leafFieldDescriptor ─────────────────────────────────────────────────

func TestLeafFieldDescriptor(t *testing.T) {
	md := (&samples.ScalarTypes{}).ProtoReflect().Descriptor()

	// FieldDescriptor → success
	fd := md.Fields().Get(0)
	got, err := leafFieldDescriptor(fd)
	if err != nil {
		t.Fatalf("leafFieldDescriptor(FieldDescriptor) error: %v", err)
	}
	if got != fd {
		t.Fatal("returned descriptor does not match input")
	}

	// MessageDescriptor → error
	_, err = leafFieldDescriptor(md)
	if err == nil {
		t.Fatal("leafFieldDescriptor(MessageDescriptor) should return error")
	}
}

// ── RecompileAsync ──────────────────────────────────────────────────────

func TestRecompileAsync(t *testing.T) {
	md := (&samples.ScalarTypes{}).ProtoReflect().Descriptor()
	ht := NewHyperType(md)

	// RecompileAsync should return a channel that gets closed.
	done := ht.RecompileAsync()
	<-done // should not hang

	// The type should still be valid after recompile.
	if ht.Type() == nil {
		t.Fatal("Type() is nil after RecompileAsync")
	}
}

func TestRecompileAsyncConcurrent(t *testing.T) {
	md := (&samples.ScalarTypes{}).ProtoReflect().Descriptor()
	ht := NewHyperType(md)

	// Fire multiple concurrent recompiles — exercises the CAS guard.
	done1 := ht.RecompileAsync()
	done2 := ht.RecompileAsync()
	<-done1
	<-done2

	if ht.Type() == nil {
		t.Fatal("Type() is nil after concurrent RecompileAsync")
	}
}

func TestRecordMessageThreshold(t *testing.T) {
	md := (&samples.ScalarTypes{}).ProtoReflect().Descriptor()
	ht := NewHyperType(md, WithAutoRecompile(10, 0.5))

	for i := int64(1); i <= 9; i++ {
		if ht.RecordMessage() {
			t.Fatalf("RecordMessage() returned true at count %d; want false", i)
		}
	}
	if !ht.RecordMessage() {
		t.Fatal("RecordMessage() returned false at threshold; want true")
	}
	if ht.SampleRate() != 0.5 {
		t.Fatalf("SampleRate() = %v; want 0.5", ht.SampleRate())
	}
}
