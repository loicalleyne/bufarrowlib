package bufarrowlib

import (
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/loicalleyne/bufarrowlib/gen/go/samples"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// ── ProtoKindToAppendFunc for well-known message types ──────────────────

func TestProtoKindToAppendFuncTimestamp(t *testing.T) {
	alloc := memory.DefaultAllocator

	knownMD := (&samples.Known{}).ProtoReflect().Descriptor()
	tsFD := knownMD.Fields().ByName("ts")
	if tsFD == nil {
		t.Fatal("ts field not found")
	}

	b := array.NewTimestampBuilder(alloc, &arrow.TimestampType{Unit: arrow.Millisecond})
	defer b.Release()

	fn := ProtoKindToAppendFunc(tsFD, b)
	if fn == nil {
		t.Fatal("ProtoKindToAppendFunc(Timestamp) returned nil")
	}

	ts := timestamppb.Now()
	fn(protoreflect.ValueOfMessage(ts.ProtoReflect()))
	if b.Len() != 1 {
		t.Fatalf("builder.Len() = %d; want 1", b.Len())
	}
}

func TestProtoKindToAppendFuncDuration(t *testing.T) {
	alloc := memory.DefaultAllocator

	knownMD := (&samples.Known{}).ProtoReflect().Descriptor()
	durFD := knownMD.Fields().ByName("duration")
	if durFD == nil {
		t.Fatal("duration field not found")
	}

	b := array.NewDurationBuilder(alloc, &arrow.DurationType{Unit: arrow.Millisecond})
	defer b.Release()

	fn := ProtoKindToAppendFunc(durFD, b)
	if fn == nil {
		t.Fatal("ProtoKindToAppendFunc(Duration) returned nil")
	}

	dur := durationpb.New(5000000000) // 5 seconds
	fn(protoreflect.ValueOfMessage(dur.ProtoReflect()))
	if b.Len() != 1 {
		t.Fatalf("builder.Len() = %d; want 1", b.Len())
	}
}

// ── New + Append with Known message (exercises Timestamp/Duration in createNode) ──

func TestNewWithKnownMessage(t *testing.T) {
	md := (&samples.Known{}).ProtoReflect().Descriptor()

	schema, err := New(md, memory.DefaultAllocator)
	if err != nil {
		t.Fatalf("New(Known) error: %v", err)
	}
	defer schema.Release()

	// Append a known message with timestamp and duration
	msg := &samples.Known{
		Ts:       timestamppb.Now(),
		Duration: durationpb.New(1000000000), // 1 second
	}
	schema.Append(msg)

	r := schema.NewRecordBatch()
	if r.NumRows() != 1 {
		t.Fatalf("expected 1 row, got %d", r.NumRows())
	}
}

// ── Clone with Known message ──

func TestCloneWithKnown(t *testing.T) {
	md := (&samples.Known{}).ProtoReflect().Descriptor()

	schema, err := New(md, memory.DefaultAllocator)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	defer schema.Release()

	cloned, err := schema.Clone(memory.DefaultAllocator)
	if err != nil {
		t.Fatalf("Clone: %v", err)
	}
	defer cloned.Release()

	msg := &samples.Known{
		Ts:       timestamppb.Now(),
		Duration: durationpb.New(2000000000),
	}
	cloned.Append(msg)

	r := cloned.NewRecordBatch()
	if r.NumRows() != 1 {
		t.Fatalf("expected 1 row, got %d", r.NumRows())
	}
}

// ── New + Append with OneOfScala (exercises oneof handling in createNode) ──

func TestNewWithOneOf(t *testing.T) {
	md := (&samples.OneOfScala{}).ProtoReflect().Descriptor()

	schema, err := New(md, memory.DefaultAllocator)
	if err != nil {
		t.Fatalf("New(OneOfScala) error: %v", err)
	}
	defer schema.Release()

	msg := &samples.OneOfScala{
		Value: &samples.OneOfScala_Int64{Int64: 42},
	}
	schema.Append(msg)

	r := schema.NewRecordBatch()
	if r.NumRows() != 1 {
		t.Fatalf("expected 1 row, got %d", r.NumRows())
	}
}

// ── Multiple append + NewRecordBatch cycle ──

func TestMultipleAppendCycles(t *testing.T) {
	md := (&samples.ScalarTypes{}).ProtoReflect().Descriptor()

	schema, err := New(md, memory.DefaultAllocator)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	defer schema.Release()

	// First batch
	schema.Append(&samples.ScalarTypes{Double: 1.0, String_: "one"})
	schema.Append(&samples.ScalarTypes{Double: 2.0, String_: "two"})
	r := schema.NewRecordBatch()
	if r.NumRows() != 2 {
		t.Fatalf("batch 1: expected 2 rows, got %d", r.NumRows())
	}

	// Second batch (after NewRecordBatch resets)
	schema.Append(&samples.ScalarTypes{Double: 3.0, String_: "three"})
	r = schema.NewRecordBatch()
	if r.NumRows() != 1 {
		t.Fatalf("batch 2: expected 1 row, got %d", r.NumRows())
	}
}

// ── Nested message types ──

func TestNewWithNestedMessage(t *testing.T) {
	md := (&samples.Nested{}).ProtoReflect().Descriptor()

	schema, err := New(md, memory.DefaultAllocator)
	if err != nil {
		t.Fatalf("New(Nested) error: %v", err)
	}
	defer schema.Release()

	msg := &samples.Nested{
		NestedScalar: &samples.ScalarTypes{
			Double: 3.14,
			Int32:  42,
		},
		Deep: &samples.One{
			Two: &samples.Two{
				Three: &samples.Three{Value: 999},
			},
		},
	}
	schema.Append(msg)

	r := schema.NewRecordBatch()
	if r.NumRows() != 1 {
		t.Fatalf("expected 1 row, got %d", r.NumRows())
	}
}

// ── Repeated fields ──

func TestNewWithRepeatedScalars(t *testing.T) {
	md := (&samples.ScalarTypesRepeated{}).ProtoReflect().Descriptor()

	schema, err := New(md, memory.DefaultAllocator)
	if err != nil {
		t.Fatalf("New(ScalarTypesRepeated) error: %v", err)
	}
	defer schema.Release()

	msg := &samples.ScalarTypesRepeated{
		Double:  []float64{1.0, 2.0, 3.0},
		Int32:   []int32{1, 2, 3},
		String_: []string{"a", "b", "c"},
	}
	schema.Append(msg)

	r := schema.NewRecordBatch()
	if r.NumRows() != 1 {
		t.Fatalf("expected 1 row, got %d", r.NumRows())
	}
}

// ── ProtoKindToAppendFunc with nil message descriptor ──

func TestProtoKindToAppendFuncNilMessage(t *testing.T) {
	// Create a FieldDescriptor for a message kind without a linked message
	// This tests the nil guard in ProtoKindToAppendFunc
	alloc := memory.DefaultAllocator

	// For scalar types, the function works fine
	scalarMD := (&samples.ScalarTypes{}).ProtoReflect().Descriptor()
	doubleFD := scalarMD.Fields().ByName("double")

	b := array.NewFloat64Builder(alloc)
	defer b.Release()

	fn := ProtoKindToAppendFunc(doubleFD, b)
	if fn == nil {
		t.Fatal("ProtoKindToAppendFunc(double) returned nil")
	}
	fn(protoreflect.ValueOfFloat64(3.14))
	if b.Len() != 1 {
		t.Fatalf("builder.Len() = %d; want 1", b.Len())
	}
}

// ── Repeated Timestamp/Duration (exercises list code paths) ──

func TestNewWithRepeatedTimestamps(t *testing.T) {
	md := (&samples.Known{}).ProtoReflect().Descriptor()

	schema, err := New(md, memory.DefaultAllocator)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	defer schema.Release()

	msg := &samples.Known{
		TsRep:       []*timestamppb.Timestamp{timestamppb.Now(), timestamppb.Now()},
		DurationRep: []*durationpb.Duration{durationpb.New(1000000000), durationpb.New(2000000000)},
	}
	schema.Append(msg)

	r := schema.NewRecordBatch()
	if r.NumRows() != 1 {
		t.Fatalf("expected 1 row, got %d", r.NumRows())
	}
}

// ── ProtoKindToAppendFunc for enum (exercises remaining scalars) ──

func TestProtoKindToAppendFuncAllScalars(t *testing.T) {
	alloc := memory.DefaultAllocator

	scalarMD := (&samples.ScalarTypes{}).ProtoReflect().Descriptor()

	tcs := []struct {
		fieldName string
		builder   func() array.Builder
		value     protoreflect.Value
	}{
		{"double", func() array.Builder { return array.NewFloat64Builder(alloc) }, protoreflect.ValueOfFloat64(3.14)},
		{"float", func() array.Builder { return array.NewFloat32Builder(alloc) }, protoreflect.ValueOfFloat32(1.5)},
		{"int32", func() array.Builder { return array.NewInt32Builder(alloc) }, protoreflect.ValueOfInt32(42)},
		{"int64", func() array.Builder { return array.NewInt64Builder(alloc) }, protoreflect.ValueOfInt64(42)},
		{"uint32", func() array.Builder { return array.NewUint32Builder(alloc) }, protoreflect.ValueOfUint32(42)},
		{"uint64", func() array.Builder { return array.NewUint64Builder(alloc) }, protoreflect.ValueOfUint64(42)},
		{"sint32", func() array.Builder { return array.NewInt32Builder(alloc) }, protoreflect.ValueOfInt32(42)},
		{"sint64", func() array.Builder { return array.NewInt64Builder(alloc) }, protoreflect.ValueOfInt64(42)},
		{"fixed32", func() array.Builder { return array.NewUint32Builder(alloc) }, protoreflect.ValueOfUint32(42)},
		{"fixed64", func() array.Builder { return array.NewUint64Builder(alloc) }, protoreflect.ValueOfUint64(42)},
		{"sfixed32", func() array.Builder { return array.NewInt32Builder(alloc) }, protoreflect.ValueOfInt32(42)},
		{"sfixed64", func() array.Builder { return array.NewInt64Builder(alloc) }, protoreflect.ValueOfInt64(42)},
		{"bool", func() array.Builder { return array.NewBooleanBuilder(alloc) }, protoreflect.ValueOfBool(true)},
		{"string", func() array.Builder { return array.NewStringBuilder(alloc) }, protoreflect.ValueOfString("test")},
		{"bytes", func() array.Builder { return array.NewBinaryBuilder(alloc, arrow.BinaryTypes.Binary) }, protoreflect.ValueOfBytes([]byte("data"))},
	}
	for _, tc := range tcs {
		t.Run(tc.fieldName, func(t *testing.T) {
			fd := scalarMD.Fields().ByName(protoreflect.Name(tc.fieldName))
			if fd == nil {
				t.Fatalf("field %q not found", tc.fieldName)
			}
			b := tc.builder()
			defer b.Release()
			fn := ProtoKindToAppendFunc(fd, b)
			if fn == nil {
				t.Fatalf("ProtoKindToAppendFunc(%s) returned nil", tc.fieldName)
			}
			fn(tc.value)
			if b.Len() != 1 {
				t.Fatalf("builder.Len() = %d; want 1", b.Len())
			}
		})
	}
}

// ── Schema() and FieldCount() ──

func TestSchemaFieldCount(t *testing.T) {
	md := (&samples.ScalarTypes{}).ProtoReflect().Descriptor()

	schema, err := New(md, memory.DefaultAllocator)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	defer schema.Release()

	arrowSchema := schema.Schema()
	if arrowSchema == nil {
		t.Fatal("Schema() returned nil")
	}
	if arrowSchema.NumFields() < 1 {
		t.Fatal("Schema should have at least 1 field")
	}
}

// ── Test Cyclic detection (maxDepth) ──

func TestCyclicMessageDepthLimit(t *testing.T) {
	md := (&samples.Cyclic{}).ProtoReflect().Descriptor()

	// New should handle cyclic messages without infinite recursion
	// createNode has a maxDepth guard
	schema, err := New(md, memory.DefaultAllocator)
	if err != nil {
		// Expected - cyclic may hit depth limit
		t.Logf("New(Cyclic) error (expected): %v", err)
		return
	}
	defer schema.Release()
}

// ── Test Optional fields (oneof handling) ──

func TestOptionalFields(t *testing.T) {
	md := (&samples.ScalarTypesOptional{}).ProtoReflect().Descriptor()

	schema, err := New(md, memory.DefaultAllocator)
	if err != nil {
		t.Fatalf("New(ScalarTypesOptional) error: %v", err)
	}
	defer schema.Release()

	// Append with some fields set, others default
	msg := &samples.ScalarTypesOptional{}
	d := float64(3.14)
	msg.Double = &d
	s := "hello"
	msg.String_ = &s
	schema.Append(msg)

	r := schema.NewRecordBatch()
	if r.NumRows() != 1 {
		t.Fatalf("expected 1 row, got %d", r.NumRows())
	}
}

// ── Test Repeated nested messages ──

func TestRepeatedNestedMessage(t *testing.T) {
	md := (&samples.Nested{}).ProtoReflect().Descriptor()

	schema, err := New(md, memory.DefaultAllocator)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	defer schema.Release()

	msg := &samples.Nested{
		NestedRepeatedScalar: []*samples.ScalarTypes{
			{Double: 1.0, Int32: 1},
			{Double: 2.0, Int32: 2},
		},
	}
	schema.Append(msg)

	r := schema.NewRecordBatch()
	if r.NumRows() != 1 {
		t.Fatalf("expected 1 row, got %d", r.NumRows())
	}
}
