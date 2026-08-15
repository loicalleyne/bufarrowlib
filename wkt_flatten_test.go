package bufarrowlib

import (
	"testing"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/types/dynamicpb"
)

// flatDesc compiles testdata/proto/wkt_flatten_test.proto and returns the
// descriptor for the named message.
func flatDesc(t *testing.T, name string) protoreflect.MessageDescriptor {
	t.Helper()
	fd, err := CompileProtoToFileDescriptor("wkt_flatten_test.proto", []string{testdataProtoDir(t)})
	if err != nil {
		t.Fatalf("compile wkt_flatten_test.proto: %v", err)
	}
	md, err := GetMessageDescriptorByName(fd, name)
	if err != nil {
		t.Fatalf("descriptor %s: %v", name, err)
	}
	return md
}

// TestWithWellKnownTypesSchema verifies that the option flattens non-recursive
// well-known types into scalar Arrow columns, matching the denormalizer's
// mapping, and that it is off by default.
func TestWithWellKnownTypesSchema(t *testing.T) {
	md := flatDesc(t, "Flat")

	want := map[string]arrow.Type{
		"ts":    arrow.TIMESTAMP,
		"dur":   arrow.STRUCT, // Parquet has no DURATION; stays structural
		"mask":  arrow.STRING,
		"bv":    arrow.BOOL,
		"i32":   arrow.INT32,
		"i64":   arrow.INT64,
		"u32":   arrow.UINT32,
		"u64":   arrow.UINT64,
		"f32":   arrow.FLOAT32,
		"f64":   arrow.FLOAT64,
		"sv":    arrow.STRING,
		"bs":    arrow.BINARY,
		"plain": arrow.STRUCT, // ordinary messages still expand
	}

	t.Run("enabled flattens", func(t *testing.T) {
		tc, err := New(md, memory.DefaultAllocator, WithWellKnownTypes())
		if err != nil {
			t.Fatalf("New(WithWellKnownTypes) error = %v", err)
		}
		for name, wantID := range want {
			got := fieldType(t, tc.Schema(), name)
			if got.ID() != wantID {
				t.Errorf("field %s type = %v, want %v", name, got, wantID)
			}
		}
	})

	t.Run("disabled by default", func(t *testing.T) {
		tc, err := New(md, memory.DefaultAllocator)
		if err != nil {
			t.Fatalf("New() error = %v", err)
		}
		if got := fieldType(t, tc.Schema(), "ts"); got.ID() != arrow.STRUCT {
			t.Errorf("default ts type = %v, want struct (unchanged behaviour)", got)
		}
	})
}

// TestWithWellKnownTypesRepeatedAndMap verifies list and map wrapping around a
// flattened leaf.
func TestWithWellKnownTypesRepeatedAndMap(t *testing.T) {
	t.Run("repeated", func(t *testing.T) {
		tc, err := New(flatDesc(t, "FlatRepeated"), memory.DefaultAllocator, WithWellKnownTypes())
		if err != nil {
			t.Fatalf("New error = %v", err)
		}
		lt, ok := fieldType(t, tc.Schema(), "ts").(*arrow.ListType)
		if !ok {
			t.Fatalf("ts type = %v, want list", fieldType(t, tc.Schema(), "ts"))
		}
		if lt.Elem().ID() != arrow.TIMESTAMP {
			t.Errorf("ts elem = %v, want timestamp", lt.Elem())
		}
	})

	t.Run("map value", func(t *testing.T) {
		tc, err := New(flatDesc(t, "FlatMap"), memory.DefaultAllocator, WithWellKnownTypes())
		if err != nil {
			t.Fatalf("New error = %v", err)
		}
		mt, ok := fieldType(t, tc.Schema(), "ts").(*arrow.MapType)
		if !ok {
			t.Fatalf("ts type = %v, want map", fieldType(t, tc.Schema(), "ts"))
		}
		if mt.ItemType().ID() != arrow.TIMESTAMP {
			t.Errorf("ts item = %v, want timestamp", mt.ItemType())
		}
	})
}

// TestWithWellKnownTypesRoundTrip verifies the decode direction: a flattened
// column must reconstruct the original protobuf message via Proto().
func TestWithWellKnownTypesRoundTrip(t *testing.T) {
	md := flatDesc(t, "Flat")
	tc, err := New(md, memory.DefaultAllocator, WithWellKnownTypes())
	if err != nil {
		t.Fatalf("New error = %v", err)
	}

	msg := dynamicpb.NewMessage(md)
	setMsgField := func(field string, inner func(protoreflect.Message)) {
		fd := md.Fields().ByName(protoreflect.Name(field))
		m := dynamicpb.NewMessage(fd.Message())
		inner(m)
		msg.Set(fd, protoreflect.ValueOfMessage(m))
	}
	setScalarWrapper := func(field string, v protoreflect.Value) {
		setMsgField(field, func(m protoreflect.Message) {
			m.Set(m.Descriptor().Fields().ByName("value"), v)
		})
	}

	ref := time.Date(2026, 8, 15, 12, 34, 56, 789000000, time.UTC)
	setMsgField("ts", func(m protoreflect.Message) {
		m.Set(m.Descriptor().Fields().ByName("seconds"), protoreflect.ValueOfInt64(ref.Unix()))
		m.Set(m.Descriptor().Fields().ByName("nanos"), protoreflect.ValueOfInt32(int32(ref.Nanosecond())))
	})
	setMsgField("dur", func(m protoreflect.Message) {
		m.Set(m.Descriptor().Fields().ByName("seconds"), protoreflect.ValueOfInt64(90))
		m.Set(m.Descriptor().Fields().ByName("nanos"), protoreflect.ValueOfInt32(500000000))
	})
	setMsgField("mask", func(m protoreflect.Message) {
		l := m.Mutable(m.Descriptor().Fields().ByName("paths")).List()
		l.Append(protoreflect.ValueOfString("a.b"))
		l.Append(protoreflect.ValueOfString("c"))
	})
	setScalarWrapper("bv", protoreflect.ValueOfBool(true))
	setScalarWrapper("i32", protoreflect.ValueOfInt32(-7))
	setScalarWrapper("i64", protoreflect.ValueOfInt64(-8))
	setScalarWrapper("u32", protoreflect.ValueOfUint32(9))
	setScalarWrapper("u64", protoreflect.ValueOfUint64(10))
	setScalarWrapper("f32", protoreflect.ValueOfFloat32(1.5))
	setScalarWrapper("f64", protoreflect.ValueOfFloat64(2.5))
	setScalarWrapper("sv", protoreflect.ValueOfString("hello"))
	setScalarWrapper("bs", protoreflect.ValueOfBytes([]byte{1, 2, 3}))
	// Populated so the comparison isolates well-known types: an unset ordinary
	// message decodes as empty rather than staying unset, independently of this
	// option.
	setMsgField("plain", func(m protoreflect.Message) {
		m.Set(m.Descriptor().Fields().ByName("a"), protoreflect.ValueOfString("x"))
	})

	tc.Append(msg.Interface())
	rec := tc.NewRecordBatch()
	defer rec.Release()

	got := tc.Proto(rec, nil)
	if len(got) != 1 {
		t.Fatalf("Proto returned %d messages, want 1", len(got))
	}
	if !proto.Equal(got[0], msg.Interface()) {
		t.Errorf("round-trip mismatch:\n got %v\nwant %v", got[0], msg.Interface())
	}
}

// TestWithWellKnownTypesUnset verifies unset well-known fields become nulls
// rather than desynchronising the builder.
func TestWithWellKnownTypesUnset(t *testing.T) {
	md := flatDesc(t, "Flat")
	tc, err := New(md, memory.DefaultAllocator, WithWellKnownTypes())
	if err != nil {
		t.Fatalf("New error = %v", err)
	}
	tc.Append(dynamicpb.NewMessage(md).Interface())
	rec := tc.NewRecordBatch()
	defer rec.Release()

	if rec.NumRows() != 1 {
		t.Fatalf("NumRows = %d, want 1", rec.NumRows())
	}
	for i := 0; i < int(rec.NumCols()); i++ {
		if rec.Column(i).Len() != 1 {
			t.Errorf("column %q len = %d, want 1 (builder desync)",
				rec.ColumnName(i), rec.Column(i).Len())
		}
	}
	idx := rec.Schema().FieldIndices("ts")
	if !rec.Column(idx[0]).IsNull(0) {
		t.Errorf("unset ts = %v, want null", rec.Column(idx[0]))
	}
}

// TestWithWellKnownTypesClone verifies Clone inherits the option; a clone whose
// schema disagreed with its parent would corrupt concurrent writers.
func TestWithWellKnownTypesClone(t *testing.T) {
	tc, err := New(flatDesc(t, "Flat"), memory.DefaultAllocator, WithWellKnownTypes())
	if err != nil {
		t.Fatalf("New error = %v", err)
	}
	clone, err := tc.Clone(memory.DefaultAllocator)
	if err != nil {
		t.Fatalf("Clone error = %v", err)
	}
	if got := fieldType(t, clone.Schema(), "ts"); got.ID() != arrow.TIMESTAMP {
		t.Errorf("clone ts type = %v, want timestamp (option not inherited)", got)
	}
}
