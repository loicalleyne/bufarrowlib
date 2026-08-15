package bufarrowlib

import (
	"errors"
	"strings"
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/loicalleyne/bufarrowlib/proto/pbpath"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/types/dynamicpb"
)

// wktDesc compiles testdata/proto/wkt_test.proto and returns the descriptor for
// the named message.
func wktDesc(t *testing.T, name string) protoreflect.MessageDescriptor {
	t.Helper()
	fd, err := CompileProtoToFileDescriptor("wkt_test.proto", []string{testdataProtoDir(t)})
	if err != nil {
		t.Fatalf("compile wkt_test.proto: %v", err)
	}
	md, err := GetMessageDescriptorByName(fd, name)
	if err != nil {
		t.Fatalf("descriptor %s: %v", name, err)
	}
	return md
}

// anyHolderDesc compiles the AnyValue stand-in holder message.
func anyHolderDesc(t *testing.T) protoreflect.MessageDescriptor {
	t.Helper()
	fd, err := CompileProtoToFileDescriptor("wkt_any_holder.proto", []string{testdataProtoDir(t)})
	if err != nil {
		t.Fatalf("compile wkt_any_holder.proto: %v", err)
	}
	md, err := GetMessageDescriptorByName(fd, "WithAny")
	if err != nil {
		t.Fatalf("descriptor WithAny: %v", err)
	}
	return md
}

// fieldType returns the Arrow type of the named top-level field in the schema.
func fieldType(t *testing.T, s *arrow.Schema, name string) arrow.DataType {
	t.Helper()
	fields := s.FieldIndices(name)
	if len(fields) == 0 {
		t.Fatalf("field %q not in schema %s", name, s.String())
	}
	return s.Field(fields[0]).Type
}

// TestSetAWKTLeafTypes verifies that the recursive well-known types terminate
// as protojson strings instead of being expanded into nested Arrow structs.
func TestSetAWKTLeafTypes(t *testing.T) {
	t.Run("Struct is a string leaf", func(t *testing.T) {
		tc, err := New(wktDesc(t, "WithStruct"), memory.DefaultAllocator)
		if err != nil {
			t.Fatalf("New(WithStruct) error = %v", err)
		}
		if got := fieldType(t, tc.Schema(), "settings"); got.ID() != arrow.STRING {
			t.Errorf("settings type = %v, want string", got)
		}
	})

	t.Run("map value Value is a string leaf", func(t *testing.T) {
		tc, err := New(wktDesc(t, "WithValueMap"), memory.DefaultAllocator)
		if err != nil {
			t.Fatalf("New(WithValueMap) error = %v", err)
		}
		mt, ok := fieldType(t, tc.Schema(), "attrs").(*arrow.MapType)
		if !ok {
			t.Fatalf("attrs type = %v, want map", fieldType(t, tc.Schema(), "attrs"))
		}
		if mt.ItemType().ID() != arrow.STRING {
			t.Errorf("attrs item type = %v, want string", mt.ItemType())
		}
	})

	t.Run("repeated Value is a list of string", func(t *testing.T) {
		tc, err := New(wktDesc(t, "WithValueList"), memory.DefaultAllocator)
		if err != nil {
			t.Fatalf("New(WithValueList) error = %v", err)
		}
		lt, ok := fieldType(t, tc.Schema(), "values").(*arrow.ListType)
		if !ok {
			t.Fatalf("values type = %v, want list", fieldType(t, tc.Schema(), "values"))
		}
		if lt.Elem().ID() != arrow.STRING {
			t.Errorf("values elem type = %v, want string", lt.Elem())
		}
	})

	// The originally reported failure: map<string, NsEntry> where NsEntry holds
	// map<string, google.protobuf.Value>. Before the fix this exhausted maxDepth.
	t.Run("nested namespace map builds", func(t *testing.T) {
		if _, err := New(wktDesc(t, "WithNestedNs"), memory.DefaultAllocator); err != nil {
			t.Fatalf("New(WithNestedNs) error = %v", err)
		}
	})
}

// TestSetAWKTUnsetValue covers the protojson.Marshal failure surface. An unset
// google.protobuf.Value cannot be marshalled, so the append closure must emit a
// null rather than returning an error, which would desynchronise the builder.
func TestSetAWKTUnsetValue(t *testing.T) {
	md := wktDesc(t, "WithValue")
	tc, err := New(md, memory.DefaultAllocator)
	if err != nil {
		t.Fatalf("New(WithValue) error = %v", err)
	}

	// Every field left unset, including the oneof.
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
	idx := rec.Schema().FieldIndices("v")
	if len(idx) == 0 {
		t.Fatal("field v missing from schema")
	}
	if !rec.Column(idx[0]).IsNull(0) {
		t.Errorf("unset Value cell = %v, want null", rec.Column(idx[0]))
	}
}

// TestSetAWKTEmptyMapValue covers a present-but-empty Value inside a map, which
// passes the IsValid guard but still fails to marshal.
func TestSetAWKTEmptyMapValue(t *testing.T) {
	md := wktDesc(t, "WithValueMap")
	tc, err := New(md, memory.DefaultAllocator)
	if err != nil {
		t.Fatalf("New(WithValueMap) error = %v", err)
	}

	msg := dynamicpb.NewMessage(md)
	attrsFD := md.Fields().ByName("attrs")
	empty := dynamicpb.NewMessage(attrsFD.MapValue().Message())
	msg.Mutable(attrsFD).Map().Set(
		protoreflect.ValueOfString("k").MapKey(),
		protoreflect.ValueOfMessage(empty),
	)

	tc.Append(msg.Interface())
	rec := tc.NewRecordBatch()
	defer rec.Release()
	if rec.NumRows() != 1 {
		t.Fatalf("NumRows = %d, want 1", rec.NumRows())
	}
}

// TestSetAWKTAppendAndRoundTrip appends a populated Struct and verifies the JSON
// cell semantically. protojson output is not byte-stable across processes, so
// the comparison unmarshals both sides rather than comparing strings.
func TestSetAWKTAppendAndRoundTrip(t *testing.T) {
	md := wktDesc(t, "WithStruct")
	tc, err := New(md, memory.DefaultAllocator)
	if err != nil {
		t.Fatalf("New(WithStruct) error = %v", err)
	}

	msg := dynamicpb.NewMessage(md)
	settingsFD := md.Fields().ByName("settings")
	structMD := settingsFD.Message()
	st := dynamicpb.NewMessage(structMD)
	fieldsFD := structMD.Fields().ByName("fields")
	valueMD := fieldsFD.MapValue().Message()
	v := dynamicpb.NewMessage(valueMD)
	v.Set(valueMD.Fields().ByName("string_value"), protoreflect.ValueOfString("on"))
	st.Mutable(fieldsFD).Map().Set(
		protoreflect.ValueOfString("mode").MapKey(),
		protoreflect.ValueOfMessage(v),
	)
	msg.Set(settingsFD, protoreflect.ValueOfMessage(st))

	tc.Append(msg.Interface())
	rec := tc.NewRecordBatch()
	defer rec.Release()

	idx := rec.Schema().FieldIndices("settings")
	col, ok := rec.Column(idx[0]).(*array.String)
	if !ok {
		t.Fatalf("settings column = %T, want *array.String", rec.Column(idx[0]))
	}

	got := dynamicpb.NewMessage(structMD)
	if err := protojson.Unmarshal([]byte(col.Value(0)), got.Interface()); err != nil {
		t.Fatalf("protojson.Unmarshal(%q) error = %v", col.Value(0), err)
	}
	if !proto.Equal(got.Interface(), st.Interface()) {
		t.Errorf("round-trip mismatch:\n got %v\nwant %v", got, st)
	}
}

// TestSetAWKTAppendRawHyperType exercises the path pybufarrow actually uses:
// raw protobuf bytes parsed by hyperpb, not dynamicpb.
func TestSetAWKTAppendRawHyperType(t *testing.T) {
	md := wktDesc(t, "WithValueMap")

	msg := dynamicpb.NewMessage(md)
	attrsFD := md.Fields().ByName("attrs")
	valueMD := attrsFD.MapValue().Message()
	v := dynamicpb.NewMessage(valueMD)
	v.Set(valueMD.Fields().ByName("string_value"), protoreflect.ValueOfString("hello"))
	msg.Mutable(attrsFD).Map().Set(
		protoreflect.ValueOfString("k").MapKey(),
		protoreflect.ValueOfMessage(v),
	)
	raw, err := proto.MarshalOptions{Deterministic: true}.Marshal(msg.Interface())
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}

	ht := NewHyperType(md)
	tc, err := New(md, memory.DefaultAllocator, WithHyperType(ht))
	if err != nil {
		t.Fatalf("New(WithHyperType) error = %v", err)
	}
	if err := tc.AppendRaw(raw); err != nil {
		t.Fatalf("AppendRaw error = %v", err)
	}

	rec := tc.NewRecordBatch()
	defer rec.Release()
	if rec.NumRows() != 1 {
		t.Fatalf("NumRows = %d, want 1", rec.NumRows())
	}

	ma, ok := rec.Column(rec.Schema().FieldIndices("attrs")[0]).(*array.Map)
	if !ok {
		t.Fatalf("attrs column = %T, want *array.Map", rec.Column(0))
	}
	items, ok := ma.Items().(*array.String)
	if !ok {
		t.Fatalf("attrs items = %T, want *array.String", ma.Items())
	}
	got := dynamicpb.NewMessage(valueMD)
	if err := protojson.Unmarshal([]byte(items.Value(0)), got.Interface()); err != nil {
		t.Fatalf("protojson.Unmarshal(%q) error = %v", items.Value(0), err)
	}
	if !proto.Equal(got.Interface(), v.Interface()) {
		t.Errorf("hyperpb round-trip mismatch: got %v want %v", got, v)
	}
}

// TestOtelAnyValueViaProtocompile guards the FullName-based dispatch. A
// protocompile-produced AnyValue descriptor is not pointer-identical to the
// linked-in one, so identity dispatch silently missed it.
func TestOtelAnyValueViaProtocompile(t *testing.T) {
	md := anyHolderDesc(t)
	tc, err := New(md, memory.DefaultAllocator)
	if err != nil {
		t.Fatalf("New(WithAny) error = %v", err)
	}
	if got := fieldType(t, tc.Schema(), "any"); got.ID() != arrow.BINARY {
		t.Fatalf("any type = %v, want binary", got)
	}

	msg := dynamicpb.NewMessage(md)
	anyFD := md.Fields().ByName("any")
	anyMD := anyFD.Message()
	av := dynamicpb.NewMessage(anyMD)
	av.Set(anyMD.Fields().ByName("string_value"), protoreflect.ValueOfString("x"))
	msg.Set(anyFD, protoreflect.ValueOfMessage(av))

	// Before the fix this panicked on the (*commonv1.AnyValue) type assertion.
	tc.Append(msg.Interface())
	rec := tc.NewRecordBatch()
	defer rec.Release()
	if rec.NumRows() != 1 {
		t.Fatalf("NumRows = %d, want 1", rec.NumRows())
	}
}

// TestCycleDetectionDiamond ensures a message type reused in two sibling fields
// is not mistaken for a cycle. Without backtracking in the ancestor set this
// regresses.
func TestCycleDetectionDiamond(t *testing.T) {
	if _, err := New(wktDesc(t, "DiamondOuter"), memory.DefaultAllocator); err != nil {
		t.Fatalf("New(DiamondOuter) error = %v, want nil", err)
	}
}

// TestCycleDetectionSelfRef verifies a genuinely recursive message reports
// ErrCyclicType naming the type and path.
func TestCycleDetectionSelfRef(t *testing.T) {
	_, err := New(wktDesc(t, "SelfRef"), memory.DefaultAllocator)
	if err == nil {
		t.Fatal("New(SelfRef) error = nil, want ErrCyclicType")
	}
	if !errors.Is(err, ErrCyclicType) {
		t.Fatalf("New(SelfRef) error = %v, want ErrCyclicType", err)
	}
	if !strings.Contains(err.Error(), "SelfRef") {
		t.Errorf("error %q does not name the cyclic type", err)
	}
}

// TestDepthGuardStillReachable proves maxDepth remains a live secondary guard
// for deep acyclic nesting after cycle detection is added.
func TestDepthGuardStillReachable(t *testing.T) {
	_, err := New(wktDesc(t, "DeepAcyclic12"), memory.DefaultAllocator)
	if err == nil {
		t.Fatal("New(DeepAcyclic12) error = nil, want ErrMxDepth")
	}
	if !errors.Is(err, ErrMxDepth) {
		t.Fatalf("New(DeepAcyclic12) error = %v, want ErrMxDepth", err)
	}
}

// TestSetAWKTDenormLeaf exercises the denormalizer path, which resolves column
// types through ProtoKindToArrowType and appends through ProtoKindToAppendFunc
// rather than through the node tree.
func TestSetAWKTDenormLeaf(t *testing.T) {
	md := wktDesc(t, "WithStruct")
	tc, err := New(md, memory.DefaultAllocator,
		WithDenormalizerPlan(pbpath.PlanPath("settings")))
	if err != nil {
		t.Fatalf("New(WithStruct, denorm) error = %v", err)
	}

	settingsFD := md.Fields().ByName("settings")
	structMD := settingsFD.Message()
	st := dynamicpb.NewMessage(structMD)
	fieldsFD := structMD.Fields().ByName("fields")
	valueMD := fieldsFD.MapValue().Message()
	v := dynamicpb.NewMessage(valueMD)
	v.Set(valueMD.Fields().ByName("number_value"), protoreflect.ValueOfFloat64(42))
	st.Mutable(fieldsFD).Map().Set(
		protoreflect.ValueOfString("n").MapKey(),
		protoreflect.ValueOfMessage(v),
	)
	msg := dynamicpb.NewMessage(md)
	msg.Set(settingsFD, protoreflect.ValueOfMessage(st))

	if err := tc.AppendDenorm(msg.Interface()); err != nil {
		t.Fatalf("AppendDenorm error = %v", err)
	}
	rec := tc.NewDenormalizerRecordBatch()
	defer rec.Release()

	if rec.NumRows() != 1 {
		t.Fatalf("NumRows = %d, want 1", rec.NumRows())
	}
	col, ok := rec.Column(0).(*array.String)
	if !ok {
		t.Fatalf("denorm column = %T, want *array.String", rec.Column(0))
	}
	got := dynamicpb.NewMessage(structMD)
	if err := protojson.Unmarshal([]byte(col.Value(0)), got.Interface()); err != nil {
		t.Fatalf("protojson.Unmarshal(%q) error = %v", col.Value(0), err)
	}
	if !proto.Equal(got.Interface(), st.Interface()) {
		t.Errorf("denorm round-trip mismatch: got %v want %v", got, st)
	}
}
