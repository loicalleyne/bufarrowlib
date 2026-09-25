package bufarrowlib

import (
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/loicalleyne/bufarrowlib/gen/go/samples"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/types/dynamicpb"
)

// TestProtoKindToArrowTypeMapAndMessageBranches exercises map and message
// branch behavior that is easy to miss in end-to-end tests.
func TestProtoKindToArrowTypeMapAndMessageBranches(t *testing.T) {
	t.Run("map to supported value", func(t *testing.T) {
		md := flatDesc(t, "FlatMap")
		fd := md.Fields().ByName("ts")
		if fd == nil {
			t.Fatal("field ts not found")
		}
		got := ProtoKindToArrowType(fd)
		mt, ok := got.(*arrow.MapType)
		if !ok {
			t.Fatalf("ProtoKindToArrowType(ts) = %T, want *arrow.MapType", got)
		}
		if mt.KeyType().ID() != arrow.STRING {
			t.Fatalf("map key type = %v, want string", mt.KeyType())
		}
		if mt.ItemType().ID() != arrow.TIMESTAMP {
			t.Fatalf("map item type = %v, want timestamp", mt.ItemType())
		}
	})

	t.Run("map to unsupported nested message", func(t *testing.T) {
		md := wktDesc(t, "WithNestedNs")
		fd := md.Fields().ByName("settings_namespaces")
		if fd == nil {
			t.Fatal("field settings_namespaces not found")
		}
		if got := ProtoKindToArrowType(fd); got != nil {
			t.Fatalf("ProtoKindToArrowType(settings_namespaces) = %v, want nil", got)
		}
	})

	t.Run("plain message remains unsupported", func(t *testing.T) {
		md := flatDesc(t, "Flat")
		fd := md.Fields().ByName("plain")
		if fd == nil {
			t.Fatal("field plain not found")
		}
		if got := ProtoKindToArrowType(fd); got != nil {
			t.Fatalf("ProtoKindToArrowType(plain) = %v, want nil", got)
		}
	})

	t.Run("otel any maps to binary", func(t *testing.T) {
		md := anyHolderDesc(t)
		fd := md.Fields().ByName("any")
		if fd == nil {
			t.Fatal("field any not found")
		}
		got := ProtoKindToArrowType(fd)
		if got == nil || got.ID() != arrow.BINARY {
			t.Fatalf("ProtoKindToArrowType(any) = %v, want binary", got)
		}
	})
}

// TestFlattenableWKTBranches covers negative and positive flattenability
// checks used by WithWellKnownTypes.
func TestFlattenableWKTBranches(t *testing.T) {
	flatMD := flatDesc(t, "Flat")
	if !flattenableWKT(flatMD.Fields().ByName("ts")) {
		t.Fatal("flattenableWKT(ts) = false, want true")
	}
	if flattenableWKT(flatMD.Fields().ByName("dur")) {
		t.Fatal("flattenableWKT(dur) = true, want false")
	}
	if flattenableWKT(flatMD.Fields().ByName("plain")) {
		t.Fatal("flattenableWKT(plain) = true, want false")
	}

	flatMapMD := flatDesc(t, "FlatMap")
	if flattenableWKT(flatMapMD.Fields().ByName("ts")) {
		t.Fatal("flattenableWKT(map field) = true, want false")
	}

	anyMD := anyHolderDesc(t)
	if flattenableWKT(anyMD.Fields().ByName("any")) {
		t.Fatal("flattenableWKT(otel any) = true, want false")
	}

	structMD := wktDesc(t, "WithStruct")
	if flattenableWKT(structMD.Fields().ByName("settings")) {
		t.Fatal("flattenableWKT(Struct) = true, want false")
	}

	scalarMD := (&samples.ScalarTypes{}).ProtoReflect().Descriptor()
	if flattenableWKT(scalarMD.Fields().ByName("double")) {
		t.Fatal("flattenableWKT(double scalar) = true, want false")
	}
}

// TestStringAndBinaryCellDictionary exercises dictionary decode helpers used
// by flattened WKT decode paths.
func TestStringAndBinaryCellDictionary(t *testing.T) {
	alloc := memory.DefaultAllocator

	t.Run("stringCell dictionary", func(t *testing.T) {
		dt := &arrow.DictionaryType{IndexType: arrow.PrimitiveTypes.Int32, ValueType: arrow.BinaryTypes.String}
		db := array.NewDictionaryBuilder(alloc, dt)
		defer db.Release()

		bb, ok := db.(*array.BinaryDictionaryBuilder)
		if !ok {
			t.Fatalf("dictionary builder type = %T, want *array.BinaryDictionaryBuilder", db)
		}
		if err := bb.AppendString("alpha"); err != nil {
			t.Fatalf("AppendString(alpha): %v", err)
		}
		if err := bb.AppendString("beta"); err != nil {
			t.Fatalf("AppendString(beta): %v", err)
		}

		arr := bb.NewDictionaryArray()
		defer arr.Release()

		if got := stringCell(arr, 1); got != "beta" {
			t.Fatalf("stringCell(dictionary, 1) = %q, want beta", got)
		}
	})

	t.Run("binaryCell dictionary", func(t *testing.T) {
		dt := &arrow.DictionaryType{IndexType: arrow.PrimitiveTypes.Int32, ValueType: arrow.BinaryTypes.Binary}
		db := array.NewDictionaryBuilder(alloc, dt)
		defer db.Release()

		bb, ok := db.(*array.BinaryDictionaryBuilder)
		if !ok {
			t.Fatalf("dictionary builder type = %T, want *array.BinaryDictionaryBuilder", db)
		}
		if err := bb.Append([]byte("x")); err != nil {
			t.Fatalf("Append(x): %v", err)
		}
		if err := bb.Append([]byte("y")); err != nil {
			t.Fatalf("Append(y): %v", err)
		}

		arr := bb.NewDictionaryArray()
		defer arr.Release()

		if got := string(binaryCell(arr, 0)); got != "x" {
			t.Fatalf("binaryCell(dictionary, 0) = %q, want x", got)
		}
	})
}

// TestFloorDivSignedInputs covers sign combinations to ensure Euclidean
// decomposition remains correct for timestamp decoding.
func TestFloorDivSignedInputs(t *testing.T) {
	tcs := []struct {
		a, b int64
		want int64
	}{
		{5, 2, 2},
		{-5, 2, -3},
		{5, -2, -3},
		{-5, -2, 2},
	}

	for _, tc := range tcs {
		if got := floorDiv(tc.a, tc.b); got != tc.want {
			t.Fatalf("floorDiv(%d, %d) = %d, want %d", tc.a, tc.b, got, tc.want)
		}
		if mod := floorMod(tc.a, tc.b); mod != tc.a-tc.want*tc.b {
			t.Fatalf("floorMod(%d, %d) = %d, want %d", tc.a, tc.b, mod, tc.a-tc.want*tc.b)
		}
	}
}

// TestWKTFlattenEncodeUnsupportedMessage exercises the nil encode function
// fallback for message types outside the flattening allowlist.
func TestWKTFlattenEncodeUnsupportedMessage(t *testing.T) {
	md := flatDesc(t, "Flat")
	fd := md.Fields().ByName(protoreflect.Name("plain"))
	if fd == nil {
		t.Fatal("field plain not found")
	}
	if enc := wktFlattenEncode(fd); enc != nil {
		t.Fatalf("wktFlattenEncode(plain) = %v, want nil", enc)
	}
}

// TestProtoKindToAppendFuncMap exercises map append closures in both supported
// and unsupported map-value scenarios.
func TestProtoKindToAppendFuncMap(t *testing.T) {
	alloc := memory.DefaultAllocator

	t.Run("supported map value", func(t *testing.T) {
		md := flatDesc(t, "FlatMap")
		fd := md.Fields().ByName("ts")
		if fd == nil {
			t.Fatal("field ts not found")
		}

		dt := ProtoKindToArrowType(fd)
		mt, ok := dt.(*arrow.MapType)
		if !ok {
			t.Fatalf("ProtoKindToArrowType(ts) = %T, want *arrow.MapType", dt)
		}

		mb := array.NewMapBuilder(alloc, mt.KeyType(), mt.ItemType(), false)
		defer mb.Release()

		appendFn := ProtoKindToAppendFunc(fd, mb)
		if appendFn == nil {
			t.Fatal("ProtoKindToAppendFunc(map) returned nil")
		}

		msg := dynamicpb.NewMessage(md)
		m := msg.Mutable(fd).Map()
		tsFD := fd.MapValue().Message().Fields().ByName("seconds")
		nanosFD := fd.MapValue().Message().Fields().ByName("nanos")

		ts := dynamicpb.NewMessage(fd.MapValue().Message())
		ts.Set(tsFD, protoreflect.ValueOfInt64(1))
		ts.Set(nanosFD, protoreflect.ValueOfInt32(500000000))
		m.Set(protoreflect.ValueOfString("a").MapKey(), protoreflect.ValueOfMessage(ts))

		appendFn(msg.Get(fd))

		arr := mb.NewMapArray()
		defer arr.Release()
		if arr.Len() != 1 {
			t.Fatalf("map array len = %d, want 1", arr.Len())
		}
		if arr.ListValues().Len() != 1 {
			t.Fatalf("map entry count = %d, want 1", arr.ListValues().Len())
		}
	})

	t.Run("unsupported nested value map", func(t *testing.T) {
		md := wktDesc(t, "WithNestedNs")
		fd := md.Fields().ByName("settings_namespaces")
		if fd == nil {
			t.Fatal("field settings_namespaces not found")
		}

		mb := array.NewMapBuilder(alloc, arrow.BinaryTypes.String, arrow.BinaryTypes.String, false)
		defer mb.Release()

		if fn := ProtoKindToAppendFunc(fd, mb); fn != nil {
			t.Fatal("ProtoKindToAppendFunc(unsupported map) should return nil")
		}
	})
}

// TestBinaryWKTSetupInvalidUTF8 covers the branch where proto.Marshal fails
// inside binaryWKTSetup and the closure appends null instead of erroring.
func TestBinaryWKTSetupInvalidUTF8(t *testing.T) {
	alloc := memory.DefaultAllocator
	b := array.NewBinaryBuilder(alloc, arrow.BinaryTypes.Binary)
	defer b.Release()

	vf := binaryWKTSetup()(b)

	md := anyHolderDesc(t).Fields().ByName("any").Message()
	msg := dynamicpb.NewMessage(md)
	msg.Set(md.Fields().ByName("string_value"), protoreflect.ValueOfString(string([]byte{0xff})))

	if err := vf(protoreflect.ValueOfMessage(msg), true); err != nil {
		t.Fatalf("binaryWKTSetup invalid utf8 error = %v, want nil", err)
	}
	if err := vf(protoreflect.Value{}, true); err != nil {
		t.Fatalf("binaryWKTSetup invalid value error = %v, want nil", err)
	}

	arr := b.NewBinaryArray()
	defer arr.Release()
	if !arr.IsNull(0) {
		t.Fatal("expected marshal failure row to be null")
	}
	if !arr.IsNull(1) {
		t.Fatal("expected invalid value row to be null")
	}
}

// TestProtoKindToAppendFuncMapScalars covers scalar map-entry appends and the
// empty-map case via generated sample descriptors.
func TestProtoKindToAppendFuncMapScalars(t *testing.T) {
	alloc := memory.DefaultAllocator
	md := (&samples.MapVariety{}).ProtoReflect().Descriptor()
	fd := md.Fields().ByName("string_int_map")
	if fd == nil {
		t.Fatal("field string_int_map not found")
	}

	dt := ProtoKindToArrowType(fd)
	mt, ok := dt.(*arrow.MapType)
	if !ok {
		t.Fatalf("ProtoKindToArrowType(string_int_map) = %T, want *arrow.MapType", dt)
	}

	t.Run("non-empty", func(t *testing.T) {
		mb := array.NewMapBuilder(alloc, mt.KeyType(), mt.ItemType(), false)
		defer mb.Release()

		fn := ProtoKindToAppendFunc(fd, mb)
		if fn == nil {
			t.Fatal("ProtoKindToAppendFunc(string_int_map) returned nil")
		}

		msg := (&samples.MapVariety{StringIntMap: map[string]int32{"a": 1, "b": 2}}).ProtoReflect()
		fn(msg.Get(fd))

		arr := mb.NewMapArray()
		defer arr.Release()
		if arr.Len() != 1 {
			t.Fatalf("map array len = %d, want 1", arr.Len())
		}
		if arr.ListValues().Len() != 2 {
			t.Fatalf("map entry count = %d, want 2", arr.ListValues().Len())
		}
	})

	t.Run("empty map", func(t *testing.T) {
		mb := array.NewMapBuilder(alloc, mt.KeyType(), mt.ItemType(), false)
		defer mb.Release()

		fn := ProtoKindToAppendFunc(fd, mb)
		if fn == nil {
			t.Fatal("ProtoKindToAppendFunc(string_int_map) returned nil")
		}

		msg := (&samples.MapVariety{StringIntMap: map[string]int32{}}).ProtoReflect()
		fn(msg.Get(fd))

		arr := mb.NewMapArray()
		defer arr.Release()
		if arr.Len() != 1 {
			t.Fatalf("map array len = %d, want 1", arr.Len())
		}
		if arr.ListValues().Len() != 0 {
			t.Fatalf("map entry count = %d, want 0", arr.ListValues().Len())
		}
	})
}

// TestProtoKindToAppendFuncMarshalFailures verifies marshal-error paths append
// nulls for message kinds encoded as JSON/Binary leaves.
func TestProtoKindToAppendFuncMarshalFailures(t *testing.T) {
	alloc := memory.DefaultAllocator

	t.Run("otel any marshal error appends null", func(t *testing.T) {
		md := anyHolderDesc(t)
		fd := md.Fields().ByName("any")
		if fd == nil {
			t.Fatal("field any not found")
		}

		b := array.NewBinaryBuilder(alloc, arrow.BinaryTypes.Binary)
		defer b.Release()

		fn := ProtoKindToAppendFunc(fd, b)
		if fn == nil {
			t.Fatal("ProtoKindToAppendFunc(any) returned nil")
		}

		msg := dynamicpb.NewMessage(fd.Message())
		msg.Set(fd.Message().Fields().ByName("string_value"), protoreflect.ValueOfString(string([]byte{0xff})))
		fn(protoreflect.ValueOfMessage(msg))

		arr := b.NewBinaryArray()
		defer arr.Release()
		if !arr.IsNull(0) {
			t.Fatal("expected null when AnyValue marshal fails")
		}
	})

	t.Run("google type money marshal error appends null", func(t *testing.T) {
		md := buildWKTFiles(t)
		fd := md.Fields().ByName("money")
		if fd == nil {
			t.Fatal("field money not found")
		}

		b := array.NewStringBuilder(alloc)
		defer b.Release()

		fn := ProtoKindToAppendFunc(fd, b)
		if fn == nil {
			t.Fatal("ProtoKindToAppendFunc(money) returned nil")
		}

		msg := dynamicpb.NewMessage(fd.Message())
		msg.Set(fd.Message().Fields().ByName("currency_code"), protoreflect.ValueOfString(string([]byte{0xff})))
		fn(protoreflect.ValueOfMessage(msg))

		arr := b.NewStringArray()
		defer arr.Release()
		if !arr.IsNull(0) {
			t.Fatal("expected null when Money protojson marshal fails")
		}
	})

	t.Run("recursive value marshal error appends null", func(t *testing.T) {
		md := wktDesc(t, "WithValue")
		fd := md.Fields().ByName("v")
		if fd == nil {
			t.Fatal("field v not found")
		}

		b := array.NewStringBuilder(alloc)
		defer b.Release()

		fn := ProtoKindToAppendFunc(fd, b)
		if fn == nil {
			t.Fatal("ProtoKindToAppendFunc(v) returned nil")
		}

		msg := dynamicpb.NewMessage(fd.Message())
		fn(protoreflect.ValueOfMessage(msg))

		arr := b.NewStringArray()
		defer arr.Release()
		if !arr.IsNull(0) {
			t.Fatal("expected null when Value protojson marshal fails")
		}
	})
}
