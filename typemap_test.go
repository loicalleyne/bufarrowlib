package bufarrowlib

import (
	"encoding/json"
	"fmt"
	"testing"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/loicalleyne/bufarrowlib/proto/pbpath"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protodesc"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/types/descriptorpb"
	"google.golang.org/protobuf/types/dynamicpb"
)

// ---------- inline proto schema helpers ----------

// Shorthand enum pointers used in every descriptor below.
var (
	tBool    = descriptorpb.FieldDescriptorProto_TYPE_BOOL.Enum()
	tInt32   = descriptorpb.FieldDescriptorProto_TYPE_INT32.Enum()
	tInt64   = descriptorpb.FieldDescriptorProto_TYPE_INT64.Enum()
	tUint32  = descriptorpb.FieldDescriptorProto_TYPE_UINT32.Enum()
	tUint64  = descriptorpb.FieldDescriptorProto_TYPE_UINT64.Enum()
	tFloat   = descriptorpb.FieldDescriptorProto_TYPE_FLOAT.Enum()
	tDouble  = descriptorpb.FieldDescriptorProto_TYPE_DOUBLE.Enum()
	tString  = descriptorpb.FieldDescriptorProto_TYPE_STRING.Enum()
	tBytes   = descriptorpb.FieldDescriptorProto_TYPE_BYTES.Enum()
	tMessage = descriptorpb.FieldDescriptorProto_TYPE_MESSAGE.Enum()
	lOpt     = descriptorpb.FieldDescriptorProto_LABEL_OPTIONAL.Enum()
	lRep     = descriptorpb.FieldDescriptorProto_LABEL_REPEATED.Enum()
)

func field(name string, num int32, typ *descriptorpb.FieldDescriptorProto_Type, label *descriptorpb.FieldDescriptorProto_Label, typeName ...string) *descriptorpb.FieldDescriptorProto {
	f := &descriptorpb.FieldDescriptorProto{
		Name:   proto.String(name),
		Number: proto.Int32(num),
		Type:   typ,
		Label:  label,
	}
	if len(typeName) > 0 {
		f.TypeName = proto.String(typeName[0])
	}
	return f
}

// mustNewFile compiles a FileDescriptorProto into a FileDescriptor, failing the test on error.
func mustNewFile(t *testing.T, fdp *descriptorpb.FileDescriptorProto, resolver protodesc.Resolver) protoreflect.FileDescriptor {
	t.Helper()
	fd, err := protodesc.NewFile(fdp, resolver)
	if err != nil {
		t.Fatalf("protodesc.NewFile(%s): %v", fdp.GetName(), err)
	}
	return fd
}

// chainResolver implements protodesc.Resolver by trying each resolver in order.
type chainResolver []protodesc.Resolver

func (c chainResolver) FindFileByPath(path string) (protoreflect.FileDescriptor, error) {
	var lastErr error
	for _, r := range c {
		fd, err := r.FindFileByPath(path)
		if err == nil {
			return fd, nil
		}
		lastErr = err
	}
	return nil, lastErr
}

func (c chainResolver) FindDescriptorByName(name protoreflect.FullName) (protoreflect.Descriptor, error) {
	var lastErr error
	for _, r := range c {
		d, err := r.FindDescriptorByName(name)
		if err == nil {
			return d, nil
		}
		lastErr = err
	}
	return nil, lastErr
}

// ---------- well-known type file descriptors ----------

// buildWKTFiles creates inline file descriptors for all the well-known and
// common protobuf types that the typemap handles, plus a Container message
// that references them all. Everything is built from raw descriptorpb so the
// tests have no dependency on genproto or the global proto registry.
func buildWKTFiles(t *testing.T) protoreflect.MessageDescriptor {
	t.Helper()

	// --- google.protobuf well-known types ---

	timestampFile := mustNewFile(t, &descriptorpb.FileDescriptorProto{
		Name: proto.String("google/protobuf/timestamp.proto"), Package: proto.String("google.protobuf"), Syntax: proto.String("proto3"),
		MessageType: []*descriptorpb.DescriptorProto{{
			Name:  proto.String("Timestamp"),
			Field: []*descriptorpb.FieldDescriptorProto{field("seconds", 1, tInt64, lOpt), field("nanos", 2, tInt32, lOpt)},
		}},
	}, nil)

	durationFile := mustNewFile(t, &descriptorpb.FileDescriptorProto{
		Name: proto.String("google/protobuf/duration.proto"), Package: proto.String("google.protobuf"), Syntax: proto.String("proto3"),
		MessageType: []*descriptorpb.DescriptorProto{{
			Name:  proto.String("Duration"),
			Field: []*descriptorpb.FieldDescriptorProto{field("seconds", 1, tInt64, lOpt), field("nanos", 2, tInt32, lOpt)},
		}},
	}, nil)

	fieldMaskFile := mustNewFile(t, &descriptorpb.FileDescriptorProto{
		Name: proto.String("google/protobuf/field_mask.proto"), Package: proto.String("google.protobuf"), Syntax: proto.String("proto3"),
		MessageType: []*descriptorpb.DescriptorProto{{
			Name:  proto.String("FieldMask"),
			Field: []*descriptorpb.FieldDescriptorProto{field("paths", 1, tString, lRep)},
		}},
	}, nil)

	wrappersFile := mustNewFile(t, &descriptorpb.FileDescriptorProto{
		Name: proto.String("google/protobuf/wrappers.proto"), Package: proto.String("google.protobuf"), Syntax: proto.String("proto3"),
		MessageType: []*descriptorpb.DescriptorProto{
			{Name: proto.String("BoolValue"), Field: []*descriptorpb.FieldDescriptorProto{field("value", 1, tBool, lOpt)}},
			{Name: proto.String("Int32Value"), Field: []*descriptorpb.FieldDescriptorProto{field("value", 1, tInt32, lOpt)}},
			{Name: proto.String("Int64Value"), Field: []*descriptorpb.FieldDescriptorProto{field("value", 1, tInt64, lOpt)}},
			{Name: proto.String("UInt32Value"), Field: []*descriptorpb.FieldDescriptorProto{field("value", 1, tUint32, lOpt)}},
			{Name: proto.String("UInt64Value"), Field: []*descriptorpb.FieldDescriptorProto{field("value", 1, tUint64, lOpt)}},
			{Name: proto.String("FloatValue"), Field: []*descriptorpb.FieldDescriptorProto{field("value", 1, tFloat, lOpt)}},
			{Name: proto.String("DoubleValue"), Field: []*descriptorpb.FieldDescriptorProto{field("value", 1, tDouble, lOpt)}},
			{Name: proto.String("StringValue"), Field: []*descriptorpb.FieldDescriptorProto{field("value", 1, tString, lOpt)}},
			{Name: proto.String("BytesValue"), Field: []*descriptorpb.FieldDescriptorProto{field("value", 1, tBytes, lOpt)}},
		},
	}, nil)

	// --- google.type common types ---

	dateFile := mustNewFile(t, &descriptorpb.FileDescriptorProto{
		Name: proto.String("google/type/date.proto"), Package: proto.String("google.type"), Syntax: proto.String("proto3"),
		MessageType: []*descriptorpb.DescriptorProto{{
			Name: proto.String("Date"),
			Field: []*descriptorpb.FieldDescriptorProto{
				field("year", 1, tInt32, lOpt), field("month", 2, tInt32, lOpt), field("day", 3, tInt32, lOpt),
			},
		}},
	}, nil)

	timeOfDayFile := mustNewFile(t, &descriptorpb.FileDescriptorProto{
		Name: proto.String("google/type/timeofday.proto"), Package: proto.String("google.type"), Syntax: proto.String("proto3"),
		MessageType: []*descriptorpb.DescriptorProto{{
			Name: proto.String("TimeOfDay"),
			Field: []*descriptorpb.FieldDescriptorProto{
				field("hours", 1, tInt32, lOpt), field("minutes", 2, tInt32, lOpt),
				field("seconds", 3, tInt32, lOpt), field("nanos", 4, tInt32, lOpt),
			},
		}},
	}, nil)

	moneyFile := mustNewFile(t, &descriptorpb.FileDescriptorProto{
		Name: proto.String("google/type/money.proto"), Package: proto.String("google.type"), Syntax: proto.String("proto3"),
		MessageType: []*descriptorpb.DescriptorProto{{
			Name: proto.String("Money"),
			Field: []*descriptorpb.FieldDescriptorProto{
				field("currency_code", 1, tString, lOpt), field("units", 2, tInt64, lOpt), field("nanos", 3, tInt32, lOpt),
			},
		}},
	}, nil)

	latLngFile := mustNewFile(t, &descriptorpb.FileDescriptorProto{
		Name: proto.String("google/type/latlng.proto"), Package: proto.String("google.type"), Syntax: proto.String("proto3"),
		MessageType: []*descriptorpb.DescriptorProto{{
			Name: proto.String("LatLng"),
			Field: []*descriptorpb.FieldDescriptorProto{
				field("latitude", 1, tDouble, lOpt), field("longitude", 2, tDouble, lOpt),
			},
		}},
	}, nil)

	colorFile := mustNewFile(t, &descriptorpb.FileDescriptorProto{
		Name: proto.String("google/type/color.proto"), Package: proto.String("google.type"), Syntax: proto.String("proto3"),
		Dependency: []string{"google/protobuf/wrappers.proto"},
		MessageType: []*descriptorpb.DescriptorProto{{
			Name: proto.String("Color"),
			Field: []*descriptorpb.FieldDescriptorProto{
				field("red", 1, tFloat, lOpt), field("green", 2, tFloat, lOpt),
				field("blue", 3, tFloat, lOpt),
				field("alpha", 4, tMessage, lOpt, ".google.protobuf.FloatValue"),
			},
		}},
	}, chainResolver{simpleFiles{wrappersFile}})

	postalAddressFile := mustNewFile(t, &descriptorpb.FileDescriptorProto{
		Name: proto.String("google/type/postal_address.proto"), Package: proto.String("google.type"), Syntax: proto.String("proto3"),
		MessageType: []*descriptorpb.DescriptorProto{{
			Name: proto.String("PostalAddress"),
			Field: []*descriptorpb.FieldDescriptorProto{
				field("revision", 1, tInt32, lOpt),
				field("region_code", 2, tString, lOpt),
				field("language_code", 3, tString, lOpt),
				field("postal_code", 4, tString, lOpt),
				field("sorting_code", 5, tString, lOpt),
				field("administrative_area", 6, tString, lOpt),
				field("locality", 7, tString, lOpt),
				field("sublocality", 8, tString, lOpt),
				field("address_lines", 9, tString, lRep),
				field("recipients", 10, tString, lRep),
				field("organization", 11, tString, lOpt),
			},
		}},
	}, nil)

	intervalFile := mustNewFile(t, &descriptorpb.FileDescriptorProto{
		Name: proto.String("google/type/interval.proto"), Package: proto.String("google.type"), Syntax: proto.String("proto3"),
		Dependency: []string{"google/protobuf/timestamp.proto"},
		MessageType: []*descriptorpb.DescriptorProto{{
			Name: proto.String("Interval"),
			Field: []*descriptorpb.FieldDescriptorProto{
				field("start_time", 1, tMessage, lOpt, ".google.protobuf.Timestamp"),
				field("end_time", 2, tMessage, lOpt, ".google.protobuf.Timestamp"),
			},
		}},
	}, chainResolver{simpleFiles{timestampFile}})

	// --- Container message referencing all of the above ---

	allFiles := simpleFiles{
		timestampFile, durationFile, fieldMaskFile, wrappersFile,
		dateFile, timeOfDayFile, moneyFile, latLngFile,
		colorFile, postalAddressFile, intervalFile,
	}

	containerFile := mustNewFile(t, &descriptorpb.FileDescriptorProto{
		Name:    proto.String("wkt_test.proto"),
		Package: proto.String("wkttest"),
		Syntax:  proto.String("proto3"),
		Dependency: []string{
			"google/protobuf/timestamp.proto",
			"google/protobuf/duration.proto",
			"google/protobuf/field_mask.proto",
			"google/protobuf/wrappers.proto",
			"google/type/date.proto",
			"google/type/timeofday.proto",
			"google/type/money.proto",
			"google/type/latlng.proto",
			"google/type/color.proto",
			"google/type/postal_address.proto",
			"google/type/interval.proto",
		},
		MessageType: []*descriptorpb.DescriptorProto{{
			Name: proto.String("Container"),
			Field: []*descriptorpb.FieldDescriptorProto{
				field("ts", 1, tMessage, lOpt, ".google.protobuf.Timestamp"),
				field("dur", 2, tMessage, lOpt, ".google.protobuf.Duration"),
				field("mask", 3, tMessage, lOpt, ".google.protobuf.FieldMask"),
				field("bool_val", 4, tMessage, lOpt, ".google.protobuf.BoolValue"),
				field("int32_val", 5, tMessage, lOpt, ".google.protobuf.Int32Value"),
				field("int64_val", 6, tMessage, lOpt, ".google.protobuf.Int64Value"),
				field("uint32_val", 7, tMessage, lOpt, ".google.protobuf.UInt32Value"),
				field("uint64_val", 8, tMessage, lOpt, ".google.protobuf.UInt64Value"),
				field("float_val", 9, tMessage, lOpt, ".google.protobuf.FloatValue"),
				field("double_val", 10, tMessage, lOpt, ".google.protobuf.DoubleValue"),
				field("string_val", 11, tMessage, lOpt, ".google.protobuf.StringValue"),
				field("bytes_val", 12, tMessage, lOpt, ".google.protobuf.BytesValue"),
				field("date", 13, tMessage, lOpt, ".google.type.Date"),
				field("tod", 14, tMessage, lOpt, ".google.type.TimeOfDay"),
				field("money", 15, tMessage, lOpt, ".google.type.Money"),
				field("latlng", 16, tMessage, lOpt, ".google.type.LatLng"),
				field("color", 17, tMessage, lOpt, ".google.type.Color"),
				field("address", 18, tMessage, lOpt, ".google.type.PostalAddress"),
				field("interval", 19, tMessage, lOpt, ".google.type.Interval"),
				field("label", 20, tString, lOpt),
			},
		}},
	}, chainResolver{allFiles})

	return containerFile.Messages().ByName("Container")
}

// simpleFiles is a minimal protodesc.Resolver backed by a slice of FileDescriptors.
type simpleFiles []protoreflect.FileDescriptor

func (s simpleFiles) FindFileByPath(path string) (protoreflect.FileDescriptor, error) {
	for _, f := range s {
		if f.Path() == path {
			return f, nil
		}
	}
	return nil, fmt.Errorf("file not found: %s", path)
}

func (s simpleFiles) FindDescriptorByName(name protoreflect.FullName) (protoreflect.Descriptor, error) {
	for _, f := range s {
		if d := findInFile(f, name); d != nil {
			return d, nil
		}
	}
	return nil, fmt.Errorf("descriptor not found: %s", name)
}

func findInFile(fd protoreflect.FileDescriptor, name protoreflect.FullName) protoreflect.Descriptor {
	msgs := fd.Messages()
	for i := 0; i < msgs.Len(); i++ {
		m := msgs.Get(i)
		if m.FullName() == name {
			return m
		}
		// Check nested fields
		fields := m.Fields()
		for j := 0; j < fields.Len(); j++ {
			if fields.Get(j).FullName() == name {
				return fields.Get(j)
			}
		}
	}
	return nil
}

// setSubMsg is a convenience helper that creates a dynamic sub-message,
// calls fn to populate it, and sets it on the parent.
func setSubMsg(parent *dynamicpb.Message, fieldName protoreflect.Name, fn func(m *dynamicpb.Message)) {
	fd := parent.Descriptor().Fields().ByName(fieldName)
	sub := dynamicpb.NewMessage(fd.Message())
	fn(sub)
	parent.Set(fd, protoreflect.ValueOfMessage(sub))
}

// ---------- Tests ----------

func TestDenormDuration(t *testing.T) {
	md := buildWKTFiles(t)
	tc, err := New(md, memory.DefaultAllocator,
		WithDenormalizerPlan(
			pbpath.PlanPath("dur", pbpath.Alias("dur")),
		),
	)
	if err != nil {
		t.Fatal(err)
	}
	defer tc.Release()

	msg := dynamicpb.NewMessage(md)
	setSubMsg(msg, "dur", func(m *dynamicpb.Message) {
		m.Set(m.Descriptor().Fields().ByName("seconds"), protoreflect.ValueOfInt64(90))
		m.Set(m.Descriptor().Fields().ByName("nanos"), protoreflect.ValueOfInt32(500_000_000))
	})

	if err := tc.AppendDenorm(msg); err != nil {
		t.Fatal(err)
	}
	rec := tc.NewDenormalizerRecordBatch()
	defer rec.Release()

	if rec.NumRows() != 1 || rec.NumCols() != 1 {
		t.Fatalf("got %d rows %d cols, want 1 row 1 col", rec.NumRows(), rec.NumCols())
	}

	// 90 seconds + 500ms = 90_500 ms
	got := rec.Column(0).(*array.Duration).Value(0)
	want := arrow.Duration(90_500)
	if got != want {
		t.Errorf("Duration: got %v, want %v", got, want)
	}

	// Verify schema type
	dt := tc.DenormalizerSchema().Field(0).Type
	if _, ok := dt.(*arrow.DurationType); !ok {
		t.Errorf("schema type: got %T, want *arrow.DurationType", dt)
	}
}

func TestDenormFieldMask(t *testing.T) {
	md := buildWKTFiles(t)
	tc, err := New(md, memory.DefaultAllocator,
		WithDenormalizerPlan(
			pbpath.PlanPath("mask", pbpath.Alias("mask")),
		),
	)
	if err != nil {
		t.Fatal(err)
	}
	defer tc.Release()

	msg := dynamicpb.NewMessage(md)
	setSubMsg(msg, "mask", func(m *dynamicpb.Message) {
		paths := m.Mutable(m.Descriptor().Fields().ByName("paths")).List()
		paths.Append(protoreflect.ValueOfString("foo.bar"))
		paths.Append(protoreflect.ValueOfString("baz"))
	})

	if err := tc.AppendDenorm(msg); err != nil {
		t.Fatal(err)
	}
	rec := tc.NewDenormalizerRecordBatch()
	defer rec.Release()

	got := rec.Column(0).(*array.String).Value(0)
	want := "foo.bar,baz"
	if got != want {
		t.Errorf("FieldMask: got %q, want %q", got, want)
	}
}

func TestDenormWrapperTypes(t *testing.T) {
	md := buildWKTFiles(t)
	tc, err := New(md, memory.DefaultAllocator,
		WithDenormalizerPlan(
			pbpath.PlanPath("bool_val"),
			pbpath.PlanPath("int32_val"),
			pbpath.PlanPath("int64_val"),
			pbpath.PlanPath("uint32_val"),
			pbpath.PlanPath("uint64_val"),
			pbpath.PlanPath("float_val"),
			pbpath.PlanPath("double_val"),
			pbpath.PlanPath("string_val"),
			pbpath.PlanPath("bytes_val"),
		),
	)
	if err != nil {
		t.Fatal(err)
	}
	defer tc.Release()

	msg := dynamicpb.NewMessage(md)
	setSubMsg(msg, "bool_val", func(m *dynamicpb.Message) {
		m.Set(m.Descriptor().Fields().ByName("value"), protoreflect.ValueOfBool(true))
	})
	setSubMsg(msg, "int32_val", func(m *dynamicpb.Message) {
		m.Set(m.Descriptor().Fields().ByName("value"), protoreflect.ValueOfInt32(42))
	})
	setSubMsg(msg, "int64_val", func(m *dynamicpb.Message) {
		m.Set(m.Descriptor().Fields().ByName("value"), protoreflect.ValueOfInt64(100))
	})
	setSubMsg(msg, "uint32_val", func(m *dynamicpb.Message) {
		m.Set(m.Descriptor().Fields().ByName("value"), protoreflect.ValueOfUint32(200))
	})
	setSubMsg(msg, "uint64_val", func(m *dynamicpb.Message) {
		m.Set(m.Descriptor().Fields().ByName("value"), protoreflect.ValueOfUint64(300))
	})
	setSubMsg(msg, "float_val", func(m *dynamicpb.Message) {
		m.Set(m.Descriptor().Fields().ByName("value"), protoreflect.ValueOfFloat32(1.5))
	})
	setSubMsg(msg, "double_val", func(m *dynamicpb.Message) {
		m.Set(m.Descriptor().Fields().ByName("value"), protoreflect.ValueOfFloat64(2.5))
	})
	setSubMsg(msg, "string_val", func(m *dynamicpb.Message) {
		m.Set(m.Descriptor().Fields().ByName("value"), protoreflect.ValueOfString("hello"))
	})
	setSubMsg(msg, "bytes_val", func(m *dynamicpb.Message) {
		m.Set(m.Descriptor().Fields().ByName("value"), protoreflect.ValueOfBytes([]byte{0xDE, 0xAD}))
	})

	if err := tc.AppendDenorm(msg); err != nil {
		t.Fatal(err)
	}
	rec := tc.NewDenormalizerRecordBatch()
	defer rec.Release()

	if rec.NumRows() != 1 {
		t.Fatalf("rows: got %d, want 1", rec.NumRows())
	}

	check := func(col int, want interface{}) {
		t.Helper()
		switch w := want.(type) {
		case bool:
			if got := rec.Column(col).(*array.Boolean).Value(0); got != w {
				t.Errorf("col %d: got %v, want %v", col, got, w)
			}
		case int32:
			if got := rec.Column(col).(*array.Int32).Value(0); got != w {
				t.Errorf("col %d: got %v, want %v", col, got, w)
			}
		case int64:
			if got := rec.Column(col).(*array.Int64).Value(0); got != w {
				t.Errorf("col %d: got %v, want %v", col, got, w)
			}
		case uint32:
			if got := rec.Column(col).(*array.Uint32).Value(0); got != w {
				t.Errorf("col %d: got %v, want %v", col, got, w)
			}
		case uint64:
			if got := rec.Column(col).(*array.Uint64).Value(0); got != w {
				t.Errorf("col %d: got %v, want %v", col, got, w)
			}
		case float32:
			if got := rec.Column(col).(*array.Float32).Value(0); got != w {
				t.Errorf("col %d: got %v, want %v", col, got, w)
			}
		case float64:
			if got := rec.Column(col).(*array.Float64).Value(0); got != w {
				t.Errorf("col %d: got %v, want %v", col, got, w)
			}
		case string:
			if got := rec.Column(col).(*array.String).Value(0); got != w {
				t.Errorf("col %d: got %q, want %q", col, got, w)
			}
		case []byte:
			if got := rec.Column(col).(*array.Binary).Value(0); string(got) != string(w) {
				t.Errorf("col %d: got %x, want %x", col, got, w)
			}
		}
	}

	check(0, true)
	check(1, int32(42))
	check(2, int64(100))
	check(3, uint32(200))
	check(4, uint64(300))
	check(5, float32(1.5))
	check(6, float64(2.5))
	check(7, "hello")
	check(8, []byte{0xDE, 0xAD})
}

func TestDenormWrapperSchemaTypes(t *testing.T) {
	md := buildWKTFiles(t)
	tc, err := New(md, memory.DefaultAllocator,
		WithDenormalizerPlan(
			pbpath.PlanPath("bool_val"),
			pbpath.PlanPath("int32_val"),
			pbpath.PlanPath("int64_val"),
			pbpath.PlanPath("uint32_val"),
			pbpath.PlanPath("uint64_val"),
			pbpath.PlanPath("float_val"),
			pbpath.PlanPath("double_val"),
			pbpath.PlanPath("string_val"),
			pbpath.PlanPath("bytes_val"),
		),
	)
	if err != nil {
		t.Fatal(err)
	}
	defer tc.Release()

	schema := tc.DenormalizerSchema()
	tests := []struct {
		col  int
		want arrow.DataType
	}{
		{0, arrow.FixedWidthTypes.Boolean},
		{1, arrow.PrimitiveTypes.Int32},
		{2, arrow.PrimitiveTypes.Int64},
		{3, arrow.PrimitiveTypes.Uint32},
		{4, arrow.PrimitiveTypes.Uint64},
		{5, arrow.PrimitiveTypes.Float32},
		{6, arrow.PrimitiveTypes.Float64},
		{7, arrow.BinaryTypes.String},
		{8, arrow.BinaryTypes.Binary},
	}
	for _, tt := range tests {
		got := schema.Field(tt.col).Type
		if got.ID() != tt.want.ID() {
			t.Errorf("col %d: got %s, want %s", tt.col, got, tt.want)
		}
	}
}

func TestDenormDate(t *testing.T) {
	md := buildWKTFiles(t)
	tc, err := New(md, memory.DefaultAllocator,
		WithDenormalizerPlan(
			pbpath.PlanPath("date", pbpath.Alias("date")),
		),
	)
	if err != nil {
		t.Fatal(err)
	}
	defer tc.Release()

	msg := dynamicpb.NewMessage(md)
	setSubMsg(msg, "date", func(m *dynamicpb.Message) {
		m.Set(m.Descriptor().Fields().ByName("year"), protoreflect.ValueOfInt32(2025))
		m.Set(m.Descriptor().Fields().ByName("month"), protoreflect.ValueOfInt32(3))
		m.Set(m.Descriptor().Fields().ByName("day"), protoreflect.ValueOfInt32(25))
	})

	if err := tc.AppendDenorm(msg); err != nil {
		t.Fatal(err)
	}
	rec := tc.NewDenormalizerRecordBatch()
	defer rec.Release()

	got := rec.Column(0).(*array.Date32).Value(0)
	want := arrow.Date32FromTime(time.Date(2025, 3, 25, 0, 0, 0, 0, time.UTC))
	if got != want {
		t.Errorf("Date: got %v, want %v", got, want)
	}

	// Verify schema type
	if schema := tc.DenormalizerSchema(); schema.Field(0).Type.ID() != arrow.DATE32 {
		t.Errorf("schema type: got %s, want date32", schema.Field(0).Type)
	}
}

func TestDenormTimeOfDay(t *testing.T) {
	md := buildWKTFiles(t)
	tc, err := New(md, memory.DefaultAllocator,
		WithDenormalizerPlan(
			pbpath.PlanPath("tod", pbpath.Alias("tod")),
		),
	)
	if err != nil {
		t.Fatal(err)
	}
	defer tc.Release()

	msg := dynamicpb.NewMessage(md)
	setSubMsg(msg, "tod", func(m *dynamicpb.Message) {
		m.Set(m.Descriptor().Fields().ByName("hours"), protoreflect.ValueOfInt32(14))
		m.Set(m.Descriptor().Fields().ByName("minutes"), protoreflect.ValueOfInt32(30))
		m.Set(m.Descriptor().Fields().ByName("seconds"), protoreflect.ValueOfInt32(15))
		m.Set(m.Descriptor().Fields().ByName("nanos"), protoreflect.ValueOfInt32(500_000))
	})

	if err := tc.AppendDenorm(msg); err != nil {
		t.Fatal(err)
	}
	rec := tc.NewDenormalizerRecordBatch()
	defer rec.Release()

	got := rec.Column(0).(*array.Time64).Value(0)
	// 14*3600+30*60+15 = 52215 seconds → 52215_000_000 µs + 500 µs = 52215_000_500
	want := arrow.Time64(52215_000_500)
	if got != want {
		t.Errorf("TimeOfDay: got %v, want %v", got, want)
	}

	// Verify schema type
	dt := tc.DenormalizerSchema().Field(0).Type
	if _, ok := dt.(*arrow.Time64Type); !ok {
		t.Errorf("schema type: got %T, want *arrow.Time64Type", dt)
	}
}

func TestDenormMoney(t *testing.T) {
	md := buildWKTFiles(t)
	tc, err := New(md, memory.DefaultAllocator,
		WithDenormalizerPlan(
			pbpath.PlanPath("money", pbpath.Alias("money")),
		),
	)
	if err != nil {
		t.Fatal(err)
	}
	defer tc.Release()

	msg := dynamicpb.NewMessage(md)
	setSubMsg(msg, "money", func(m *dynamicpb.Message) {
		m.Set(m.Descriptor().Fields().ByName("currency_code"), protoreflect.ValueOfString("USD"))
		m.Set(m.Descriptor().Fields().ByName("units"), protoreflect.ValueOfInt64(42))
		m.Set(m.Descriptor().Fields().ByName("nanos"), protoreflect.ValueOfInt32(500_000_000))
	})

	if err := tc.AppendDenorm(msg); err != nil {
		t.Fatal(err)
	}
	rec := tc.NewDenormalizerRecordBatch()
	defer rec.Release()

	got := rec.Column(0).(*array.String).Value(0)
	// Should be valid JSON containing the fields
	var parsed map[string]interface{}
	if err := json.Unmarshal([]byte(got), &parsed); err != nil {
		t.Fatalf("Money value is not valid JSON: %v\ngot: %s", err, got)
	}
	if cc, ok := parsed["currencyCode"].(string); !ok || cc != "USD" {
		t.Errorf("Money currencyCode: got %v from JSON %s", parsed["currencyCode"], got)
	}
}

func TestDenormLatLng(t *testing.T) {
	md := buildWKTFiles(t)
	tc, err := New(md, memory.DefaultAllocator,
		WithDenormalizerPlan(
			pbpath.PlanPath("latlng", pbpath.Alias("latlng")),
		),
	)
	if err != nil {
		t.Fatal(err)
	}
	defer tc.Release()

	msg := dynamicpb.NewMessage(md)
	setSubMsg(msg, "latlng", func(m *dynamicpb.Message) {
		m.Set(m.Descriptor().Fields().ByName("latitude"), protoreflect.ValueOfFloat64(37.386051))
		m.Set(m.Descriptor().Fields().ByName("longitude"), protoreflect.ValueOfFloat64(-122.083855))
	})

	if err := tc.AppendDenorm(msg); err != nil {
		t.Fatal(err)
	}
	rec := tc.NewDenormalizerRecordBatch()
	defer rec.Release()

	got := rec.Column(0).(*array.String).Value(0)
	var parsed map[string]interface{}
	if err := json.Unmarshal([]byte(got), &parsed); err != nil {
		t.Fatalf("LatLng value is not valid JSON: %v\ngot: %s", err, got)
	}
	if lat, ok := parsed["latitude"].(float64); !ok || lat != 37.386051 {
		t.Errorf("LatLng latitude: got %v from JSON %s", parsed["latitude"], got)
	}
}

func TestDenormPostalAddress(t *testing.T) {
	md := buildWKTFiles(t)
	tc, err := New(md, memory.DefaultAllocator,
		WithDenormalizerPlan(
			pbpath.PlanPath("address", pbpath.Alias("address")),
		),
	)
	if err != nil {
		t.Fatal(err)
	}
	defer tc.Release()

	msg := dynamicpb.NewMessage(md)
	setSubMsg(msg, "address", func(m *dynamicpb.Message) {
		m.Set(m.Descriptor().Fields().ByName("region_code"), protoreflect.ValueOfString("US"))
		m.Set(m.Descriptor().Fields().ByName("postal_code"), protoreflect.ValueOfString("94043"))
		m.Set(m.Descriptor().Fields().ByName("locality"), protoreflect.ValueOfString("Mountain View"))
		lines := m.Mutable(m.Descriptor().Fields().ByName("address_lines")).List()
		lines.Append(protoreflect.ValueOfString("1600 Amphitheatre Parkway"))
	})

	if err := tc.AppendDenorm(msg); err != nil {
		t.Fatal(err)
	}
	rec := tc.NewDenormalizerRecordBatch()
	defer rec.Release()

	got := rec.Column(0).(*array.String).Value(0)
	var parsed map[string]interface{}
	if err := json.Unmarshal([]byte(got), &parsed); err != nil {
		t.Fatalf("PostalAddress value is not valid JSON: %v\ngot: %s", err, got)
	}
	if rc, ok := parsed["regionCode"].(string); !ok || rc != "US" {
		t.Errorf("PostalAddress regionCode: got %v from JSON %s", parsed["regionCode"], got)
	}
}

func TestDenormInterval(t *testing.T) {
	md := buildWKTFiles(t)
	tc, err := New(md, memory.DefaultAllocator,
		WithDenormalizerPlan(
			pbpath.PlanPath("interval", pbpath.Alias("interval")),
		),
	)
	if err != nil {
		t.Fatal(err)
	}
	defer tc.Release()

	msg := dynamicpb.NewMessage(md)
	setSubMsg(msg, "interval", func(m *dynamicpb.Message) {
		setSubMsg(m, "start_time", func(ts *dynamicpb.Message) {
			ts.Set(ts.Descriptor().Fields().ByName("seconds"), protoreflect.ValueOfInt64(1000))
		})
		setSubMsg(m, "end_time", func(ts *dynamicpb.Message) {
			ts.Set(ts.Descriptor().Fields().ByName("seconds"), protoreflect.ValueOfInt64(2000))
		})
	})

	if err := tc.AppendDenorm(msg); err != nil {
		t.Fatal(err)
	}
	rec := tc.NewDenormalizerRecordBatch()
	defer rec.Release()

	got := rec.Column(0).(*array.String).Value(0)
	var parsed map[string]interface{}
	if err := json.Unmarshal([]byte(got), &parsed); err != nil {
		t.Fatalf("Interval value is not valid JSON: %v\ngot: %s", err, got)
	}
	if _, ok := parsed["startTime"]; !ok {
		t.Errorf("Interval missing startTime in JSON %s", got)
	}
}

func TestDenormColor(t *testing.T) {
	md := buildWKTFiles(t)
	tc, err := New(md, memory.DefaultAllocator,
		WithDenormalizerPlan(
			pbpath.PlanPath("color", pbpath.Alias("color")),
		),
	)
	if err != nil {
		t.Fatal(err)
	}
	defer tc.Release()

	msg := dynamicpb.NewMessage(md)
	setSubMsg(msg, "color", func(m *dynamicpb.Message) {
		m.Set(m.Descriptor().Fields().ByName("red"), protoreflect.ValueOfFloat32(1.0))
		m.Set(m.Descriptor().Fields().ByName("green"), protoreflect.ValueOfFloat32(0.5))
		m.Set(m.Descriptor().Fields().ByName("blue"), protoreflect.ValueOfFloat32(0.0))
	})

	if err := tc.AppendDenorm(msg); err != nil {
		t.Fatal(err)
	}
	rec := tc.NewDenormalizerRecordBatch()
	defer rec.Release()

	got := rec.Column(0).(*array.String).Value(0)
	var parsed map[string]interface{}
	if err := json.Unmarshal([]byte(got), &parsed); err != nil {
		t.Fatalf("Color value is not valid JSON: %v\ngot: %s", err, got)
	}
	if red, ok := parsed["red"].(float64); !ok || red != 1.0 {
		t.Errorf("Color red: got %v from JSON %s", parsed["red"], got)
	}
}

func TestDenormAllWKTSchemaTypes(t *testing.T) {
	md := buildWKTFiles(t)
	tc, err := New(md, memory.DefaultAllocator,
		WithDenormalizerPlan(
			pbpath.PlanPath("ts"),       // 0: Timestamp
			pbpath.PlanPath("dur"),      // 1: Duration
			pbpath.PlanPath("mask"),     // 2: FieldMask → String
			pbpath.PlanPath("date"),     // 3: Date → Date32
			pbpath.PlanPath("tod"),      // 4: TimeOfDay → Time64
			pbpath.PlanPath("money"),    // 5: Money → String
			pbpath.PlanPath("latlng"),   // 6: LatLng → String
			pbpath.PlanPath("color"),    // 7: Color → String
			pbpath.PlanPath("address"),  // 8: PostalAddress → String
			pbpath.PlanPath("interval"), // 9: Interval → String
			pbpath.PlanPath("label"),    // 10: plain string
		),
	)
	if err != nil {
		t.Fatal(err)
	}
	defer tc.Release()

	schema := tc.DenormalizerSchema()
	tests := []struct {
		col  int
		name string
		want arrow.Type
	}{
		{0, "ts", arrow.TIMESTAMP},
		{1, "dur", arrow.DURATION},
		{2, "mask", arrow.STRING},
		{3, "date", arrow.DATE32},
		{4, "tod", arrow.TIME64},
		{5, "money", arrow.STRING},
		{6, "latlng", arrow.STRING},
		{7, "color", arrow.STRING},
		{8, "address", arrow.STRING},
		{9, "interval", arrow.STRING},
		{10, "label", arrow.STRING},
	}
	for _, tt := range tests {
		f := schema.Field(tt.col)
		if f.Name != tt.name {
			t.Errorf("col %d: name got %q, want %q", tt.col, f.Name, tt.name)
		}
		if f.Type.ID() != tt.want {
			t.Errorf("col %d (%s): type got %s, want %s", tt.col, tt.name, f.Type, tt.want)
		}
		if !f.Nullable {
			t.Errorf("col %d (%s): expected nullable", tt.col, tt.name)
		}
	}
}
