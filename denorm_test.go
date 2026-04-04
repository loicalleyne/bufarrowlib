package bufarrowlib

import (
	"path/filepath"
	"runtime"
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/loicalleyne/bufarrowlib/proto/pbpath"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protodesc"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/types/descriptorpb"
	"google.golang.org/protobuf/types/dynamicpb"
)

// ---------- helpers ----------

// buildDenormTestSchema constructs an inline proto schema for denormalizer tests:
//
//	message Item {
//	  string id    = 1;
//	  double price = 2;
//	}
//	message Order {
//	  string   name   = 1;
//	  repeated Item   items = 2;
//	  repeated string tags  = 3;
//	  int64    seq    = 4;
//	}
func buildDenormTestSchema(t *testing.T) (orderMD, itemMD protoreflect.MessageDescriptor) {
	t.Helper()
	stringType := descriptorpb.FieldDescriptorProto_TYPE_STRING.Enum()
	doubleType := descriptorpb.FieldDescriptorProto_TYPE_DOUBLE.Enum()
	int64Type := descriptorpb.FieldDescriptorProto_TYPE_INT64.Enum()
	messageType := descriptorpb.FieldDescriptorProto_TYPE_MESSAGE.Enum()
	labelOpt := descriptorpb.FieldDescriptorProto_LABEL_OPTIONAL.Enum()
	labelRep := descriptorpb.FieldDescriptorProto_LABEL_REPEATED.Enum()

	fdp := &descriptorpb.FileDescriptorProto{
		Name:    proto.String("denorm_test.proto"),
		Package: proto.String("denormtest"),
		Syntax:  proto.String("proto3"),
		MessageType: []*descriptorpb.DescriptorProto{
			{
				Name: proto.String("Item"),
				Field: []*descriptorpb.FieldDescriptorProto{
					{Name: proto.String("id"), Number: proto.Int32(1), Type: stringType, Label: labelOpt},
					{Name: proto.String("price"), Number: proto.Int32(2), Type: doubleType, Label: labelOpt},
				},
			},
			{
				Name: proto.String("Order"),
				Field: []*descriptorpb.FieldDescriptorProto{
					{Name: proto.String("name"), Number: proto.Int32(1), Type: stringType, Label: labelOpt},
					{Name: proto.String("items"), Number: proto.Int32(2), Type: messageType, TypeName: proto.String(".denormtest.Item"), Label: labelRep},
					{Name: proto.String("tags"), Number: proto.Int32(3), Type: stringType, Label: labelRep},
					{Name: proto.String("seq"), Number: proto.Int32(4), Type: int64Type, Label: labelOpt},
				},
			},
		},
	}
	fd, err := protodesc.NewFile(fdp, nil)
	if err != nil {
		t.Fatalf("protodesc.NewFile: %v", err)
	}
	orderMD = fd.Messages().ByName("Order")
	itemMD = fd.Messages().ByName("Item")
	return
}

func makeOrder(t *testing.T, orderMD, itemMD protoreflect.MessageDescriptor, name string, items []struct {
	id    string
	price float64
}, tags []string, seq int64) proto.Message {
	t.Helper()
	msg := dynamicpb.NewMessage(orderMD)
	msg.Set(orderMD.Fields().ByName("name"), protoreflect.ValueOfString(name))
	msg.Set(orderMD.Fields().ByName("seq"), protoreflect.ValueOfInt64(seq))

	list := msg.Mutable(orderMD.Fields().ByName("items")).List()
	for _, it := range items {
		item := dynamicpb.NewMessage(itemMD)
		item.Set(itemMD.Fields().ByName("id"), protoreflect.ValueOfString(it.id))
		item.Set(itemMD.Fields().ByName("price"), protoreflect.ValueOfFloat64(it.price))
		list.Append(protoreflect.ValueOfMessage(item))
	}

	tagList := msg.Mutable(orderMD.Fields().ByName("tags")).List()
	for _, tg := range tags {
		tagList.Append(protoreflect.ValueOfString(tg))
	}

	return msg
}

// ---------- tests ----------

func TestDenormSingleScalar(t *testing.T) {
	orderMD, itemMD := buildDenormTestSchema(t)
	tc, err := New(orderMD, memory.DefaultAllocator,
		WithDenormalizerPlan(
			pbpath.PlanPath("name", pbpath.Alias("order_name")),
			pbpath.PlanPath("seq", pbpath.Alias("order_seq")),
		),
	)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	defer tc.Release()

	msg := makeOrder(t, orderMD, itemMD, "test", nil, nil, 42)
	if err := tc.AppendDenorm(msg); err != nil {
		t.Fatalf("AppendDenorm: %v", err)
	}

	rec := tc.NewDenormalizerRecordBatch()
	defer rec.Release()

	if rec.NumRows() != 1 {
		t.Fatalf("expected 1 row, got %d", rec.NumRows())
	}
	if rec.NumCols() != 2 {
		t.Fatalf("expected 2 cols, got %d", rec.NumCols())
	}
	// Check column names
	if rec.ColumnName(0) != "order_name" {
		t.Errorf("col 0 name = %q, want %q", rec.ColumnName(0), "order_name")
	}
	if rec.ColumnName(1) != "order_seq" {
		t.Errorf("col 1 name = %q, want %q", rec.ColumnName(1), "order_seq")
	}
}

func TestDenormWildcardFanout(t *testing.T) {
	orderMD, itemMD := buildDenormTestSchema(t)
	tc, err := New(orderMD, memory.DefaultAllocator,
		WithDenormalizerPlan(
			pbpath.PlanPath("name", pbpath.Alias("order_name")),
			pbpath.PlanPath("items[*].id", pbpath.Alias("item_id")),
			pbpath.PlanPath("items[*].price", pbpath.Alias("item_price")),
		),
	)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	defer tc.Release()

	items := []struct {
		id    string
		price float64
	}{
		{"A", 1.0},
		{"B", 2.0},
		{"C", 3.0},
	}
	msg := makeOrder(t, orderMD, itemMD, "order1", items, nil, 1)
	if err := tc.AppendDenorm(msg); err != nil {
		t.Fatalf("AppendDenorm: %v", err)
	}

	rec := tc.NewDenormalizerRecordBatch()
	defer rec.Release()

	if rec.NumRows() != 3 {
		t.Fatalf("expected 3 rows, got %d", rec.NumRows())
	}
	if rec.NumCols() != 3 {
		t.Fatalf("expected 3 cols, got %d", rec.NumCols())
	}
}

func TestDenormCrossJoin(t *testing.T) {
	orderMD, itemMD := buildDenormTestSchema(t)
	tc, err := New(orderMD, memory.DefaultAllocator,
		WithDenormalizerPlan(
			pbpath.PlanPath("name", pbpath.Alias("order_name")),
			pbpath.PlanPath("items[*].id", pbpath.Alias("item_id")),
			pbpath.PlanPath("tags[*]", pbpath.Alias("tag")),
		),
	)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	defer tc.Release()

	items := []struct {
		id    string
		price float64
	}{
		{"A", 1.0},
		{"B", 2.0},
	}
	msg := makeOrder(t, orderMD, itemMD, "order1", items, []string{"x", "y", "z"}, 1)
	if err := tc.AppendDenorm(msg); err != nil {
		t.Fatalf("AppendDenorm: %v", err)
	}

	rec := tc.NewDenormalizerRecordBatch()
	defer rec.Release()

	// 2 items × 3 tags = 6 rows
	if rec.NumRows() != 6 {
		t.Fatalf("expected 6 rows (2 items × 3 tags), got %d", rec.NumRows())
	}
}

func TestDenormLeftJoinEmptyRepeated(t *testing.T) {
	orderMD, itemMD := buildDenormTestSchema(t)
	tc, err := New(orderMD, memory.DefaultAllocator,
		WithDenormalizerPlan(
			pbpath.PlanPath("name", pbpath.Alias("order_name")),
			pbpath.PlanPath("items[*].id", pbpath.Alias("item_id")),
			pbpath.PlanPath("tags[*]", pbpath.Alias("tag")),
		),
	)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	defer tc.Release()

	// 0 items, 2 tags → items group is null → 1×2 = 2 rows
	msg := makeOrder(t, orderMD, itemMD, "order1", nil, []string{"x", "y"}, 1)
	if err := tc.AppendDenorm(msg); err != nil {
		t.Fatalf("AppendDenorm: %v", err)
	}

	rec := tc.NewDenormalizerRecordBatch()
	defer rec.Release()

	if rec.NumRows() != 2 {
		t.Fatalf("expected 2 rows (1 null-item × 2 tags), got %d", rec.NumRows())
	}

	// item_id column (index 1) should be all null
	col := rec.Column(1)
	for i := 0; i < int(rec.NumRows()); i++ {
		if !col.IsNull(i) {
			t.Errorf("row %d: item_id should be null", i)
		}
	}
}

func TestDenormLeftJoinBothEmpty(t *testing.T) {
	orderMD, itemMD := buildDenormTestSchema(t)
	tc, err := New(orderMD, memory.DefaultAllocator,
		WithDenormalizerPlan(
			pbpath.PlanPath("name", pbpath.Alias("order_name")),
			pbpath.PlanPath("items[*].id", pbpath.Alias("item_id")),
			pbpath.PlanPath("tags[*]", pbpath.Alias("tag")),
		),
	)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	defer tc.Release()

	// Both empty → 1×1 = 1 row, item_id and tag both null
	msg := makeOrder(t, orderMD, itemMD, "order1", nil, nil, 1)
	if err := tc.AppendDenorm(msg); err != nil {
		t.Fatalf("AppendDenorm: %v", err)
	}

	rec := tc.NewDenormalizerRecordBatch()
	defer rec.Release()

	if rec.NumRows() != 1 {
		t.Fatalf("expected 1 row, got %d", rec.NumRows())
	}
	// item_id (1) and tag (2) should be null
	if !rec.Column(1).IsNull(0) {
		t.Error("item_id should be null")
	}
	if !rec.Column(2).IsNull(0) {
		t.Error("tag should be null")
	}
	// name (0) should NOT be null
	if rec.Column(0).IsNull(0) {
		t.Error("name should not be null")
	}
}

func TestDenormListIndexBroadcast(t *testing.T) {
	orderMD, itemMD := buildDenormTestSchema(t)
	tc, err := New(orderMD, memory.DefaultAllocator,
		WithDenormalizerPlan(
			pbpath.PlanPath("items[0].id", pbpath.Alias("first_item_id")),
			pbpath.PlanPath("tags[*]", pbpath.Alias("tag")),
		),
	)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	defer tc.Release()

	items := []struct {
		id    string
		price float64
	}{
		{"A", 1.0},
		{"B", 2.0},
	}
	msg := makeOrder(t, orderMD, itemMD, "order1", items, []string{"x", "y", "z"}, 1)
	if err := tc.AppendDenorm(msg); err != nil {
		t.Fatalf("AppendDenorm: %v", err)
	}

	rec := tc.NewDenormalizerRecordBatch()
	defer rec.Release()

	// items[0] is scalar (ListIndexStep excluded from fan-out sig)
	// tags[*] is 3 branches → 1×3 = 3 rows, items[0].id broadcast to all
	if rec.NumRows() != 3 {
		t.Fatalf("expected 3 rows (items[0] broadcast × 3 tags), got %d", rec.NumRows())
	}
}

func TestDenormRangeSlice(t *testing.T) {
	orderMD, itemMD := buildDenormTestSchema(t)
	tc, err := New(orderMD, memory.DefaultAllocator,
		WithDenormalizerPlan(
			pbpath.PlanPath("items[0:2].id", pbpath.Alias("item_id")),
		),
	)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	defer tc.Release()

	items := []struct {
		id    string
		price float64
	}{
		{"A", 1.0}, {"B", 2.0}, {"C", 3.0},
	}
	msg := makeOrder(t, orderMD, itemMD, "order1", items, nil, 1)
	if err := tc.AppendDenorm(msg); err != nil {
		t.Fatalf("AppendDenorm: %v", err)
	}

	rec := tc.NewDenormalizerRecordBatch()
	defer rec.Release()

	// [0:2] selects first 2 of 3
	if rec.NumRows() != 2 {
		t.Fatalf("expected 2 rows, got %d", rec.NumRows())
	}
}

func TestDenormNegativeIndex(t *testing.T) {
	orderMD, itemMD := buildDenormTestSchema(t)
	tc, err := New(orderMD, memory.DefaultAllocator,
		WithDenormalizerPlan(
			pbpath.PlanPath("items[-1].id", pbpath.Alias("last_item_id")),
		),
	)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	defer tc.Release()

	items := []struct {
		id    string
		price float64
	}{
		{"A", 1.0}, {"B", 2.0}, {"C", 3.0},
	}
	msg := makeOrder(t, orderMD, itemMD, "order1", items, nil, 1)
	if err := tc.AppendDenorm(msg); err != nil {
		t.Fatalf("AppendDenorm: %v", err)
	}

	rec := tc.NewDenormalizerRecordBatch()
	defer rec.Release()

	if rec.NumRows() != 1 {
		t.Fatalf("expected 1 row, got %d", rec.NumRows())
	}
}

func TestDenormAlias(t *testing.T) {
	orderMD, _ := buildDenormTestSchema(t)
	tc, err := New(orderMD, memory.DefaultAllocator,
		WithDenormalizerPlan(
			pbpath.PlanPath("name", pbpath.Alias("custom_name")),
		),
	)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	defer tc.Release()

	schema := tc.DenormalizerSchema()
	if schema == nil {
		t.Fatal("DenormalizerSchema returned nil")
	}
	if schema.Field(0).Name != "custom_name" {
		t.Errorf("field name = %q, want %q", schema.Field(0).Name, "custom_name")
	}
}

func TestDenormMessageLeafRejected(t *testing.T) {
	orderMD, _ := buildDenormTestSchema(t)
	// items is a message type without drilling into a scalar field
	_, err := New(orderMD, memory.DefaultAllocator,
		WithDenormalizerPlan(
			pbpath.PlanPath("items[*]"),
		),
	)
	if err == nil {
		t.Fatal("expected error for message-typed leaf, got nil")
	}
}

func TestDenormStrictPath(t *testing.T) {
	orderMD, itemMD := buildDenormTestSchema(t)
	tc, err := New(orderMD, memory.DefaultAllocator,
		WithDenormalizerPlan(
			pbpath.PlanPath("items[0:10].id", pbpath.Alias("item_id"), pbpath.StrictPath()),
		),
	)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	defer tc.Release()

	// Only 2 items but range asks for 0:10 → should error with strict
	items := []struct {
		id    string
		price float64
	}{
		{"A", 1.0}, {"B", 2.0},
	}
	msg := makeOrder(t, orderMD, itemMD, "order1", items, nil, 1)
	err = tc.AppendDenorm(msg)
	if err == nil {
		t.Fatal("expected strict path error, got nil")
	}
}

func TestDenormClone(t *testing.T) {
	orderMD, itemMD := buildDenormTestSchema(t)
	tc, err := New(orderMD, memory.DefaultAllocator,
		WithDenormalizerPlan(
			pbpath.PlanPath("name", pbpath.Alias("order_name")),
			pbpath.PlanPath("items[*].id", pbpath.Alias("item_id")),
		),
	)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	defer tc.Release()

	tc2, err := tc.Clone(memory.DefaultAllocator)
	if err != nil {
		t.Fatalf("Clone: %v", err)
	}
	defer tc2.Release()

	if tc2.DenormalizerSchema() == nil {
		t.Fatal("cloned transcoder has nil DenormalizerSchema")
	}
	if tc2.DenormalizerBuilder() == nil {
		t.Fatal("cloned transcoder has nil DenormalizerBuilder")
	}

	// Append to cloned and verify
	items := []struct {
		id    string
		price float64
	}{{"X", 9.0}}
	msg := makeOrder(t, orderMD, itemMD, "clone_test", items, nil, 1)
	if err := tc2.AppendDenorm(msg); err != nil {
		t.Fatalf("AppendDenorm on clone: %v", err)
	}
	rec := tc2.NewDenormalizerRecordBatch()
	defer rec.Release()
	if rec.NumRows() != 1 {
		t.Fatalf("expected 1 row from clone, got %d", rec.NumRows())
	}
}

func TestDenormMultipleMessages(t *testing.T) {
	orderMD, itemMD := buildDenormTestSchema(t)
	tc, err := New(orderMD, memory.DefaultAllocator,
		WithDenormalizerPlan(
			pbpath.PlanPath("name", pbpath.Alias("order_name")),
			pbpath.PlanPath("items[*].id", pbpath.Alias("item_id")),
		),
	)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	defer tc.Release()

	// First message: 2 items
	items1 := []struct {
		id    string
		price float64
	}{{"A", 1.0}, {"B", 2.0}}
	msg1 := makeOrder(t, orderMD, itemMD, "order1", items1, nil, 1)
	if err := tc.AppendDenorm(msg1); err != nil {
		t.Fatalf("AppendDenorm msg1: %v", err)
	}

	// Second message: 1 item
	items2 := []struct {
		id    string
		price float64
	}{{"C", 3.0}}
	msg2 := makeOrder(t, orderMD, itemMD, "order2", items2, nil, 2)
	if err := tc.AppendDenorm(msg2); err != nil {
		t.Fatalf("AppendDenorm msg2: %v", err)
	}

	rec := tc.NewDenormalizerRecordBatch()
	defer rec.Release()

	// 2 + 1 = 3 rows
	if rec.NumRows() != 3 {
		t.Fatalf("expected 3 rows, got %d", rec.NumRows())
	}
}

func TestDenormTimestamp(t *testing.T) {
	// Use the real Known message from samples.proto which has google.protobuf.Timestamp
	_, f, _, ok := runtime.Caller(0)
	if !ok {
		t.Fatal("unable to determine test file path")
	}
	protoDir := filepath.Join(filepath.Dir(f), "proto", "samples")
	tc, err := NewFromFile("samples.proto", "Known", []string{protoDir}, memory.DefaultAllocator,
		WithDenormalizerPlan(
			pbpath.PlanPath("ts", pbpath.Alias("timestamp")),
		),
	)
	if err != nil {
		t.Fatalf("NewFromFile: %v", err)
	}
	defer tc.Release()

	schema := tc.DenormalizerSchema()
	if schema == nil {
		t.Fatal("no denorm schema")
	}
	tsField := schema.Field(0)
	if tsField.Type.ID() != arrow.TIMESTAMP {
		t.Fatalf("expected Timestamp type, got %v", tsField.Type)
	}
	tsType := tsField.Type.(*arrow.TimestampType)
	if tsType.Unit != arrow.Millisecond {
		t.Errorf("expected Millisecond, got %v", tsType.Unit)
	}
	if tsType.TimeZone != "UTC" {
		t.Errorf("expected UTC timezone, got %q", tsType.TimeZone)
	}
}

func TestDenormNoPlan(t *testing.T) {
	orderMD, itemMD := buildDenormTestSchema(t)
	tc, err := New(orderMD, memory.DefaultAllocator)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	defer tc.Release()

	if tc.DenormalizerSchema() != nil {
		t.Error("expected nil DenormalizerSchema without plan")
	}
	if tc.DenormalizerBuilder() != nil {
		t.Error("expected nil DenormalizerBuilder without plan")
	}
	if tc.NewDenormalizerRecordBatch() != nil {
		t.Error("expected nil NewDenormalizerRecordBatch without plan")
	}

	msg := makeOrder(t, orderMD, itemMD, "test", nil, nil, 1)
	err = tc.AppendDenorm(msg)
	if err == nil {
		t.Error("expected error from AppendDenorm without plan")
	}
}

func TestDenormSchemaFieldTypes(t *testing.T) {
	orderMD, _ := buildDenormTestSchema(t)
	tc, err := New(orderMD, memory.DefaultAllocator,
		WithDenormalizerPlan(
			pbpath.PlanPath("name", pbpath.Alias("order_name")),
			pbpath.PlanPath("seq", pbpath.Alias("order_seq")),
			pbpath.PlanPath("items[*].price", pbpath.Alias("item_price")),
		),
	)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	defer tc.Release()

	schema := tc.DenormalizerSchema()
	tests := []struct {
		idx  int
		name string
		typ  arrow.DataType
	}{
		{0, "order_name", arrow.BinaryTypes.String},
		{1, "order_seq", arrow.PrimitiveTypes.Int64},
		{2, "item_price", arrow.PrimitiveTypes.Float64},
	}
	for _, tt := range tests {
		f := schema.Field(tt.idx)
		if f.Name != tt.name {
			t.Errorf("field %d: name = %q, want %q", tt.idx, f.Name, tt.name)
		}
		if f.Type.ID() != tt.typ.ID() {
			t.Errorf("field %d (%s): type = %v, want %v", tt.idx, tt.name, f.Type, tt.typ)
		}
		if !f.Nullable {
			t.Errorf("field %d (%s): expected nullable", tt.idx, tt.name)
		}
	}
}

// TestAppendDenormRaw_WithHyperType_SeparateCompilations reproduces a bug where
// AppendDenormRaw produces 0 rows when the HyperType is created from a separate
// proto compilation than the Transcoder. The denorm plan is compiled against the
// Transcoder's message descriptor, but AppendDenormRaw unmarshals using the
// HyperType's message descriptor. If these are different descriptor instances
// (from separate CompileProtoToFileDescriptor calls), EvalLeaves silently
// returns empty results because protobuf field descriptor identity doesn't match.
func TestAppendDenormRaw_WithHyperType_SeparateCompilations(t *testing.T) {
	protoDir := testProtoDir(t)

	// Step 1: Compile proto SEPARATELY for HyperType (simulates BufarrowNewHyperType)
	htFD, err := CompileProtoToFileDescriptor("samples.proto", []string{protoDir})
	if err != nil {
		t.Fatalf("compile for HyperType: %v", err)
	}
	htMD, err := GetMessageDescriptorByName(htFD, "Nested")
	if err != nil {
		t.Fatalf("get message for HyperType: %v", err)
	}
	ht := NewHyperType(htMD)

	// Step 2: Create Transcoder via NewFromFile (compiles proto AGAIN, simulates BufarrowNewFromFileWithHyperType)
	tc, err := NewFromFile("samples.proto", "Nested", []string{protoDir}, memory.DefaultAllocator,
		WithHyperType(ht),
		WithDenormalizerPlan(
			pbpath.PlanPath("nested_scalar.string", pbpath.Alias("scalar_string")),
			pbpath.PlanPath("nested_repeated_scalar[*].int32", pbpath.Alias("rep_int32")),
		),
	)
	if err != nil {
		t.Fatalf("NewFromFile: %v", err)
	}
	defer tc.Release()

	// Step 3: Build a Nested message and serialize it
	nestedMD := htMD // use the HyperType's descriptor to build the message
	msg := dynamicpb.NewMessage(nestedMD)
	scalarMD := nestedMD.Fields().ByName("nested_scalar").Message()
	scalar := dynamicpb.NewMessage(scalarMD)
	scalar.Set(scalarMD.Fields().ByName("string"), protoreflect.ValueOfString("hello"))
	msg.Set(nestedMD.Fields().ByName("nested_scalar"), protoreflect.ValueOfMessage(scalar))

	repField := nestedMD.Fields().ByName("nested_repeated_scalar")
	repList := msg.Mutable(repField).List()
	for _, v := range []int32{10, 20, 30} {
		item := dynamicpb.NewMessage(repField.Message())
		item.Set(repField.Message().Fields().ByName("int32"), protoreflect.ValueOfInt32(v))
		repList.Append(protoreflect.ValueOfMessage(item))
	}

	data, err := proto.Marshal(msg)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}

	// Step 4: AppendDenormRaw — this should produce 3 rows (fan-out on nested_repeated_scalar)
	if err := tc.AppendDenormRaw(data); err != nil {
		t.Fatalf("AppendDenormRaw: %v", err)
	}

	rb := tc.NewDenormalizerRecordBatch()
	if rb == nil {
		t.Fatal("NewDenormalizerRecordBatch returned nil")
	}
	defer rb.Release()

	// Fixed: denorm plan now compiles against HyperType's descriptor
	if rb.NumRows() != 3 {
		t.Errorf("expected 3 denorm rows, got %d", rb.NumRows())
	}
}

// TestAppendDenormRaw_WithoutHyperType verifies the dynamicpb fallback path
// for AppendDenormRaw when no HyperType is configured.
func TestAppendDenormRaw_WithoutHyperType(t *testing.T) {
	protoDir := testProtoDir(t)

	tc, err := NewFromFile("samples.proto", "Nested", []string{protoDir}, memory.DefaultAllocator,
		WithDenormalizerPlan(
			pbpath.PlanPath("nested_scalar.string", pbpath.Alias("scalar_string")),
			pbpath.PlanPath("nested_repeated_scalar[*].int32", pbpath.Alias("rep_int32")),
		),
	)
	if err != nil {
		t.Fatalf("NewFromFile: %v", err)
	}
	defer tc.Release()

	// Build message using the transcoder's own descriptor
	nestedFD, err := CompileProtoToFileDescriptor("samples.proto", []string{protoDir})
	if err != nil {
		t.Fatalf("compile: %v", err)
	}
	nestedMD, err := GetMessageDescriptorByName(nestedFD, "Nested")
	if err != nil {
		t.Fatalf("get message: %v", err)
	}

	msg := dynamicpb.NewMessage(nestedMD)
	scalarMD := nestedMD.Fields().ByName("nested_scalar").Message()
	scalar := dynamicpb.NewMessage(scalarMD)
	scalar.Set(scalarMD.Fields().ByName("string"), protoreflect.ValueOfString("world"))
	msg.Set(nestedMD.Fields().ByName("nested_scalar"), protoreflect.ValueOfMessage(scalar))

	repField := nestedMD.Fields().ByName("nested_repeated_scalar")
	repList := msg.Mutable(repField).List()
	for _, v := range []int32{100, 200} {
		item := dynamicpb.NewMessage(repField.Message())
		item.Set(repField.Message().Fields().ByName("int32"), protoreflect.ValueOfInt32(v))
		repList.Append(protoreflect.ValueOfMessage(item))
	}

	data, err := proto.Marshal(msg)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}

	if err := tc.AppendDenormRaw(data); err != nil {
		t.Fatalf("AppendDenormRaw: %v", err)
	}

	rb := tc.NewDenormalizerRecordBatch()
	if rb == nil {
		t.Fatal("NewDenormalizerRecordBatch returned nil")
	}
	defer rb.Release()

	if rb.NumRows() != 2 {
		t.Errorf("expected 2 denorm rows, got %d", rb.NumRows())
	}
	if rb.NumCols() != 2 {
		t.Errorf("expected 2 columns, got %d", rb.NumCols())
	}
}
