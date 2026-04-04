package bufarrowlib_test

import (
	"fmt"
	"log"

	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/loicalleyne/bufarrowlib"
	"github.com/loicalleyne/bufarrowlib/proto/pbpath"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protodesc"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/types/descriptorpb"
	"google.golang.org/protobuf/types/dynamicpb"
)

// buildExampleDescriptors constructs an inline proto schema for examples:
//
//	message Item { string id = 1; double price = 2; }
//	message Order {
//	  string         name  = 1;
//	  repeated Item  items = 2;
//	  repeated string tags = 3;
//	  int64          seq   = 4;
//	}
func buildExampleDescriptors() (orderMD, itemMD protoreflect.MessageDescriptor) {
	stringType := descriptorpb.FieldDescriptorProto_TYPE_STRING.Enum()
	doubleType := descriptorpb.FieldDescriptorProto_TYPE_DOUBLE.Enum()
	int64Type := descriptorpb.FieldDescriptorProto_TYPE_INT64.Enum()
	messageType := descriptorpb.FieldDescriptorProto_TYPE_MESSAGE.Enum()
	labelOpt := descriptorpb.FieldDescriptorProto_LABEL_OPTIONAL.Enum()
	labelRep := descriptorpb.FieldDescriptorProto_LABEL_REPEATED.Enum()

	fdp := &descriptorpb.FileDescriptorProto{
		Name:    proto.String("example.proto"),
		Package: proto.String("example"),
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
					{Name: proto.String("items"), Number: proto.Int32(2), Type: messageType, TypeName: proto.String(".example.Item"), Label: labelRep},
					{Name: proto.String("tags"), Number: proto.Int32(3), Type: stringType, Label: labelRep},
					{Name: proto.String("seq"), Number: proto.Int32(4), Type: int64Type, Label: labelOpt},
				},
			},
		},
	}
	fd, err := protodesc.NewFile(fdp, nil)
	if err != nil {
		log.Fatalf("protodesc.NewFile: %v", err)
	}
	return fd.Messages().ByName("Order"), fd.Messages().ByName("Item")
}

func newOrder(orderMD, itemMD protoreflect.MessageDescriptor, name string, items []struct {
	id    string
	price float64
}, tags []string, seq int64) proto.Message {
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

// ExampleTranscoder_AppendDenorm demonstrates basic denormalization: projecting
// scalar and fan-out fields from a protobuf message into a flat Arrow record.
func ExampleTranscoder_AppendDenorm() {
	orderMD, itemMD := buildExampleDescriptors()

	tc, err := bufarrowlib.New(orderMD, memory.DefaultAllocator,
		bufarrowlib.WithDenormalizerPlan(
			pbpath.PlanPath("name", pbpath.Alias("order_name")),
			pbpath.PlanPath("items[*].id", pbpath.Alias("item_id")),
			pbpath.PlanPath("items[*].price", pbpath.Alias("item_price")),
		),
	)
	if err != nil {
		log.Fatal(err)
	}
	defer tc.Release()

	msg := newOrder(orderMD, itemMD, "order-1",
		[]struct {
			id    string
			price float64
		}{{"A", 1.50}, {"B", 2.75}},
		nil, 1,
	)
	if err := tc.AppendDenorm(msg); err != nil {
		log.Fatal(err)
	}

	rec := tc.NewDenormalizerRecordBatch()
	defer rec.Release()

	fmt.Printf("rows: %d, cols: %d\n", rec.NumRows(), rec.NumCols())
	for i := 0; i < int(rec.NumRows()); i++ {
		name := rec.Column(0).(*array.String).Value(i)
		id := rec.Column(1).(*array.String).Value(i)
		price := rec.Column(2).(*array.Float64).Value(i)
		fmt.Printf("  %s | %s | %.2f\n", name, id, price)
	}
	// Output:
	// rows: 2, cols: 3
	//   order-1 | A | 1.50
	//   order-1 | B | 2.75
}

// ExampleTranscoder_AppendDenorm_crossJoin demonstrates cross-join behaviour
// when two independent repeated fields are denormalized together.
func ExampleTranscoder_AppendDenorm_crossJoin() {
	orderMD, itemMD := buildExampleDescriptors()

	tc, err := bufarrowlib.New(orderMD, memory.DefaultAllocator,
		bufarrowlib.WithDenormalizerPlan(
			pbpath.PlanPath("items[*].id", pbpath.Alias("item_id")),
			pbpath.PlanPath("tags[*]", pbpath.Alias("tag")),
		),
	)
	if err != nil {
		log.Fatal(err)
	}
	defer tc.Release()

	msg := newOrder(orderMD, itemMD, "order-1",
		[]struct {
			id    string
			price float64
		}{{"A", 1.0}, {"B", 2.0}},
		[]string{"x", "y", "z"}, 1,
	)
	if err := tc.AppendDenorm(msg); err != nil {
		log.Fatal(err)
	}

	rec := tc.NewDenormalizerRecordBatch()
	defer rec.Release()

	fmt.Printf("rows: %d (2 items × 3 tags)\n", rec.NumRows())
	for i := 0; i < int(rec.NumRows()); i++ {
		id := rec.Column(0).(*array.String).Value(i)
		tag := rec.Column(1).(*array.String).Value(i)
		fmt.Printf("  %s | %s\n", id, tag)
	}
	// Output:
	// rows: 6 (2 items × 3 tags)
	//   A | x
	//   A | y
	//   A | z
	//   B | x
	//   B | y
	//   B | z
}

// ExampleTranscoder_AppendDenorm_leftJoin demonstrates left-join semantics
// when a repeated field is empty: a single null row is produced for that
// group while the other group fans out normally.
func ExampleTranscoder_AppendDenorm_leftJoin() {
	orderMD, itemMD := buildExampleDescriptors()

	tc, err := bufarrowlib.New(orderMD, memory.DefaultAllocator,
		bufarrowlib.WithDenormalizerPlan(
			pbpath.PlanPath("items[*].id", pbpath.Alias("item_id")),
			pbpath.PlanPath("tags[*]", pbpath.Alias("tag")),
		),
	)
	if err != nil {
		log.Fatal(err)
	}
	defer tc.Release()

	// Zero items, 2 tags → items group produces 1 null row → 1 × 2 = 2 rows
	msg := newOrder(orderMD, itemMD, "order-1", nil, []string{"x", "y"}, 1)
	if err := tc.AppendDenorm(msg); err != nil {
		log.Fatal(err)
	}

	rec := tc.NewDenormalizerRecordBatch()
	defer rec.Release()

	fmt.Printf("rows: %d\n", rec.NumRows())
	for i := 0; i < int(rec.NumRows()); i++ {
		idNull := rec.Column(0).IsNull(i)
		tag := rec.Column(1).(*array.String).Value(i)
		fmt.Printf("  item_id=null:%v | tag=%s\n", idNull, tag)
	}
	// Output:
	// rows: 2
	//   item_id=null:true | tag=x
	//   item_id=null:true | tag=y
}

// ExampleTranscoder_DenormalizerSchema demonstrates inspecting the Arrow schema
// of the denormalized record, showing column names, types, and nullability.
func ExampleTranscoder_DenormalizerSchema() {
	orderMD, _ := buildExampleDescriptors()

	tc, err := bufarrowlib.New(orderMD, memory.DefaultAllocator,
		bufarrowlib.WithDenormalizerPlan(
			pbpath.PlanPath("name", pbpath.Alias("order_name")),
			pbpath.PlanPath("seq", pbpath.Alias("order_seq")),
			pbpath.PlanPath("items[*].price", pbpath.Alias("item_price")),
		),
	)
	if err != nil {
		log.Fatal(err)
	}
	defer tc.Release()

	schema := tc.DenormalizerSchema()
	for i, f := range schema.Fields() {
		fmt.Printf("  %d: %-12s %-10s nullable=%v\n", i, f.Name, f.Type, f.Nullable)
	}
	// Output:
	//   0: order_name   utf8       nullable=true
	//   1: order_seq    int64      nullable=true
	//   2: item_price   float64    nullable=true
}
