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

// buildExprExampleDescriptors constructs a proto schema for demonstrating
// Expr-based denormalization:
//
//	message LineItem { string sku = 1; double unit_price = 2; int64 quantity = 3; }
//	message Sale {
//	  string            customer  = 1;
//	  string            email     = 2;
//	  string            region    = 3;
//	  repeated LineItem items     = 4;
//	  int64             timestamp = 5;
//	}
func buildExprExampleDescriptors() (saleMD, lineItemMD protoreflect.MessageDescriptor) {
	stringType := descriptorpb.FieldDescriptorProto_TYPE_STRING.Enum()
	doubleType := descriptorpb.FieldDescriptorProto_TYPE_DOUBLE.Enum()
	int64Type := descriptorpb.FieldDescriptorProto_TYPE_INT64.Enum()
	messageType := descriptorpb.FieldDescriptorProto_TYPE_MESSAGE.Enum()
	labelOpt := descriptorpb.FieldDescriptorProto_LABEL_OPTIONAL.Enum()
	labelRep := descriptorpb.FieldDescriptorProto_LABEL_REPEATED.Enum()

	fdp := &descriptorpb.FileDescriptorProto{
		Name:    proto.String("expr_example.proto"),
		Package: proto.String("exprexample"),
		Syntax:  proto.String("proto3"),
		MessageType: []*descriptorpb.DescriptorProto{
			{
				Name: proto.String("LineItem"),
				Field: []*descriptorpb.FieldDescriptorProto{
					{Name: proto.String("sku"), Number: proto.Int32(1), Type: stringType, Label: labelOpt},
					{Name: proto.String("unit_price"), Number: proto.Int32(2), Type: doubleType, Label: labelOpt},
					{Name: proto.String("quantity"), Number: proto.Int32(3), Type: int64Type, Label: labelOpt},
				},
			},
			{
				Name: proto.String("Sale"),
				Field: []*descriptorpb.FieldDescriptorProto{
					{Name: proto.String("customer"), Number: proto.Int32(1), Type: stringType, Label: labelOpt},
					{Name: proto.String("email"), Number: proto.Int32(2), Type: stringType, Label: labelOpt},
					{Name: proto.String("region"), Number: proto.Int32(3), Type: stringType, Label: labelOpt},
					{Name: proto.String("items"), Number: proto.Int32(4), Type: messageType, TypeName: proto.String(".exprexample.LineItem"), Label: labelRep},
					{Name: proto.String("timestamp"), Number: proto.Int32(5), Type: int64Type, Label: labelOpt},
				},
			},
		},
	}
	fd, err := protodesc.NewFile(fdp, nil)
	if err != nil {
		log.Fatalf("protodesc.NewFile: %v", err)
	}
	return fd.Messages().ByName("Sale"), fd.Messages().ByName("LineItem")
}

func newSale(saleMD, lineItemMD protoreflect.MessageDescriptor, customer, email, region string, items []struct {
	sku   string
	price float64
	qty   int64
}, ts int64) proto.Message {
	msg := dynamicpb.NewMessage(saleMD)
	if customer != "" {
		msg.Set(saleMD.Fields().ByName("customer"), protoreflect.ValueOfString(customer))
	}
	if email != "" {
		msg.Set(saleMD.Fields().ByName("email"), protoreflect.ValueOfString(email))
	}
	if region != "" {
		msg.Set(saleMD.Fields().ByName("region"), protoreflect.ValueOfString(region))
	}
	msg.Set(saleMD.Fields().ByName("timestamp"), protoreflect.ValueOfInt64(ts))
	list := msg.Mutable(saleMD.Fields().ByName("items")).List()
	for _, it := range items {
		item := dynamicpb.NewMessage(lineItemMD)
		item.Set(lineItemMD.Fields().ByName("sku"), protoreflect.ValueOfString(it.sku))
		item.Set(lineItemMD.Fields().ByName("unit_price"), protoreflect.ValueOfFloat64(it.price))
		item.Set(lineItemMD.Fields().ByName("quantity"), protoreflect.ValueOfInt64(it.qty))
		list.Append(protoreflect.ValueOfMessage(item))
	}
	return msg
}

// ExampleTranscoder_AppendDenorm_coalesce demonstrates FuncCoalesce in a
// denormalization plan. Coalesce returns the first non-zero value from
// multiple paths — useful when a field can come from different sources.
func ExampleTranscoder_AppendDenorm_coalesce() {
	saleMD, lineItemMD := buildExprExampleDescriptors()

	tc, err := bufarrowlib.New(saleMD, memory.DefaultAllocator,
		bufarrowlib.WithDenormalizerPlan(
			// Coalesce: use customer name, fall back to email if customer is empty.
			pbpath.PlanPath("buyer",
				pbpath.WithExpr(pbpath.FuncCoalesce(
					pbpath.PathRef("customer"),
					pbpath.PathRef("email"),
				)),
				pbpath.Alias("buyer"),
			),
			pbpath.PlanPath("region", pbpath.Alias("region")),
		),
	)
	if err != nil {
		log.Fatal(err)
	}
	defer tc.Release()

	// Message 1: customer is set → "Alice" is used.
	msg1 := newSale(saleMD, lineItemMD, "Alice", "alice@example.com", "US", nil, 0)
	tc.AppendDenorm(msg1)

	// Message 2: customer is empty → email is used as fallback.
	msg2 := newSale(saleMD, lineItemMD, "", "bob@example.com", "EU", nil, 0)
	tc.AppendDenorm(msg2)

	rec := tc.NewDenormalizerRecordBatch()
	defer rec.Release()

	for i := 0; i < int(rec.NumRows()); i++ {
		buyer := rec.Column(0).(*array.String).Value(i)
		region := rec.Column(1).(*array.String).Value(i)
		fmt.Printf("  buyer=%-18s region=%s\n", buyer, region)
	}
	// Output:
	//   buyer=Alice              region=US
	//   buyer=bob@example.com    region=EU
}

// ExampleTranscoder_AppendDenorm_default demonstrates FuncDefault, which
// provides a literal fallback value when a field is zero/empty.
func ExampleTranscoder_AppendDenorm_default() {
	saleMD, lineItemMD := buildExprExampleDescriptors()

	tc, err := bufarrowlib.New(saleMD, memory.DefaultAllocator,
		bufarrowlib.WithDenormalizerPlan(
			pbpath.PlanPath("customer", pbpath.Alias("customer")),
			// Default: if region is empty, use "UNKNOWN".
			pbpath.PlanPath("region",
				pbpath.WithExpr(pbpath.FuncDefault(
					pbpath.PathRef("region"),
					pbpath.ScalarString("UNKNOWN"),
				)),
				pbpath.Alias("region"),
			),
		),
	)
	if err != nil {
		log.Fatal(err)
	}
	defer tc.Release()

	tc.AppendDenorm(newSale(saleMD, lineItemMD, "Alice", "", "US", nil, 0))
	tc.AppendDenorm(newSale(saleMD, lineItemMD, "Bob", "", "", nil, 0)) // no region

	rec := tc.NewDenormalizerRecordBatch()
	defer rec.Release()

	for i := 0; i < int(rec.NumRows()); i++ {
		customer := rec.Column(0).(*array.String).Value(i)
		region := rec.Column(1).(*array.String).Value(i)
		fmt.Printf("  %s: %s\n", customer, region)
	}
	// Output:
	//   Alice: US
	//   Bob: UNKNOWN
}

// ExampleTranscoder_AppendDenorm_concat demonstrates FuncConcat, which joins
// the string representations of multiple path values with a separator.
func ExampleTranscoder_AppendDenorm_concat() {
	saleMD, lineItemMD := buildExprExampleDescriptors()

	tc, err := bufarrowlib.New(saleMD, memory.DefaultAllocator,
		bufarrowlib.WithDenormalizerPlan(
			// Concat: join customer and region with " / ".
			pbpath.PlanPath("label",
				pbpath.WithExpr(pbpath.FuncConcat(" / ",
					pbpath.PathRef("customer"),
					pbpath.PathRef("region"),
				)),
				pbpath.Alias("label"),
			),
		),
	)
	if err != nil {
		log.Fatal(err)
	}
	defer tc.Release()

	tc.AppendDenorm(newSale(saleMD, lineItemMD, "Alice", "", "US", nil, 0))
	tc.AppendDenorm(newSale(saleMD, lineItemMD, "Bob", "", "EU", nil, 0))

	rec := tc.NewDenormalizerRecordBatch()
	defer rec.Release()

	for i := 0; i < int(rec.NumRows()); i++ {
		fmt.Println(rec.Column(0).(*array.String).Value(i))
	}
	// Output:
	// Alice / US
	// Bob / EU
}

// ExampleTranscoder_AppendDenorm_upper demonstrates FuncUpper, which converts
// a string field to upper case — useful for normalizing data during ingestion.
func ExampleTranscoder_AppendDenorm_upper() {
	saleMD, lineItemMD := buildExprExampleDescriptors()

	tc, err := bufarrowlib.New(saleMD, memory.DefaultAllocator,
		bufarrowlib.WithDenormalizerPlan(
			pbpath.PlanPath("region_upper",
				pbpath.WithExpr(pbpath.FuncUpper(
					pbpath.PathRef("region"),
				)),
				pbpath.Alias("region_upper"),
			),
		),
	)
	if err != nil {
		log.Fatal(err)
	}
	defer tc.Release()

	tc.AppendDenorm(newSale(saleMD, lineItemMD, "Alice", "", "us-east", nil, 0))

	rec := tc.NewDenormalizerRecordBatch()
	defer rec.Release()

	fmt.Println(rec.Column(0).(*array.String).Value(0))
	// Output:
	// US-EAST
}

// ExampleTranscoder_AppendDenorm_has demonstrates FuncHas, which returns a
// boolean indicating whether a field has a non-zero value. This is useful
// for creating presence flag columns.
func ExampleTranscoder_AppendDenorm_has() {
	saleMD, lineItemMD := buildExprExampleDescriptors()

	tc, err := bufarrowlib.New(saleMD, memory.DefaultAllocator,
		bufarrowlib.WithDenormalizerPlan(
			pbpath.PlanPath("customer", pbpath.Alias("customer")),
			// Has: does the email field have a non-empty value?
			pbpath.PlanPath("has_email",
				pbpath.WithExpr(pbpath.FuncHas(
					pbpath.PathRef("email"),
				)),
				pbpath.Alias("has_email"),
			),
		),
	)
	if err != nil {
		log.Fatal(err)
	}
	defer tc.Release()

	tc.AppendDenorm(newSale(saleMD, lineItemMD, "Alice", "alice@example.com", "", nil, 0))
	tc.AppendDenorm(newSale(saleMD, lineItemMD, "Bob", "", "", nil, 0)) // no email

	rec := tc.NewDenormalizerRecordBatch()
	defer rec.Release()

	for i := 0; i < int(rec.NumRows()); i++ {
		customer := rec.Column(0).(*array.String).Value(i)
		hasEmail := rec.Column(1).(*array.Boolean).Value(i)
		fmt.Printf("  %s: has_email=%v\n", customer, hasEmail)
	}
	// Output:
	//   Alice: has_email=true
	//   Bob: has_email=false
}

// ExampleTranscoder_AppendDenorm_composedExpr demonstrates composing multiple
// Expr functions together. This example creates a "display label" column that
// combines customer name and uppercase region, with a fallback for missing
// customer names.
func ExampleTranscoder_AppendDenorm_composedExpr() {
	saleMD, lineItemMD := buildExprExampleDescriptors()

	tc, err := bufarrowlib.New(saleMD, memory.DefaultAllocator,
		bufarrowlib.WithDenormalizerPlan(
			// Composed expression:
			//   Concat(": ", Coalesce(customer, email), Upper(region))
			// This reads as: join the first non-empty identifier with the
			// uppercased region, separated by ": ".
			pbpath.PlanPath("display",
				pbpath.WithExpr(pbpath.FuncConcat(": ",
					pbpath.FuncCoalesce(
						pbpath.PathRef("customer"),
						pbpath.PathRef("email"),
					),
					pbpath.FuncUpper(
						pbpath.PathRef("region"),
					),
				)),
				pbpath.Alias("display"),
			),
		),
	)
	if err != nil {
		log.Fatal(err)
	}
	defer tc.Release()

	tc.AppendDenorm(newSale(saleMD, lineItemMD, "Alice", "alice@ex.com", "us", nil, 0))
	tc.AppendDenorm(newSale(saleMD, lineItemMD, "", "bob@ex.com", "eu", nil, 0))

	rec := tc.NewDenormalizerRecordBatch()
	defer rec.Release()

	for i := 0; i < int(rec.NumRows()); i++ {
		fmt.Println(rec.Column(0).(*array.String).Value(i))
	}
	// Output:
	// Alice: US
	// bob@ex.com: EU
}

// ExampleTranscoder_AppendDenorm_withExprAndFanout demonstrates combining
// Expr-based computed columns with fan-out (wildcard) paths. The expression
// columns broadcast as scalars while the repeated field fans out.
func ExampleTranscoder_AppendDenorm_withExprAndFanout() {
	saleMD, lineItemMD := buildExprExampleDescriptors()

	tc, err := bufarrowlib.New(saleMD, memory.DefaultAllocator,
		bufarrowlib.WithDenormalizerPlan(
			// Computed column: coalesce customer/email (scalar, broadcasts)
			pbpath.PlanPath("buyer",
				pbpath.WithExpr(pbpath.FuncCoalesce(
					pbpath.PathRef("customer"),
					pbpath.PathRef("email"),
				)),
				pbpath.Alias("buyer"),
			),
			// Fan-out: one row per line item
			pbpath.PlanPath("items[*].sku", pbpath.Alias("sku")),
			pbpath.PlanPath("items[*].unit_price", pbpath.Alias("price")),
		),
	)
	if err != nil {
		log.Fatal(err)
	}
	defer tc.Release()

	msg := newSale(saleMD, lineItemMD, "Alice", "", "US", []struct {
		sku   string
		price float64
		qty   int64
	}{
		{"WIDGET-001", 9.99, 2},
		{"GADGET-002", 24.50, 1},
	}, 0)
	tc.AppendDenorm(msg)

	rec := tc.NewDenormalizerRecordBatch()
	defer rec.Release()

	fmt.Printf("rows: %d\n", rec.NumRows())
	for i := 0; i < int(rec.NumRows()); i++ {
		buyer := rec.Column(0).(*array.String).Value(i)
		sku := rec.Column(1).(*array.String).Value(i)
		price := rec.Column(2).(*array.Float64).Value(i)
		fmt.Printf("  %s | %-12s | %.2f\n", buyer, sku, price)
	}
	// Output:
	// rows: 2
	//   Alice | WIDGET-001   | 9.99
	//   Alice | GADGET-002   | 24.50
}
