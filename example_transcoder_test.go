package bufarrowlib_test

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"path/filepath"

	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/loicalleyne/bufarrowlib"
	"github.com/loicalleyne/bufarrowlib/proto/pbpath"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protodesc"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/types/descriptorpb"
	"google.golang.org/protobuf/types/dynamicpb"
)

// buildSimpleDescriptor constructs an inline proto schema for examples:
//
//	message Product {
//	  string name  = 1;
//	  double price = 2;
//	  int32  qty   = 3;
//	}
func buildSimpleDescriptor() protoreflect.MessageDescriptor {
	stringType := descriptorpb.FieldDescriptorProto_TYPE_STRING.Enum()
	doubleType := descriptorpb.FieldDescriptorProto_TYPE_DOUBLE.Enum()
	int32Type := descriptorpb.FieldDescriptorProto_TYPE_INT32.Enum()
	labelOpt := descriptorpb.FieldDescriptorProto_LABEL_OPTIONAL.Enum()

	fdp := &descriptorpb.FileDescriptorProto{
		Name:    proto.String("example_transcoder.proto"),
		Package: proto.String("example"),
		Syntax:  proto.String("proto3"),
		MessageType: []*descriptorpb.DescriptorProto{
			{
				Name: proto.String("Product"),
				Field: []*descriptorpb.FieldDescriptorProto{
					{Name: proto.String("name"), Number: proto.Int32(1), Type: stringType, Label: labelOpt},
					{Name: proto.String("price"), Number: proto.Int32(2), Type: doubleType, Label: labelOpt},
					{Name: proto.String("qty"), Number: proto.Int32(3), Type: int32Type, Label: labelOpt},
				},
			},
		},
	}
	fd, err := protodesc.NewFile(fdp, nil)
	if err != nil {
		log.Fatalf("protodesc.NewFile: %v", err)
	}
	return fd.Messages().ByName("Product")
}

// buildCustomDescriptor constructs an inline custom-fields message:
//
//	message CustomFields {
//	  string region   = 1;
//	  int64  batch_id = 2;
//	}
func buildCustomDescriptor() protoreflect.MessageDescriptor {
	stringType := descriptorpb.FieldDescriptorProto_TYPE_STRING.Enum()
	int64Type := descriptorpb.FieldDescriptorProto_TYPE_INT64.Enum()
	labelOpt := descriptorpb.FieldDescriptorProto_LABEL_OPTIONAL.Enum()

	fdp := &descriptorpb.FileDescriptorProto{
		Name:    proto.String("example_custom.proto"),
		Package: proto.String("example"),
		Syntax:  proto.String("proto3"),
		MessageType: []*descriptorpb.DescriptorProto{
			{
				Name: proto.String("CustomFields"),
				Field: []*descriptorpb.FieldDescriptorProto{
					{Name: proto.String("region"), Number: proto.Int32(1), Type: stringType, Label: labelOpt},
					{Name: proto.String("batch_id"), Number: proto.Int32(2), Type: int64Type, Label: labelOpt},
				},
			},
		},
	}
	fd, err := protodesc.NewFile(fdp, nil)
	if err != nil {
		log.Fatalf("protodesc.NewFile: %v", err)
	}
	return fd.Messages().ByName("CustomFields")
}

func newProduct(md protoreflect.MessageDescriptor, name string, price float64, qty int32) *dynamicpb.Message {
	msg := dynamicpb.NewMessage(md)
	msg.Set(md.Fields().ByName("name"), protoreflect.ValueOfString(name))
	msg.Set(md.Fields().ByName("price"), protoreflect.ValueOfFloat64(price))
	msg.Set(md.Fields().ByName("qty"), protoreflect.ValueOfInt32(qty))
	return msg
}

// ExampleNew demonstrates creating a Transcoder, appending protobuf messages,
// and retrieving the result as an Arrow record batch.
func ExampleNew() {
	md := buildSimpleDescriptor()
	tc, err := bufarrowlib.New(md, memory.DefaultAllocator)
	if err != nil {
		log.Fatal(err)
	}
	defer tc.Release()

	tc.Append(newProduct(md, "Widget", 9.99, 5))
	tc.Append(newProduct(md, "Gadget", 24.50, 2))

	rec := tc.NewRecordBatch()
	defer rec.Release()

	fmt.Printf("rows: %d, cols: %d\n", rec.NumRows(), rec.NumCols())
	fmt.Printf("fields: %v\n", tc.FieldNames())
	// Output:
	// rows: 2, cols: 3
	// fields: [name price qty]
}

// ExampleNew_withCustomMessage demonstrates augmenting a protobuf schema
// with custom fields using WithCustomMessage, and populating both the
// base and custom fields via AppendWithCustom.
func ExampleNew_withCustomMessage() {
	baseMD := buildSimpleDescriptor()
	customMD := buildCustomDescriptor()

	tc, err := bufarrowlib.New(baseMD, memory.DefaultAllocator,
		bufarrowlib.WithCustomMessage(customMD),
	)
	if err != nil {
		log.Fatal(err)
	}
	defer tc.Release()

	fmt.Printf("fields: %v\n", tc.FieldNames())

	base := newProduct(baseMD, "Widget", 9.99, 5)
	custom := dynamicpb.NewMessage(customMD)
	custom.Set(customMD.Fields().ByName("region"), protoreflect.ValueOfString("US-EAST"))
	custom.Set(customMD.Fields().ByName("batch_id"), protoreflect.ValueOfInt64(42))

	if err := tc.AppendWithCustom(base, custom); err != nil {
		log.Fatal(err)
	}

	rec := tc.NewRecordBatch()
	defer rec.Release()

	fmt.Printf("rows: %d, cols: %d\n", rec.NumRows(), rec.NumCols())
	// Output:
	// fields: [name price qty region batch_id]
	// rows: 1, cols: 5
}

// ExampleTranscoder_Append shows the basic append-and-flush cycle.
func ExampleTranscoder_Append() {
	md := buildSimpleDescriptor()
	tc, err := bufarrowlib.New(md, memory.DefaultAllocator)
	if err != nil {
		log.Fatal(err)
	}
	defer tc.Release()

	tc.Append(newProduct(md, "Alpha", 1.0, 10))
	tc.Append(newProduct(md, "Bravo", 2.0, 20))

	rec := tc.NewRecordBatch()
	defer rec.Release()

	fmt.Printf("rows: %d\n", rec.NumRows())
	// Output:
	// rows: 2
}

// ExampleTranscoder_AppendWithCustom shows appending a message that includes
// both base and custom fields.
func ExampleTranscoder_AppendWithCustom() {
	baseMD := buildSimpleDescriptor()
	customMD := buildCustomDescriptor()

	tc, err := bufarrowlib.New(baseMD, memory.DefaultAllocator,
		bufarrowlib.WithCustomMessage(customMD),
	)
	if err != nil {
		log.Fatal(err)
	}
	defer tc.Release()

	base := newProduct(baseMD, "Widget", 9.99, 5)
	custom := dynamicpb.NewMessage(customMD)
	custom.Set(customMD.Fields().ByName("region"), protoreflect.ValueOfString("EU"))
	custom.Set(customMD.Fields().ByName("batch_id"), protoreflect.ValueOfInt64(7))

	if err := tc.AppendWithCustom(base, custom); err != nil {
		log.Fatal(err)
	}

	rec := tc.NewRecordBatch()
	defer rec.Release()

	fmt.Printf("rows: %d, cols: %d\n", rec.NumRows(), rec.NumCols())
	// Output:
	// rows: 1, cols: 5
}

// ExampleTranscoder_Schema demonstrates inspecting the Arrow schema
// derived from a protobuf message descriptor.
func ExampleTranscoder_Schema() {
	md := buildSimpleDescriptor()
	tc, err := bufarrowlib.New(md, memory.DefaultAllocator)
	if err != nil {
		log.Fatal(err)
	}
	defer tc.Release()

	for i, f := range tc.Schema().Fields() {
		fmt.Printf("  %d: %-8s %s\n", i, f.Name, f.Type)
	}
	// Output:
	//   0: name     utf8
	//   1: price    float64
	//   2: qty      int32
}

// ExampleTranscoder_Parquet demonstrates inspecting the Parquet schema.
func ExampleTranscoder_Parquet() {
	md := buildSimpleDescriptor()
	tc, err := bufarrowlib.New(md, memory.DefaultAllocator)
	if err != nil {
		log.Fatal(err)
	}
	defer tc.Release()

	pqSchema := tc.Parquet()
	fmt.Printf("parquet columns: %d\n", pqSchema.NumColumns())
	for i := 0; i < pqSchema.NumColumns(); i++ {
		fmt.Printf("  %s\n", pqSchema.Column(i).Name())
	}
	// Output:
	// parquet columns: 3
	//   name
	//   price
	//   qty
}

// ExampleTranscoder_FieldNames shows retrieving top-level field names.
func ExampleTranscoder_FieldNames() {
	md := buildSimpleDescriptor()
	tc, err := bufarrowlib.New(md, memory.DefaultAllocator)
	if err != nil {
		log.Fatal(err)
	}
	defer tc.Release()

	fmt.Println(tc.FieldNames())
	// Output:
	// [name price qty]
}

// ExampleTranscoder_NewRecordBatch shows building an Arrow record batch from
// appended messages.
func ExampleTranscoder_NewRecordBatch() {
	md := buildSimpleDescriptor()
	tc, err := bufarrowlib.New(md, memory.DefaultAllocator)
	if err != nil {
		log.Fatal(err)
	}
	defer tc.Release()

	tc.Append(newProduct(md, "Widget", 9.99, 5))
	rec := tc.NewRecordBatch()
	defer rec.Release()

	fmt.Printf("rows: %d, cols: %d\n", rec.NumRows(), rec.NumCols())
	// Output:
	// rows: 1, cols: 3
}

// ExampleTranscoder_Clone demonstrates cloning a Transcoder for use in
// a separate goroutine. The clone has independent builders but shares
// the same schema configuration.
func ExampleTranscoder_Clone() {
	md := buildSimpleDescriptor()
	tc, err := bufarrowlib.New(md, memory.DefaultAllocator)
	if err != nil {
		log.Fatal(err)
	}
	defer tc.Release()

	clone, err := tc.Clone(memory.DefaultAllocator)
	if err != nil {
		log.Fatal(err)
	}
	defer clone.Release()

	clone.Append(newProduct(md, "Gizmo", 5.00, 10))

	rec := clone.NewRecordBatch()
	defer rec.Release()
	fmt.Printf("clone rows: %d\n", rec.NumRows())

	origRec := tc.NewRecordBatch()
	defer origRec.Release()
	fmt.Printf("original rows: %d\n", origRec.NumRows())
	// Output:
	// clone rows: 1
	// original rows: 0
}

// ExampleTranscoder_WriteParquet demonstrates writing appended messages to
// Parquet and reading them back.
func ExampleTranscoder_WriteParquet() {
	md := buildSimpleDescriptor()
	tc, err := bufarrowlib.New(md, memory.DefaultAllocator)
	if err != nil {
		log.Fatal(err)
	}
	defer tc.Release()

	tc.Append(newProduct(md, "Widget", 9.99, 5))
	tc.Append(newProduct(md, "Gadget", 24.50, 2))

	var buf bytes.Buffer
	if err := tc.WriteParquet(&buf); err != nil {
		log.Fatal(err)
	}

	fmt.Printf("parquet bytes: %d\n", buf.Len())
	fmt.Println("wrote parquet successfully")
	// Output:
	// parquet bytes: 714
	// wrote parquet successfully
}

// ExampleTranscoder_WriteParquetRecords demonstrates writing multiple
// Arrow record batches to a single Parquet file.
func ExampleTranscoder_WriteParquetRecords() {
	md := buildSimpleDescriptor()
	tc, err := bufarrowlib.New(md, memory.DefaultAllocator)
	if err != nil {
		log.Fatal(err)
	}
	defer tc.Release()

	tc.Append(newProduct(md, "Widget", 9.99, 5))
	rec1 := tc.NewRecordBatch()
	defer rec1.Release()

	tc.Append(newProduct(md, "Gadget", 24.50, 2))
	rec2 := tc.NewRecordBatch()
	defer rec2.Release()

	var buf bytes.Buffer
	if err := tc.WriteParquetRecords(&buf, rec1, rec2); err != nil {
		log.Fatal(err)
	}

	fmt.Println("wrote 2 record batches to parquet")
	// Output:
	// wrote 2 record batches to parquet
}

// ExampleTranscoder_ReadParquet demonstrates a full Parquet round-trip:
// write messages to Parquet, then read them back into an Arrow record.
func ExampleTranscoder_ReadParquet() {
	md := buildSimpleDescriptor()
	tc, err := bufarrowlib.New(md, memory.DefaultAllocator)
	if err != nil {
		log.Fatal(err)
	}
	defer tc.Release()

	tc.Append(newProduct(md, "Widget", 9.99, 5))
	tc.Append(newProduct(md, "Gadget", 24.50, 2))

	var buf bytes.Buffer
	if err := tc.WriteParquet(&buf); err != nil {
		log.Fatal(err)
	}

	reader := bytes.NewReader(buf.Bytes())
	rec, err := tc.ReadParquet(context.Background(), reader, nil)
	if err != nil {
		log.Fatal(err)
	}
	defer rec.Release()

	fmt.Printf("read back: %d rows, %d cols\n", rec.NumRows(), rec.NumCols())
	// Output:
	// read back: 2 rows, 3 cols
}

// ExampleTranscoder_Proto demonstrates round-tripping: appending protobuf
// messages, building an Arrow record, and reconstructing them back as
// protobuf messages.
func ExampleTranscoder_Proto() {
	md := buildSimpleDescriptor()
	tc, err := bufarrowlib.New(md, memory.DefaultAllocator)
	if err != nil {
		log.Fatal(err)
	}
	defer tc.Release()

	tc.Append(newProduct(md, "Widget", 9.99, 5))
	tc.Append(newProduct(md, "Gadget", 24.50, 2))

	rec := tc.NewRecordBatch()
	defer rec.Release()

	msgs := tc.Proto(rec, nil) // nil = all rows
	fmt.Printf("recovered %d messages\n", len(msgs))
	for _, m := range msgs {
		js, _ := protojson.Marshal(m)
		var c bytes.Buffer
		json.Compact(&c, js)
		fmt.Println(c.String())
	}
	// Output:
	// recovered 2 messages
	// {"name":"Widget","price":9.99,"qty":5}
	// {"name":"Gadget","price":24.5,"qty":2}
}

// ExampleTranscoder_DenormalizerBuilder shows accessing the underlying
// RecordBuilder for custom denormalization logic.
func ExampleTranscoder_DenormalizerBuilder() {
	orderMD, _ := buildExampleDescriptors()
	tc, err := bufarrowlib.New(orderMD, memory.DefaultAllocator,
		bufarrowlib.WithDenormalizerPlan(
			pbpath.PlanPath("name", pbpath.Alias("order_name")),
		),
	)
	if err != nil {
		log.Fatal(err)
	}
	defer tc.Release()

	builder := tc.DenormalizerBuilder()
	fmt.Printf("builder fields: %d\n", builder.Schema().NumFields())
	fmt.Printf("schema: %s\n", builder.Schema().Field(0).Type)
	// Output:
	// builder fields: 1
	// schema: utf8
}

// ExampleTranscoder_NewDenormalizerRecordBatch shows flushing the
// denormalizer's builder into a record batch.
func ExampleTranscoder_NewDenormalizerRecordBatch() {
	orderMD, itemMD := buildExampleDescriptors()
	tc, err := bufarrowlib.New(orderMD, memory.DefaultAllocator,
		bufarrowlib.WithDenormalizerPlan(
			pbpath.PlanPath("items[*].id", pbpath.Alias("item_id")),
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
		nil, 1,
	)
	if err := tc.AppendDenorm(msg); err != nil {
		log.Fatal(err)
	}

	rec := tc.NewDenormalizerRecordBatch()
	defer rec.Release()

	fmt.Printf("rows: %d\n", rec.NumRows())
	for i := 0; i < int(rec.NumRows()); i++ {
		fmt.Println(rec.Column(0).(*array.String).Value(i))
	}
	// Output:
	// rows: 2
	// A
	// B
}

// ExampleTranscoder_Release shows the standard cleanup pattern.
func ExampleTranscoder_Release() {
	md := buildSimpleDescriptor()
	tc, err := bufarrowlib.New(md, memory.DefaultAllocator)
	if err != nil {
		log.Fatal(err)
	}
	// Release should always be deferred after construction.
	defer tc.Release()

	tc.Append(newProduct(md, "Widget", 9.99, 5))
	rec := tc.NewRecordBatch()
	defer rec.Release()

	fmt.Printf("rows: %d\n", rec.NumRows())
	// Output:
	// rows: 1
}

// ExampleMergeMessageDescriptors demonstrates merging two message descriptors
// into one with combined fields.
func ExampleMergeMessageDescriptors() {
	baseMD := buildSimpleDescriptor()
	customMD := buildCustomDescriptor()

	merged, err := bufarrowlib.MergeMessageDescriptors(baseMD, customMD, "Merged")
	if err != nil {
		log.Fatal(err)
	}

	fields := merged.Fields()
	for i := 0; i < fields.Len(); i++ {
		fmt.Println(fields.Get(i).Name())
	}
	// Output:
	// name
	// price
	// qty
	// region
	// batch_id
}

// ExampleProtoKindToArrowType demonstrates mapping protobuf field kinds to
// Arrow data types.
func ExampleProtoKindToArrowType() {
	md := buildSimpleDescriptor()

	for i := 0; i < md.Fields().Len(); i++ {
		fd := md.Fields().Get(i)
		dt := bufarrowlib.ProtoKindToArrowType(fd)
		fmt.Printf("%-8s → %s\n", fd.Name(), dt)
	}
	// Output:
	// name     → utf8
	// price    → float64
	// qty      → int32
}

// ExampleProtoKindToAppendFunc demonstrates obtaining a typed append closure
// for a protobuf field and using it to populate an Arrow builder.
func ExampleProtoKindToAppendFunc() {
	md := buildSimpleDescriptor()
	nameFD := md.Fields().ByName("name")

	dt := bufarrowlib.ProtoKindToArrowType(nameFD)
	builder := array.NewBuilder(memory.DefaultAllocator, dt)
	defer builder.Release()

	appendFn := bufarrowlib.ProtoKindToAppendFunc(nameFD, builder)
	appendFn(protoreflect.ValueOfString("hello"))
	appendFn(protoreflect.ValueOfString("world"))

	arr := builder.NewArray()
	defer arr.Release()

	fmt.Printf("len: %d\n", arr.Len())
	fmt.Println(arr.(*array.String).Value(0))
	fmt.Println(arr.(*array.String).Value(1))
	// Output:
	// len: 2
	// hello
	// world
}

// ExampleWithDenormalizerPlan demonstrates configuring a denormalization plan.
func ExampleWithDenormalizerPlan() {
	orderMD, itemMD := buildExampleDescriptors()

	tc, err := bufarrowlib.New(orderMD, memory.DefaultAllocator,
		bufarrowlib.WithDenormalizerPlan(
			pbpath.PlanPath("name", pbpath.Alias("order_name")),
			pbpath.PlanPath("items[*].id", pbpath.Alias("item_id")),
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
		}{{"X", 1.0}, {"Y", 2.0}},
		nil, 1,
	)
	if err := tc.AppendDenorm(msg); err != nil {
		log.Fatal(err)
	}

	rec := tc.NewDenormalizerRecordBatch()
	defer rec.Release()

	fmt.Printf("rows: %d\n", rec.NumRows())
	for i := 0; i < int(rec.NumRows()); i++ {
		name := rec.Column(0).(*array.String).Value(i)
		id := rec.Column(1).(*array.String).Value(i)
		fmt.Printf("  %s | %s\n", name, id)
	}
	// Output:
	// rows: 2
	//   order-1 | X
	//   order-1 | Y
}

// ExampleWithCustomMessage demonstrates the WithCustomMessage option.
func ExampleWithCustomMessage() {
	baseMD := buildSimpleDescriptor()
	customMD := buildCustomDescriptor()

	tc, err := bufarrowlib.New(baseMD, memory.DefaultAllocator,
		bufarrowlib.WithCustomMessage(customMD),
	)
	if err != nil {
		log.Fatal(err)
	}
	defer tc.Release()

	fmt.Println(tc.FieldNames())
	// Output:
	// [name price qty region batch_id]
}

// writeProtoFile writes proto content to a temp file and returns its path and
// parent directory for use as an import path.
func writeProtoFile(name, content string) (filePath, dir string) {
	dir, err := os.MkdirTemp("", "bufarrow-example")
	if err != nil {
		log.Fatal(err)
	}
	filePath = filepath.Join(dir, name)
	if err := os.WriteFile(filePath, []byte(content), 0o644); err != nil {
		log.Fatal(err)
	}
	return filePath, dir
}

const exampleProto = `syntax = "proto3";
message Item {
  string name  = 1;
  double price = 2;
}
`

const exampleCustomProto = `syntax = "proto3";
message Extra {
  string region = 1;
}
`

// ExampleCompileProtoToFileDescriptor demonstrates compiling a .proto file
// from disk at runtime and inspecting its messages.
func ExampleCompileProtoToFileDescriptor() {
	_, dir := writeProtoFile("item.proto", exampleProto)
	defer os.RemoveAll(dir)

	fd, err := bufarrowlib.CompileProtoToFileDescriptor("item.proto", []string{dir})
	if err != nil {
		log.Fatal(err)
	}

	for i := 0; i < fd.Messages().Len(); i++ {
		m := fd.Messages().Get(i)
		fmt.Printf("%s (%d fields)\n", m.Name(), m.Fields().Len())
	}
	// Output:
	// Item (2 fields)
}

// ExampleGetMessageDescriptorByName demonstrates looking up a specific message
// by name from a compiled FileDescriptor.
func ExampleGetMessageDescriptorByName() {
	_, dir := writeProtoFile("item.proto", exampleProto)
	defer os.RemoveAll(dir)

	fd, err := bufarrowlib.CompileProtoToFileDescriptor("item.proto", []string{dir})
	if err != nil {
		log.Fatal(err)
	}

	md, err := bufarrowlib.GetMessageDescriptorByName(fd, "Item")
	if err != nil {
		log.Fatal(err)
	}

	for i := 0; i < md.Fields().Len(); i++ {
		f := md.Fields().Get(i)
		fmt.Printf("%s %s\n", f.Name(), f.Kind())
	}
	// Output:
	// name string
	// price double
}

// ExampleNewFromFile demonstrates creating a Transcoder directly from a .proto
// file on disk, without pre-compiling the descriptor yourself.
func ExampleNewFromFile() {
	path, dir := writeProtoFile("item.proto", exampleProto)
	defer os.RemoveAll(dir)

	tc, err := bufarrowlib.NewFromFile(filepath.Base(path), "Item", []string{dir}, memory.DefaultAllocator)
	if err != nil {
		log.Fatal(err)
	}
	defer tc.Release()

	// Populate a message using the schema's descriptor.
	md := tc.Schema().Field(0) // just verify schema was built
	fmt.Printf("field 0: %s\n", md.Name)
	fmt.Printf("fields: %v\n", tc.FieldNames())
	// Output:
	// field 0: name
	// fields: [name price]
}

// ExampleWithCustomMessageFile demonstrates augmenting a base schema with
// custom fields loaded from a .proto file on disk.
func ExampleWithCustomMessageFile() {
	basePath, baseDir := writeProtoFile("item.proto", exampleProto)
	defer os.RemoveAll(baseDir)

	customPath, customDir := writeProtoFile("extra.proto", exampleCustomProto)
	defer os.RemoveAll(customDir)

	tc, err := bufarrowlib.NewFromFile(
		filepath.Base(basePath), "Item", []string{baseDir},
		memory.DefaultAllocator,
		bufarrowlib.WithCustomMessageFile(filepath.Base(customPath), "Extra", []string{customDir}),
	)
	if err != nil {
		log.Fatal(err)
	}
	defer tc.Release()

	fmt.Println(tc.FieldNames())
	// Output:
	// [name price region]
}
