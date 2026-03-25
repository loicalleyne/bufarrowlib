package bufarrowlib_test

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"log"
	"os"

	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/loicalleyne/bufarrowlib"
	"github.com/loicalleyne/bufarrowlib/proto/pbpath"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/types/dynamicpb"
)

// Example_protoToParquetFile shows the complete workflow of consuming protobuf
// messages and writing them to a Parquet file on disk.
func Example_protoToParquetFile() {
	md := buildSimpleDescriptor()
	tc, err := bufarrowlib.New(md, memory.DefaultAllocator)
	if err != nil {
		log.Fatal(err)
	}
	defer tc.Release()

	// Simulate consuming a stream of protobuf messages.
	products := []struct {
		name  string
		price float64
		qty   int32
	}{
		{"Widget", 9.99, 5},
		{"Gadget", 24.50, 2},
		{"Gizmo", 7.25, 12},
	}
	for _, p := range products {
		tc.Append(newProduct(md, p.name, p.price, p.qty))
	}

	// Write to a Parquet file.
	f, err := os.CreateTemp("", "example-*.parquet")
	if err != nil {
		log.Fatal(err)
	}
	name := f.Name()
	defer os.Remove(name)

	if err := tc.WriteParquet(f); err != nil {
		log.Fatal(err)
	}
	f.Close()

	info, _ := os.Stat(name)
	fmt.Printf("wrote %d messages to parquet (%d bytes)\n", len(products), info.Size())
	// Output:
	// wrote 3 messages to parquet (738 bytes)
}

// Example_protoToParquetBatched shows writing protobuf messages to Parquet in
// batches — useful when processing a large stream and flushing periodically.
func Example_protoToParquetBatched() {
	md := buildSimpleDescriptor()
	tc, err := bufarrowlib.New(md, memory.DefaultAllocator)
	if err != nil {
		log.Fatal(err)
	}
	defer tc.Release()

	// Batch 1: first two messages.
	tc.Append(newProduct(md, "Widget", 9.99, 5))
	tc.Append(newProduct(md, "Gadget", 24.50, 2))
	batch1 := tc.NewRecordBatch()
	defer batch1.Release()

	// Batch 2: one more message.
	tc.Append(newProduct(md, "Gizmo", 7.25, 12))
	batch2 := tc.NewRecordBatch()
	defer batch2.Release()

	// Write both batches to a single Parquet file.
	var buf bytes.Buffer
	if err := tc.WriteParquetRecords(&buf, batch1, batch2); err != nil {
		log.Fatal(err)
	}

	// Read back and verify total row count.
	reader := bytes.NewReader(buf.Bytes())
	rec, err := tc.ReadParquet(context.Background(), reader, nil)
	if err != nil {
		log.Fatal(err)
	}
	defer rec.Release()

	fmt.Printf("batches: 2, total rows read back: %d\n", rec.NumRows())
	// Output:
	// batches: 2, total rows read back: 3
}

// Example_parquetRoundTrip shows a full round-trip: proto → Arrow → Parquet →
// Arrow → proto. This demonstrates that data survives serialisation intact.
func Example_parquetRoundTrip() {
	md := buildSimpleDescriptor()
	tc, err := bufarrowlib.New(md, memory.DefaultAllocator)
	if err != nil {
		log.Fatal(err)
	}
	defer tc.Release()

	// Encode two products.
	tc.Append(newProduct(md, "Widget", 9.99, 5))
	tc.Append(newProduct(md, "Gadget", 24.50, 2))

	// Proto → Arrow → Parquet (in-memory buffer).
	var buf bytes.Buffer
	if err := tc.WriteParquet(&buf); err != nil {
		log.Fatal(err)
	}

	// Parquet → Arrow.
	reader := bytes.NewReader(buf.Bytes())
	rec, err := tc.ReadParquet(context.Background(), reader, nil)
	if err != nil {
		log.Fatal(err)
	}
	defer rec.Release()

	// Arrow → Proto.
	msgs := tc.Proto(rec, nil)

	fmt.Printf("round-tripped %d messages:\n", len(msgs))
	for _, m := range msgs {
		js, _ := protojson.Marshal(m)
		var c bytes.Buffer
		json.Compact(&c, js)
		fmt.Printf("  %s\n", c.String())
	}
	// Output:
	// round-tripped 2 messages:
	//   {"name":"Widget","price":9.99,"qty":5}
	//   {"name":"Gadget","price":24.5,"qty":2}
}

// Example_parquetSelectColumns shows reading only specific columns from a
// Parquet file — useful for analytics queries that touch a subset of fields.
func Example_parquetSelectColumns() {
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

	// Read only column 0 (name) and column 2 (qty).
	reader := bytes.NewReader(buf.Bytes())
	rec, err := tc.ReadParquet(context.Background(), reader, []int{0, 2})
	if err != nil {
		log.Fatal(err)
	}
	defer rec.Release()

	fmt.Printf("cols: %d, rows: %d\n", rec.NumCols(), rec.NumRows())
	names := rec.Column(0).(*array.String)
	qtys := rec.Column(1).(*array.Int32)
	for i := 0; i < int(rec.NumRows()); i++ {
		fmt.Printf("  %s: %d\n", names.Value(i), qtys.Value(i))
	}
	// Output:
	// cols: 2, rows: 2
	//   Widget: 5
	//   Gadget: 2
}

// Example_denormToParquet shows the complete workflow of denormalizing nested
// protobuf messages into a flat Arrow record and writing to Parquet.
func Example_denormToParquet() {
	orderMD, itemMD := buildExampleDescriptors()

	tc, err := bufarrowlib.New(orderMD, memory.DefaultAllocator,
		bufarrowlib.WithDenormalizerPlan(
			pbpath.PlanPath("name", pbpath.Alias("order_name")),
			pbpath.PlanPath("items[*].id", pbpath.Alias("item_id")),
			pbpath.PlanPath("items[*].price", pbpath.Alias("item_price")),
			pbpath.PlanPath("tags[*]", pbpath.Alias("tag")),
		),
	)
	if err != nil {
		log.Fatal(err)
	}
	defer tc.Release()

	// Append an order with 2 items × 2 tags → 4 denormalized rows.
	msg := newOrder(orderMD, itemMD, "order-1",
		[]struct{ id string; price float64 }{{"A", 1.50}, {"B", 2.75}},
		[]string{"rush", "fragile"}, 1,
	)
	if err := tc.AppendDenorm(msg); err != nil {
		log.Fatal(err)
	}

	// The denormalized record builder produces a flat schema.
	fmt.Printf("schema: %v\n", tc.DenormalizerSchema().Field(0).Name)

	rec := tc.NewDenormalizerRecordBatch()
	defer rec.Release()

	fmt.Printf("denorm rows: %d (2 items × 2 tags)\n", rec.NumRows())

	// Write the flat denormalized record to Parquet via a new Transcoder
	// built from the denorm schema, or write directly using pqarrow.
	// Here we show the denorm data for inspection:
	for i := 0; i < int(rec.NumRows()); i++ {
		name := rec.Column(0).(*array.String).Value(i)
		id := rec.Column(1).(*array.String).Value(i)
		price := rec.Column(2).(*array.Float64).Value(i)
		tag := rec.Column(3).(*array.String).Value(i)
		fmt.Printf("  %s | %s | %.2f | %s\n", name, id, price, tag)
	}
	// Output:
	// schema: order_name
	// denorm rows: 4 (2 items × 2 tags)
	//   order-1 | A | 1.50 | rush
	//   order-1 | A | 1.50 | fragile
	//   order-1 | B | 2.75 | rush
	//   order-1 | B | 2.75 | fragile
}

// Example_protoFileToParquet shows the end-to-end workflow when working with
// .proto files on disk: compile the schema, create a transcoder, populate
// messages, and write Parquet.
func Example_protoFileToParquet() {
	// Write an inline .proto to a temp file.
	_, dir := writeProtoFile("event.proto", `syntax = "proto3";
message Event {
  string id      = 1;
  string action  = 2;
  int64  user_id = 3;
}
`)
	defer os.RemoveAll(dir)

	// Compile the .proto and look up the message descriptor.
	fd, err := bufarrowlib.CompileProtoToFileDescriptor("event.proto", []string{dir})
	if err != nil {
		log.Fatal(err)
	}
	md, err := bufarrowlib.GetMessageDescriptorByName(fd, "Event")
	if err != nil {
		log.Fatal(err)
	}

	// Create a Transcoder from the compiled descriptor.
	tc, err := bufarrowlib.New(md, memory.DefaultAllocator)
	if err != nil {
		log.Fatal(err)
	}
	defer tc.Release()

	// Build and append messages using dynamicpb.
	for _, e := range []struct {
		id, action string
		uid        int64
	}{
		{"e1", "click", 100},
		{"e2", "view", 200},
	} {
		msg := dynamicpb.NewMessage(md)
		msg.Set(md.Fields().ByName("id"), protoreflect.ValueOfString(e.id))
		msg.Set(md.Fields().ByName("action"), protoreflect.ValueOfString(e.action))
		msg.Set(md.Fields().ByName("user_id"), protoreflect.ValueOfInt64(e.uid))
		tc.Append(msg)
	}

	var buf bytes.Buffer
	if err := tc.WriteParquet(&buf); err != nil {
		log.Fatal(err)
	}

	fmt.Printf("fields: %v\n", tc.FieldNames())
	fmt.Printf("wrote parquet: %d bytes\n", buf.Len())
	// Output:
	// fields: [id action user_id]
	// wrote parquet: 688 bytes
}
