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

// buildHyperExampleDescriptors constructs a proto schema with nested and
// repeated fields for demonstrating AppendRaw, AppendDenormRaw, and Expr:
//
//	message Inner { string id = 1; double price = 2; }
//	message Outer {
//	  string         name     = 1;
//	  string         alt_name = 2;
//	  repeated Inner items    = 3;
//	  int64          qty      = 4;
//	}
func buildHyperExampleDescriptors() (outerMD, innerMD protoreflect.MessageDescriptor) {
	stringType := descriptorpb.FieldDescriptorProto_TYPE_STRING.Enum()
	doubleType := descriptorpb.FieldDescriptorProto_TYPE_DOUBLE.Enum()
	int64Type := descriptorpb.FieldDescriptorProto_TYPE_INT64.Enum()
	messageType := descriptorpb.FieldDescriptorProto_TYPE_MESSAGE.Enum()
	labelOpt := descriptorpb.FieldDescriptorProto_LABEL_OPTIONAL.Enum()
	labelRep := descriptorpb.FieldDescriptorProto_LABEL_REPEATED.Enum()

	fdp := &descriptorpb.FileDescriptorProto{
		Name:    proto.String("hyper_example.proto"),
		Package: proto.String("hyperexample"),
		Syntax:  proto.String("proto3"),
		MessageType: []*descriptorpb.DescriptorProto{
			{
				Name: proto.String("Inner"),
				Field: []*descriptorpb.FieldDescriptorProto{
					{Name: proto.String("id"), Number: proto.Int32(1), Type: stringType, Label: labelOpt},
					{Name: proto.String("price"), Number: proto.Int32(2), Type: doubleType, Label: labelOpt},
				},
			},
			{
				Name: proto.String("Outer"),
				Field: []*descriptorpb.FieldDescriptorProto{
					{Name: proto.String("name"), Number: proto.Int32(1), Type: stringType, Label: labelOpt},
					{Name: proto.String("alt_name"), Number: proto.Int32(2), Type: stringType, Label: labelOpt},
					{Name: proto.String("items"), Number: proto.Int32(3), Type: messageType, TypeName: proto.String(".hyperexample.Inner"), Label: labelRep},
					{Name: proto.String("qty"), Number: proto.Int32(4), Type: int64Type, Label: labelOpt},
				},
			},
		},
	}
	fd, err := protodesc.NewFile(fdp, nil)
	if err != nil {
		log.Fatalf("protodesc.NewFile: %v", err)
	}
	return fd.Messages().ByName("Outer"), fd.Messages().ByName("Inner")
}

func newOuterMessage(outerMD, innerMD protoreflect.MessageDescriptor, name, altName string, items []struct {
	id    string
	price float64
}, qty int64) *dynamicpb.Message {
	msg := dynamicpb.NewMessage(outerMD)
	msg.Set(outerMD.Fields().ByName("name"), protoreflect.ValueOfString(name))
	if altName != "" {
		msg.Set(outerMD.Fields().ByName("alt_name"), protoreflect.ValueOfString(altName))
	}
	msg.Set(outerMD.Fields().ByName("qty"), protoreflect.ValueOfInt64(qty))
	list := msg.Mutable(outerMD.Fields().ByName("items")).List()
	for _, it := range items {
		item := dynamicpb.NewMessage(innerMD)
		item.Set(innerMD.Fields().ByName("id"), protoreflect.ValueOfString(it.id))
		item.Set(innerMD.Fields().ByName("price"), protoreflect.ValueOfFloat64(it.price))
		list.Append(protoreflect.ValueOfMessage(item))
	}
	return msg
}

// ExampleNewHyperType demonstrates creating a HyperType coordinator for
// high-performance raw-bytes ingestion. HyperType compiles a hyperpb parser
// from a message descriptor and can be shared across multiple Transcoders.
func ExampleNewHyperType() {
	outerMD, _ := buildHyperExampleDescriptors()

	// Create a HyperType — this compiles the hyperpb parser once.
	ht := bufarrowlib.NewHyperType(outerMD)

	// The compiled type is accessible and can be inspected.
	fmt.Printf("type: %v\n", ht.Type() != nil)
	fmt.Printf("sample rate: %.2f\n", ht.SampleRate())
	// Output:
	// type: true
	// sample rate: 0.01
}

// ExampleNewHyperType_withAutoRecompile demonstrates enabling automatic
// profile-guided recompilation. After the threshold number of messages,
// the parser is recompiled using collected profiling data.
func ExampleNewHyperType_withAutoRecompile() {
	outerMD, _ := buildHyperExampleDescriptors()

	// Recompile every 10,000 messages, sampling 5% of them for profiling.
	ht := bufarrowlib.NewHyperType(outerMD,
		bufarrowlib.WithAutoRecompile(10_000, 0.05),
	)

	fmt.Printf("type: %v\n", ht.Type() != nil)
	fmt.Printf("sample rate: %.2f\n", ht.SampleRate())
	// Output:
	// type: true
	// sample rate: 0.05
}

// ExampleTranscoder_AppendRaw demonstrates high-performance raw-bytes
// ingestion using AppendRaw. This accepts raw protobuf wire bytes (e.g. from
// Kafka, gRPC, or a file) and decodes them using hyperpb's compiled parser —
// 2–3× faster than proto.Unmarshal with generated code.
//
// AppendRaw populates the full Arrow record (like Append), while
// AppendDenormRaw populates the denormalized flat record (like AppendDenorm).
func ExampleTranscoder_AppendRaw() {
	outerMD, innerMD := buildHyperExampleDescriptors()

	// 1. Create a shared HyperType (compile the parser once).
	ht := bufarrowlib.NewHyperType(outerMD)

	// 2. Create a Transcoder with HyperType.
	tc, err := bufarrowlib.New(outerMD, memory.DefaultAllocator,
		bufarrowlib.WithHyperType(ht),
	)
	if err != nil {
		log.Fatal(err)
	}
	defer tc.Release()

	// 3. Marshal a message to raw bytes (simulating receiving from Kafka).
	msg := newOuterMessage(outerMD, innerMD, "Widget", "", []struct {
		id    string
		price float64
	}{{"A", 9.99}}, 5)
	raw, err := proto.Marshal(msg)
	if err != nil {
		log.Fatal(err)
	}

	// 4. Feed raw bytes — no proto.Unmarshal needed.
	if err := tc.AppendRaw(raw); err != nil {
		log.Fatal(err)
	}

	rec := tc.NewRecordBatch()
	defer rec.Release()

	fmt.Printf("rows: %d, cols: %d\n", rec.NumRows(), rec.NumCols())
	// Output:
	// rows: 1, cols: 4
}

// ExampleTranscoder_AppendDenormRaw demonstrates high-performance raw-bytes
// denormalization. This combines hyperpb decoding with the Plan-based
// denormalizer to go directly from raw protobuf bytes to a flat Arrow record.
//
// This is the fastest path for streaming protobuf data into analytics-ready
// flat tables.
func ExampleTranscoder_AppendDenormRaw() {
	outerMD, innerMD := buildHyperExampleDescriptors()

	// 1. Create a shared HyperType.
	ht := bufarrowlib.NewHyperType(outerMD)

	// 2. Create a Transcoder with HyperType + denormalization plan.
	tc, err := bufarrowlib.New(outerMD, memory.DefaultAllocator,
		bufarrowlib.WithHyperType(ht),
		bufarrowlib.WithDenormalizerPlan(
			pbpath.PlanPath("name", pbpath.Alias("product")),
			pbpath.PlanPath("items[*].id", pbpath.Alias("item_id")),
			pbpath.PlanPath("items[*].price", pbpath.Alias("item_price")),
		),
	)
	if err != nil {
		log.Fatal(err)
	}
	defer tc.Release()

	// 3. Marshal messages to raw bytes (simulating Kafka/gRPC input).
	msg := newOuterMessage(outerMD, innerMD, "Gadgets", "", []struct {
		id    string
		price float64
	}{{"X", 1.50}, {"Y", 2.75}}, 10)
	raw, err := proto.Marshal(msg)
	if err != nil {
		log.Fatal(err)
	}

	// 4. Feed raw bytes directly into the denormalizer.
	if err := tc.AppendDenormRaw(raw); err != nil {
		log.Fatal(err)
	}

	rec := tc.NewDenormalizerRecordBatch()
	defer rec.Release()

	fmt.Printf("rows: %d\n", rec.NumRows())
	for i := 0; i < int(rec.NumRows()); i++ {
		name := rec.Column(0).(*array.String).Value(i)
		id := rec.Column(1).(*array.String).Value(i)
		price := rec.Column(2).(*array.Float64).Value(i)
		fmt.Printf("  %s | %s | %.2f\n", name, id, price)
	}
	// Output:
	// rows: 2
	//   Gadgets | X | 1.50
	//   Gadgets | Y | 2.75
}

// ExampleTranscoder_AppendDenormRaw_batch demonstrates a typical batch
// processing pattern: feed many raw messages, then flush to a single
// Arrow record batch.
func ExampleTranscoder_AppendDenormRaw_batch() {
	outerMD, innerMD := buildHyperExampleDescriptors()
	ht := bufarrowlib.NewHyperType(outerMD)

	tc, err := bufarrowlib.New(outerMD, memory.DefaultAllocator,
		bufarrowlib.WithHyperType(ht),
		bufarrowlib.WithDenormalizerPlan(
			pbpath.PlanPath("name", pbpath.Alias("product")),
			pbpath.PlanPath("items[*].id", pbpath.Alias("item_id")),
		),
	)
	if err != nil {
		log.Fatal(err)
	}
	defer tc.Release()

	// Simulate a batch of raw messages from a Kafka consumer.
	messages := []struct {
		name  string
		items []struct {
			id    string
			price float64
		}
	}{
		{"Order-1", []struct {
			id    string
			price float64
		}{{"A", 1.0}, {"B", 2.0}}},
		{"Order-2", []struct {
			id    string
			price float64
		}{{"C", 3.0}}},
		{"Order-3", []struct {
			id    string
			price float64
		}{{"D", 4.0}, {"E", 5.0}, {"F", 6.0}}},
	}

	for _, m := range messages {
		msg := newOuterMessage(outerMD, innerMD, m.name, "", m.items, 0)
		raw, _ := proto.Marshal(msg)
		if err := tc.AppendDenormRaw(raw); err != nil {
			log.Fatal(err)
		}
	}

	// Flush all accumulated rows into one Arrow record batch.
	rec := tc.NewDenormalizerRecordBatch()
	defer rec.Release()

	fmt.Printf("messages: %d, denorm rows: %d\n", len(messages), rec.NumRows())
	// Output:
	// messages: 3, denorm rows: 6
}

// ExampleTranscoder_Clone_withHyperType demonstrates cloning a Transcoder
// that uses HyperType. The clone shares the same HyperType (so profiling
// data is aggregated) but has independent Arrow builders. This is the
// recommended pattern for multi-goroutine pipelines.
func ExampleTranscoder_Clone_withHyperType() {
	outerMD, innerMD := buildHyperExampleDescriptors()

	// Shared HyperType — both transcoders contribute profiling data.
	ht := bufarrowlib.NewHyperType(outerMD)

	tc, err := bufarrowlib.New(outerMD, memory.DefaultAllocator,
		bufarrowlib.WithHyperType(ht),
		bufarrowlib.WithDenormalizerPlan(
			pbpath.PlanPath("name", pbpath.Alias("product")),
			pbpath.PlanPath("items[*].id", pbpath.Alias("item_id")),
		),
	)
	if err != nil {
		log.Fatal(err)
	}
	defer tc.Release()

	// Clone for a second goroutine — shares HyperType + immutable Plan,
	// fresh Arrow builders and scratch buffers.
	clone, err := tc.Clone(memory.DefaultAllocator)
	if err != nil {
		log.Fatal(err)
	}
	defer clone.Release()

	// Feed different data to each transcoder.
	msg1 := newOuterMessage(outerMD, innerMD, "Alpha", "", []struct {
		id    string
		price float64
	}{{"A1", 1.0}}, 0)
	raw1, _ := proto.Marshal(msg1)
	tc.AppendDenormRaw(raw1)

	msg2 := newOuterMessage(outerMD, innerMD, "Bravo", "", []struct {
		id    string
		price float64
	}{{"B1", 2.0}, {"B2", 3.0}}, 0)
	raw2, _ := proto.Marshal(msg2)
	clone.AppendDenormRaw(raw2)

	// Each transcoder flushes independently.
	rec1 := tc.NewDenormalizerRecordBatch()
	defer rec1.Release()
	rec2 := clone.NewDenormalizerRecordBatch()
	defer rec2.Release()

	fmt.Printf("original: %d rows\n", rec1.NumRows())
	fmt.Printf("clone:    %d rows\n", rec2.NumRows())
	// Output:
	// original: 1 rows
	// clone:    2 rows
}

// ExampleHyperType_Recompile demonstrates manual profile-guided recompilation.
// After processing a batch of messages, the profiling data is used to recompile
// the parser for better performance on subsequent batches.
func ExampleHyperType_Recompile() {
	outerMD, innerMD := buildHyperExampleDescriptors()

	// Manual recompile mode: threshold=0 disables auto-recompile,
	// rate=1.0 profiles 100% of messages for maximum accuracy.
	ht := bufarrowlib.NewHyperType(outerMD,
		bufarrowlib.WithAutoRecompile(0, 1.0),
	)

	tc, err := bufarrowlib.New(outerMD, memory.DefaultAllocator,
		bufarrowlib.WithHyperType(ht),
		bufarrowlib.WithDenormalizerPlan(
			pbpath.PlanPath("name", pbpath.Alias("product")),
			pbpath.PlanPath("items[*].id", pbpath.Alias("item_id")),
		),
	)
	if err != nil {
		log.Fatal(err)
	}
	defer tc.Release()

	// Phase 1: Profile a representative batch.
	for i := 0; i < 100; i++ {
		msg := newOuterMessage(outerMD, innerMD,
			fmt.Sprintf("product-%d", i), "",
			[]struct {
				id    string
				price float64
			}{
				{fmt.Sprintf("item-%d", i), float64(i) + 0.99},
			}, int64(i),
		)
		raw, _ := proto.Marshal(msg)
		tc.AppendDenormRaw(raw)
	}
	rec := tc.NewDenormalizerRecordBatch()
	rec.Release()

	// Phase 2: Recompile with the collected profile.
	if err := ht.Recompile(); err != nil {
		log.Fatal(err)
	}
	fmt.Println("recompiled successfully")

	// Phase 3: Process more data with the optimised parser.
	msg := newOuterMessage(outerMD, innerMD, "final", "", []struct {
		id    string
		price float64
	}{{"Z", 99.99}}, 1)
	raw, _ := proto.Marshal(msg)
	tc.AppendDenormRaw(raw)

	rec2 := tc.NewDenormalizerRecordBatch()
	defer rec2.Release()

	fmt.Printf("rows after recompile: %d\n", rec2.NumRows())
	// Output:
	// recompiled successfully
	// rows after recompile: 1
}

// ExampleWithHyperType demonstrates the WithHyperType option, which connects
// a Transcoder to a shared HyperType coordinator for raw-bytes ingestion.
func ExampleWithHyperType() {
	outerMD, innerMD := buildHyperExampleDescriptors()

	ht := bufarrowlib.NewHyperType(outerMD)

	// WithHyperType enables AppendRaw and AppendDenormRaw on the transcoder.
	tc, err := bufarrowlib.New(outerMD, memory.DefaultAllocator,
		bufarrowlib.WithHyperType(ht),
	)
	if err != nil {
		log.Fatal(err)
	}
	defer tc.Release()

	msg := newOuterMessage(outerMD, innerMD, "Test", "", nil, 42)
	raw, _ := proto.Marshal(msg)

	// AppendRaw is now available because WithHyperType was provided.
	if err := tc.AppendRaw(raw); err != nil {
		log.Fatal(err)
	}

	rec := tc.NewRecordBatch()
	defer rec.Release()

	fmt.Printf("rows: %d\n", rec.NumRows())
	// Output:
	// rows: 1
}
