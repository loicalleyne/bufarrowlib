package bufarrowlib

import (
	"bytes"
	"context"
	"encoding/binary"
	"fmt"
	"io"
	"math"
	"path/filepath"
	"runtime"
	"sort"
	"sync"
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/loicalleyne/bufarrowlib/gen/go/samples"
	"github.com/loicalleyne/bufarrowlib/gen/go/samples/custom"
	"github.com/loicalleyne/bufarrowlib/proto/pbpath"
	"github.com/sryoya/protorand"
	metricsv1 "go.opentelemetry.io/proto/otlp/metrics/v1"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protodesc"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/types/descriptorpb"
	"google.golang.org/protobuf/types/dynamicpb"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// ══════════════════════════════════════════════════════════════════════
// helpers
// ══════════════════════════════════════════════════════════════════════

func benchProtoDir() string {
	_, f, _, _ := runtime.Caller(0)
	return filepath.Join(filepath.Dir(f), "proto", "samples")
}

func benchBidRequestDescriptor(b *testing.B) protoreflect.MessageDescriptor {
	b.Helper()
	fd, err := CompileProtoToFileDescriptor("BidRequest.proto", []string{benchProtoDir()})
	if err != nil {
		b.Fatalf("compile BidRequest.proto: %v", err)
	}
	md, err := GetMessageDescriptorByName(fd, "BidRequestEvent")
	if err != nil {
		b.Fatalf("get BidRequestEvent: %v", err)
	}
	return md
}

func generateScalarMessages(b *testing.B, n int) []proto.Message {
	b.Helper()
	pr := protorand.New()
	pr.Seed(42)
	msgs := make([]proto.Message, n)
	for i := range msgs {
		m, err := pr.Gen(&samples.ScalarTypes{})
		if err != nil {
			b.Fatalf("protorand ScalarTypes: %v", err)
		}
		msgs[i] = m
	}
	return msgs
}

func generateMetricsMessages(b *testing.B, n int) []proto.Message {
	b.Helper()
	pr := protorand.New()
	pr.Seed(42)
	pr.MaxCollectionElements = 3
	pr.MaxDepth = 4
	msgs := make([]proto.Message, n)
	for i := range msgs {
		m, err := pr.Gen(&metricsv1.MetricsData{})
		if err != nil {
			b.Fatalf("protorand MetricsData: %v", err)
		}
		msgs[i] = m
	}
	return msgs
}

func generateBidRequestMessages(b *testing.B, md protoreflect.MessageDescriptor, n int) []proto.Message {
	b.Helper()
	pr := protorand.New()
	pr.Seed(42)
	pr.MaxCollectionElements = 3
	pr.MaxDepth = 5
	msgs := make([]proto.Message, n)
	for i := range msgs {
		m, err := pr.NewDynamicProtoRand(md)
		if err != nil {
			b.Fatalf("protorand BidRequestEvent: %v", err)
		}
		msgs[i] = m
	}
	return msgs
}

func benchBuildDenormSchema(b *testing.B) (orderMD, itemMD protoreflect.MessageDescriptor) {
	b.Helper()
	stringType := descriptorpb.FieldDescriptorProto_TYPE_STRING.Enum()
	doubleType := descriptorpb.FieldDescriptorProto_TYPE_DOUBLE.Enum()
	int64Type := descriptorpb.FieldDescriptorProto_TYPE_INT64.Enum()
	messageType := descriptorpb.FieldDescriptorProto_TYPE_MESSAGE.Enum()
	labelOpt := descriptorpb.FieldDescriptorProto_LABEL_OPTIONAL.Enum()
	labelRep := descriptorpb.FieldDescriptorProto_LABEL_REPEATED.Enum()

	fdp := &descriptorpb.FileDescriptorProto{
		Name:    proto.String("bench_denorm.proto"),
		Package: proto.String("benchdenorm"),
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
					{Name: proto.String("items"), Number: proto.Int32(2), Type: messageType, TypeName: proto.String(".benchdenorm.Item"), Label: labelRep},
					{Name: proto.String("tags"), Number: proto.Int32(3), Type: stringType, Label: labelRep},
					{Name: proto.String("seq"), Number: proto.Int32(4), Type: int64Type, Label: labelOpt},
				},
			},
		},
	}
	fd, err := protodesc.NewFile(fdp, nil)
	if err != nil {
		b.Fatalf("protodesc.NewFile: %v", err)
	}
	return fd.Messages().ByName("Order"), fd.Messages().ByName("Item")
}

func generateDenormOrders(b *testing.B, n, nItems, nTags int) ([]proto.Message, protoreflect.MessageDescriptor) {
	b.Helper()
	orderMD, itemMD := benchBuildDenormSchema(b)
	msgs := make([]proto.Message, n)
	for i := range msgs {
		msg := dynamicpb.NewMessage(orderMD)
		msg.Set(orderMD.Fields().ByName("name"), protoreflect.ValueOfString("order"))
		msg.Set(orderMD.Fields().ByName("seq"), protoreflect.ValueOfInt64(int64(i)))
		list := msg.Mutable(orderMD.Fields().ByName("items")).List()
		for j := 0; j < nItems; j++ {
			item := dynamicpb.NewMessage(itemMD)
			item.Set(itemMD.Fields().ByName("id"), protoreflect.ValueOfString("item"))
			item.Set(itemMD.Fields().ByName("price"), protoreflect.ValueOfFloat64(float64(j)+0.99))
			list.Append(protoreflect.ValueOfMessage(item))
		}
		tagList := msg.Mutable(orderMD.Fields().ByName("tags")).List()
		for j := 0; j < nTags; j++ {
			tagList.Append(protoreflect.ValueOfString("tag"))
		}
		msgs[i] = msg
	}
	return msgs, orderMD
}

// ══════════════════════════════════════════════════════════════════════
// Benchmark: New (schema construction)
// ══════════════════════════════════════════════════════════════════════

func BenchmarkNew(b *testing.B) {
	b.Run("ScalarTypes", func(b *testing.B) {
		md := (&samples.ScalarTypes{}).ProtoReflect().Descriptor()
		b.ReportAllocs()
		runtime.GC()
		var msBefore runtime.MemStats
		runtime.ReadMemStats(&msBefore)
		b.ResetTimer()
		for b.Loop() {
			tc, err := New(md, memory.DefaultAllocator)
			if err != nil {
				b.Fatal(err)
			}
			tc.Release()
		}
		b.StopTimer()
		var msAfter runtime.MemStats
		runtime.ReadMemStats(&msAfter)
		b.ReportMetric(float64(b.Elapsed().Nanoseconds())/float64(b.N), "ns/msg")
		b.ReportMetric(float64(b.N)/b.Elapsed().Seconds(), "msg/s")
		b.ReportMetric(float64(msAfter.TotalAlloc-msBefore.TotalAlloc)/float64(b.N), "B/msg")
		b.ReportMetric(float64(msAfter.Mallocs-msBefore.Mallocs)/float64(b.N), "allocs/msg")
	})
	b.Run("MetricsData", func(b *testing.B) {
		md := (&metricsv1.MetricsData{}).ProtoReflect().Descriptor()
		b.ReportAllocs()
		runtime.GC()
		var msBefore runtime.MemStats
		runtime.ReadMemStats(&msBefore)
		b.ResetTimer()
		for b.Loop() {
			tc, err := New(md, memory.DefaultAllocator)
			if err != nil {
				b.Fatal(err)
			}
			tc.Release()
		}
		b.StopTimer()
		var msAfter runtime.MemStats
		runtime.ReadMemStats(&msAfter)
		b.ReportMetric(float64(b.Elapsed().Nanoseconds())/float64(b.N), "ns/msg")
		b.ReportMetric(float64(b.N)/b.Elapsed().Seconds(), "msg/s")
		b.ReportMetric(float64(msAfter.TotalAlloc-msBefore.TotalAlloc)/float64(b.N), "B/msg")
		b.ReportMetric(float64(msAfter.Mallocs-msBefore.Mallocs)/float64(b.N), "allocs/msg")
	})
	b.Run("BidRequest", func(b *testing.B) {
		md := benchBidRequestDescriptor(b)
		b.ReportAllocs()
		runtime.GC()
		var msBefore runtime.MemStats
		runtime.ReadMemStats(&msBefore)
		b.ResetTimer()
		for b.Loop() {
			tc, err := New(md, memory.DefaultAllocator)
			if err != nil {
				b.Fatal(err)
			}
			tc.Release()
		}
		b.StopTimer()
		var msAfter runtime.MemStats
		runtime.ReadMemStats(&msAfter)
		b.ReportMetric(float64(b.Elapsed().Nanoseconds())/float64(b.N), "ns/msg")
		b.ReportMetric(float64(b.N)/b.Elapsed().Seconds(), "msg/s")
		b.ReportMetric(float64(msAfter.TotalAlloc-msBefore.TotalAlloc)/float64(b.N), "B/msg")
		b.ReportMetric(float64(msAfter.Mallocs-msBefore.Mallocs)/float64(b.N), "allocs/msg")
	})
}

// ══════════════════════════════════════════════════════════════════════
// Benchmark: NewFromFile (compile + construct)
// ══════════════════════════════════════════════════════════════════════

func BenchmarkNewFromFile(b *testing.B) {
	dir := benchProtoDir()
	b.Run("ScalarTypes", func(b *testing.B) {
		b.ReportAllocs()
		runtime.GC()
		var msBefore runtime.MemStats
		runtime.ReadMemStats(&msBefore)
		b.ResetTimer()
		for b.Loop() {
			tc, err := NewFromFile("samples.proto", "ScalarTypes", []string{dir}, memory.DefaultAllocator)
			if err != nil {
				b.Fatal(err)
			}
			tc.Release()
		}
		b.StopTimer()
		var msAfter runtime.MemStats
		runtime.ReadMemStats(&msAfter)
		b.ReportMetric(float64(b.Elapsed().Nanoseconds())/float64(b.N), "ns/msg")
		b.ReportMetric(float64(b.N)/b.Elapsed().Seconds(), "msg/s")
		b.ReportMetric(float64(msAfter.TotalAlloc-msBefore.TotalAlloc)/float64(b.N), "B/msg")
		b.ReportMetric(float64(msAfter.Mallocs-msBefore.Mallocs)/float64(b.N), "allocs/msg")
	})
	b.Run("BidRequest", func(b *testing.B) {
		b.ReportAllocs()
		runtime.GC()
		var msBefore runtime.MemStats
		runtime.ReadMemStats(&msBefore)
		b.ResetTimer()
		for b.Loop() {
			tc, err := NewFromFile("BidRequest.proto", "BidRequestEvent", []string{dir}, memory.DefaultAllocator)
			if err != nil {
				b.Fatal(err)
			}
			tc.Release()
		}
		b.StopTimer()
		var msAfter runtime.MemStats
		runtime.ReadMemStats(&msAfter)
		b.ReportMetric(float64(b.Elapsed().Nanoseconds())/float64(b.N), "ns/msg")
		b.ReportMetric(float64(b.N)/b.Elapsed().Seconds(), "msg/s")
		b.ReportMetric(float64(msAfter.TotalAlloc-msBefore.TotalAlloc)/float64(b.N), "B/msg")
		b.ReportMetric(float64(msAfter.Mallocs-msBefore.Mallocs)/float64(b.N), "allocs/msg")
	})
}

// ══════════════════════════════════════════════════════════════════════
// Benchmark: Append
// ══════════════════════════════════════════════════════════════════════

func BenchmarkAppend(b *testing.B) {
	const N = 100

	b.Run("ScalarTypes", func(b *testing.B) {
		md := (&samples.ScalarTypes{}).ProtoReflect().Descriptor()
		tc, err := New(md, memory.DefaultAllocator)
		if err != nil {
			b.Fatal(err)
		}
		defer tc.Release()
		msgs := generateScalarMessages(b, N)
		nMsgs := len(msgs)
		var totalBytes int64
		for _, m := range msgs {
			totalBytes += int64(proto.Size(m))
		}
		b.ReportAllocs()
		b.SetBytes(totalBytes)
		runtime.GC()
		var msBefore runtime.MemStats
		runtime.ReadMemStats(&msBefore)
		b.ResetTimer()
		for b.Loop() {
			for _, m := range msgs {
				tc.Append(m)
			}
			r := tc.NewRecordBatch()
			r.Release()
		}
		b.StopTimer()
		var msAfter runtime.MemStats
		runtime.ReadMemStats(&msAfter)
		totalMsgs := float64(b.N) * float64(nMsgs)
		b.ReportMetric(float64(b.Elapsed().Nanoseconds())/float64(b.N)/float64(nMsgs), "ns/msg")
		b.ReportMetric(float64(b.N*nMsgs)/b.Elapsed().Seconds(), "msg/s")
		b.ReportMetric(float64(msAfter.TotalAlloc-msBefore.TotalAlloc)/totalMsgs, "B/msg")
		b.ReportMetric(float64(msAfter.Mallocs-msBefore.Mallocs)/totalMsgs, "allocs/msg")
	})
	b.Run("MetricsData", func(b *testing.B) {
		md := (&metricsv1.MetricsData{}).ProtoReflect().Descriptor()
		tc, err := New(md, memory.DefaultAllocator)
		if err != nil {
			b.Fatal(err)
		}
		defer tc.Release()
		msgs := generateMetricsMessages(b, N)
		nMsgs := len(msgs)
		var totalBytes int64
		for _, m := range msgs {
			totalBytes += int64(proto.Size(m))
		}
		b.ReportAllocs()
		b.SetBytes(totalBytes)
		runtime.GC()
		var msBefore runtime.MemStats
		runtime.ReadMemStats(&msBefore)
		b.ResetTimer()
		for b.Loop() {
			for _, m := range msgs {
				tc.Append(m)
			}
			r := tc.NewRecordBatch()
			r.Release()
		}
		b.StopTimer()
		var msAfter runtime.MemStats
		runtime.ReadMemStats(&msAfter)
		totalMsgs := float64(b.N) * float64(nMsgs)
		b.ReportMetric(float64(b.Elapsed().Nanoseconds())/float64(b.N)/float64(nMsgs), "ns/msg")
		b.ReportMetric(float64(b.N*nMsgs)/b.Elapsed().Seconds(), "msg/s")
		b.ReportMetric(float64(msAfter.TotalAlloc-msBefore.TotalAlloc)/totalMsgs, "B/msg")
		b.ReportMetric(float64(msAfter.Mallocs-msBefore.Mallocs)/totalMsgs, "allocs/msg")
	})
	b.Run("BidRequest", func(b *testing.B) {
		md := benchBidRequestDescriptor(b)
		tc, err := New(md, memory.DefaultAllocator)
		if err != nil {
			b.Fatal(err)
		}
		defer tc.Release()
		msgs := generateBidRequestMessages(b, md, N)
		nMsgs := len(msgs)
		var totalBytes int64
		for _, m := range msgs {
			totalBytes += int64(proto.Size(m))
		}
		b.ReportAllocs()
		b.SetBytes(totalBytes)
		runtime.GC()
		var msBefore runtime.MemStats
		runtime.ReadMemStats(&msBefore)
		b.ResetTimer()
		for b.Loop() {
			for _, m := range msgs {
				tc.Append(m)
			}
			r := tc.NewRecordBatch()
			r.Release()
		}
		b.StopTimer()
		var msAfter runtime.MemStats
		runtime.ReadMemStats(&msAfter)
		totalMsgs := float64(b.N) * float64(nMsgs)
		b.ReportMetric(float64(b.Elapsed().Nanoseconds())/float64(b.N)/float64(nMsgs), "ns/msg")
		b.ReportMetric(float64(b.N*nMsgs)/b.Elapsed().Seconds(), "msg/s")
		b.ReportMetric(float64(msAfter.TotalAlloc-msBefore.TotalAlloc)/totalMsgs, "B/msg")
		b.ReportMetric(float64(msAfter.Mallocs-msBefore.Mallocs)/totalMsgs, "allocs/msg")
	})
}

// ══════════════════════════════════════════════════════════════════════
// Benchmark: AppendDenorm
// ══════════════════════════════════════════════════════════════════════

func BenchmarkAppendDenorm(b *testing.B) {
	for _, tt := range []struct {
		name  string
		items int
		tags  int
	}{
		{"2x2", 2, 2},
		{"5x5", 5, 5},
		{"10x10", 10, 10},
	} {
		b.Run(tt.name, func(b *testing.B) {
			const N = 100
			msgs, orderMD := generateDenormOrders(b, N, tt.items, tt.tags)
			tc, err := New(orderMD, memory.DefaultAllocator,
				WithDenormalizerPlan(
					pbpath.PlanPath("name", pbpath.Alias("order_name")),
					pbpath.PlanPath("items[*].id", pbpath.Alias("item_id")),
					pbpath.PlanPath("items[*].price", pbpath.Alias("item_price")),
					pbpath.PlanPath("tags[*]", pbpath.Alias("tag")),
				),
			)
			if err != nil {
				b.Fatalf("New: %v", err)
			}
			defer tc.Release()
			nMsgs := len(msgs)
			var totalBytes int64
			for _, m := range msgs {
				totalBytes += int64(proto.Size(m))
			}
			b.ReportAllocs()
			b.SetBytes(totalBytes)
			b.ReportMetric(float64(tt.items*tt.tags), "rows/msg")
			runtime.GC()
			var msBefore runtime.MemStats
			runtime.ReadMemStats(&msBefore)
			b.ResetTimer()
			for b.Loop() {
				for _, m := range msgs {
					tc.AppendDenorm(m)
				}
				r := tc.NewDenormalizerRecordBatch()
				r.Release()
			}
			b.StopTimer()
			var msAfter runtime.MemStats
			runtime.ReadMemStats(&msAfter)
			totalMsgs := float64(b.N) * float64(nMsgs)
			b.ReportMetric(float64(b.Elapsed().Nanoseconds())/float64(b.N)/float64(nMsgs), "ns/msg")
			b.ReportMetric(float64(b.N*nMsgs)/b.Elapsed().Seconds(), "msg/s")
			b.ReportMetric(float64(msAfter.TotalAlloc-msBefore.TotalAlloc)/totalMsgs, "B/msg")
			b.ReportMetric(float64(msAfter.Mallocs-msBefore.Mallocs)/totalMsgs, "allocs/msg")
		})
	}
}

// ══════════════════════════════════════════════════════════════════════
// Benchmark: WriteParquet
// ══════════════════════════════════════════════════════════════════════

func BenchmarkWriteParquet(b *testing.B) {
	b.Run("ScalarTypes_100rows", func(b *testing.B) {
		md := (&samples.ScalarTypes{}).ProtoReflect().Descriptor()
		tc, err := New(md, memory.DefaultAllocator)
		if err != nil {
			b.Fatal(err)
		}
		defer tc.Release()
		msgs := generateScalarMessages(b, 100)
		// Pre-fill, measure parquet output size for SetBytes, then re-fill.
		for _, m := range msgs {
			tc.Append(m)
		}
		var sizeBuf bytes.Buffer
		tc.WriteParquet(&sizeBuf)
		parquetSize := int64(sizeBuf.Len())
		// Re-fill for the loop.
		for _, m := range msgs {
			tc.Append(m)
		}
		nMsgs := len(msgs)
		b.ReportAllocs()
		b.SetBytes(parquetSize)
		runtime.GC()
		var msBefore runtime.MemStats
		runtime.ReadMemStats(&msBefore)
		b.ResetTimer()
		for b.Loop() {
			tc.WriteParquet(io.Discard)
			for _, m := range msgs {
				tc.Append(m)
			}
		}
		b.StopTimer()
		var msAfter runtime.MemStats
		runtime.ReadMemStats(&msAfter)
		totalMsgs := float64(b.N) * float64(nMsgs)
		b.ReportMetric(float64(b.Elapsed().Nanoseconds())/float64(b.N)/float64(nMsgs), "ns/msg")
		b.ReportMetric(float64(b.N*nMsgs)/b.Elapsed().Seconds(), "msg/s")
		b.ReportMetric(float64(msAfter.TotalAlloc-msBefore.TotalAlloc)/totalMsgs, "B/msg")
		b.ReportMetric(float64(msAfter.Mallocs-msBefore.Mallocs)/totalMsgs, "allocs/msg")
	})
	b.Run("BidRequest_100rows", func(b *testing.B) {
		md := benchBidRequestDescriptor(b)
		tc, err := New(md, memory.DefaultAllocator)
		if err != nil {
			b.Fatal(err)
		}
		defer tc.Release()
		msgs := generateBidRequestMessages(b, md, 100)
		// Pre-fill, measure parquet output size for SetBytes, then re-fill.
		for _, m := range msgs {
			tc.Append(m)
		}
		var sizeBuf bytes.Buffer
		tc.WriteParquet(&sizeBuf)
		parquetSize := int64(sizeBuf.Len())
		// Re-fill for the loop.
		for _, m := range msgs {
			tc.Append(m)
		}
		nMsgs := len(msgs)
		b.ReportAllocs()
		b.SetBytes(parquetSize)
		runtime.GC()
		var msBefore runtime.MemStats
		runtime.ReadMemStats(&msBefore)
		b.ResetTimer()
		for b.Loop() {
			tc.WriteParquet(io.Discard)
			for _, m := range msgs {
				tc.Append(m)
			}
		}
		b.StopTimer()
		var msAfter runtime.MemStats
		runtime.ReadMemStats(&msAfter)
		totalMsgs := float64(b.N) * float64(nMsgs)
		b.ReportMetric(float64(b.Elapsed().Nanoseconds())/float64(b.N)/float64(nMsgs), "ns/msg")
		b.ReportMetric(float64(b.N*nMsgs)/b.Elapsed().Seconds(), "msg/s")
		b.ReportMetric(float64(msAfter.TotalAlloc-msBefore.TotalAlloc)/totalMsgs, "B/msg")
		b.ReportMetric(float64(msAfter.Mallocs-msBefore.Mallocs)/totalMsgs, "allocs/msg")
	})
}

// ══════════════════════════════════════════════════════════════════════
// Benchmark: ReadParquet
// ══════════════════════════════════════════════════════════════════════

func BenchmarkReadParquet(b *testing.B) {
	b.Run("ScalarTypes_100rows", func(b *testing.B) {
		md := (&samples.ScalarTypes{}).ProtoReflect().Descriptor()
		tc, err := New(md, memory.DefaultAllocator)
		if err != nil {
			b.Fatal(err)
		}
		defer tc.Release()
		msgs := generateScalarMessages(b, 100)
		for _, m := range msgs {
			tc.Append(m)
		}
		var buf bytes.Buffer
		tc.WriteParquet(&buf)
		data := buf.Bytes()
		nMsgs := len(msgs)
		b.ReportAllocs()
		b.SetBytes(int64(len(data)))
		runtime.GC()
		var msBefore runtime.MemStats
		runtime.ReadMemStats(&msBefore)
		b.ResetTimer()
		for b.Loop() {
			reader := bytes.NewReader(data)
			rec, err := tc.ReadParquet(context.Background(), reader, nil)
			if err != nil {
				b.Fatal(err)
			}
			rec.Release()
		}
		b.StopTimer()
		var msAfter runtime.MemStats
		runtime.ReadMemStats(&msAfter)
		totalMsgs := float64(b.N) * float64(nMsgs)
		b.ReportMetric(float64(b.Elapsed().Nanoseconds())/float64(b.N)/float64(nMsgs), "ns/msg")
		b.ReportMetric(float64(b.N*nMsgs)/b.Elapsed().Seconds(), "msg/s")
		b.ReportMetric(float64(msAfter.TotalAlloc-msBefore.TotalAlloc)/totalMsgs, "B/msg")
		b.ReportMetric(float64(msAfter.Mallocs-msBefore.Mallocs)/totalMsgs, "allocs/msg")
	})
	b.Run("BidRequest_100rows", func(b *testing.B) {
		md := benchBidRequestDescriptor(b)
		tc, err := New(md, memory.DefaultAllocator)
		if err != nil {
			b.Fatal(err)
		}
		defer tc.Release()
		msgs := generateBidRequestMessages(b, md, 100)
		for _, m := range msgs {
			tc.Append(m)
		}
		var buf bytes.Buffer
		tc.WriteParquet(&buf)
		data := buf.Bytes()
		nMsgs := len(msgs)
		b.ReportAllocs()
		b.SetBytes(int64(len(data)))
		b.ReportMetric(float64(len(data)), "parquet-bytes")
		runtime.GC()
		var msBefore runtime.MemStats
		runtime.ReadMemStats(&msBefore)
		b.ResetTimer()
		for b.Loop() {
			reader := bytes.NewReader(data)
			rec, err := tc.ReadParquet(context.Background(), reader, nil)
			if err != nil {
				b.Fatal(err)
			}
			rec.Release()
		}
		b.StopTimer()
		var msAfter runtime.MemStats
		runtime.ReadMemStats(&msAfter)
		totalMsgs := float64(b.N) * float64(nMsgs)
		b.ReportMetric(float64(b.Elapsed().Nanoseconds())/float64(b.N)/float64(nMsgs), "ns/msg")
		b.ReportMetric(float64(b.N*nMsgs)/b.Elapsed().Seconds(), "msg/s")
		b.ReportMetric(float64(msAfter.TotalAlloc-msBefore.TotalAlloc)/totalMsgs, "B/msg")
		b.ReportMetric(float64(msAfter.Mallocs-msBefore.Mallocs)/totalMsgs, "allocs/msg")
	})
}

// ══════════════════════════════════════════════════════════════════════
// Benchmark: Proto (Arrow → protobuf reconstruction)
// ══════════════════════════════════════════════════════════════════════

func BenchmarkProto(b *testing.B) {
	b.Run("ScalarTypes_100rows", func(b *testing.B) {
		md := (&samples.ScalarTypes{}).ProtoReflect().Descriptor()
		tc, err := New(md, memory.DefaultAllocator)
		if err != nil {
			b.Fatal(err)
		}
		defer tc.Release()
		msgs := generateScalarMessages(b, 100)
		for _, m := range msgs {
			tc.Append(m)
		}
		rec := tc.NewRecordBatch()
		defer rec.Release()
		// Measure proto output size for mb/s.
		sampleProto := tc.Proto(rec, nil)
		var protoBytes int64
		for _, pm := range sampleProto {
			protoBytes += int64(proto.Size(pm))
		}
		nMsgs := len(msgs)
		b.ReportAllocs()
		b.SetBytes(protoBytes)
		runtime.GC()
		var msBefore runtime.MemStats
		runtime.ReadMemStats(&msBefore)
		b.ResetTimer()
		for b.Loop() {
			_ = tc.Proto(rec, nil)
		}
		b.StopTimer()
		var msAfter runtime.MemStats
		runtime.ReadMemStats(&msAfter)
		totalMsgs := float64(b.N) * float64(nMsgs)
		b.ReportMetric(float64(b.Elapsed().Nanoseconds())/float64(b.N)/float64(nMsgs), "ns/msg")
		b.ReportMetric(float64(b.N*nMsgs)/b.Elapsed().Seconds(), "msg/s")
		b.ReportMetric(float64(msAfter.TotalAlloc-msBefore.TotalAlloc)/totalMsgs, "B/msg")
		b.ReportMetric(float64(msAfter.Mallocs-msBefore.Mallocs)/totalMsgs, "allocs/msg")
	})
	b.Run("BidRequest_100rows", func(b *testing.B) {
		md := benchBidRequestDescriptor(b)
		tc, err := New(md, memory.DefaultAllocator)
		if err != nil {
			b.Fatal(err)
		}
		defer tc.Release()
		msgs := generateBidRequestMessages(b, md, 100)
		for _, m := range msgs {
			tc.Append(m)
		}
		rec := tc.NewRecordBatch()
		defer rec.Release()
		// Measure proto output size for mb/s.
		sampleProto := tc.Proto(rec, nil)
		var protoBytes int64
		for _, pm := range sampleProto {
			protoBytes += int64(proto.Size(pm))
		}
		nMsgs := len(msgs)
		b.ReportAllocs()
		b.SetBytes(protoBytes)
		runtime.GC()
		var msBefore runtime.MemStats
		runtime.ReadMemStats(&msBefore)
		b.ResetTimer()
		for b.Loop() {
			_ = tc.Proto(rec, nil)
		}
		b.StopTimer()
		var msAfter runtime.MemStats
		runtime.ReadMemStats(&msAfter)
		totalMsgs := float64(b.N) * float64(nMsgs)
		b.ReportMetric(float64(b.Elapsed().Nanoseconds())/float64(b.N)/float64(nMsgs), "ns/msg")
		b.ReportMetric(float64(b.N*nMsgs)/b.Elapsed().Seconds(), "msg/s")
		b.ReportMetric(float64(msAfter.TotalAlloc-msBefore.TotalAlloc)/totalMsgs, "B/msg")
		b.ReportMetric(float64(msAfter.Mallocs-msBefore.Mallocs)/totalMsgs, "allocs/msg")
	})
}

// ══════════════════════════════════════════════════════════════════════
// Benchmark: Clone
// ══════════════════════════════════════════════════════════════════════

func BenchmarkClone(b *testing.B) {
	b.Run("ScalarTypes", func(b *testing.B) {
		md := (&samples.ScalarTypes{}).ProtoReflect().Descriptor()
		tc, err := New(md, memory.DefaultAllocator)
		if err != nil {
			b.Fatal(err)
		}
		defer tc.Release()
		b.ReportAllocs()
		runtime.GC()
		var msBefore runtime.MemStats
		runtime.ReadMemStats(&msBefore)
		b.ResetTimer()
		for b.Loop() {
			c, err := tc.Clone(memory.DefaultAllocator)
			if err != nil {
				b.Fatal(err)
			}
			c.Release()
		}
		b.StopTimer()
		var msAfter runtime.MemStats
		runtime.ReadMemStats(&msAfter)
		b.ReportMetric(float64(b.Elapsed().Nanoseconds())/float64(b.N), "ns/msg")
		b.ReportMetric(float64(b.N)/b.Elapsed().Seconds(), "msg/s")
		b.ReportMetric(float64(msAfter.TotalAlloc-msBefore.TotalAlloc)/float64(b.N), "B/msg")
		b.ReportMetric(float64(msAfter.Mallocs-msBefore.Mallocs)/float64(b.N), "allocs/msg")
	})
	b.Run("BidRequest_with_denorm", func(b *testing.B) {
		md := benchBidRequestDescriptor(b)
		tc, err := New(md, memory.DefaultAllocator,
			WithDenormalizerPlan(
				pbpath.PlanPath("id"),
				pbpath.PlanPath("imp[*].id", pbpath.Alias("imp_id")),
				pbpath.PlanPath("imp[*].banner.w", pbpath.Alias("banner_w")),
				pbpath.PlanPath("device.geo.country", pbpath.Alias("country")),
			),
		)
		if err != nil {
			b.Fatalf("New: %v", err)
		}
		defer tc.Release()
		b.ReportAllocs()
		runtime.GC()
		var msBefore runtime.MemStats
		runtime.ReadMemStats(&msBefore)
		b.ResetTimer()
		for b.Loop() {
			c, err := tc.Clone(memory.DefaultAllocator)
			if err != nil {
				b.Fatal(err)
			}
			c.Release()
		}
		b.StopTimer()
		var msAfter runtime.MemStats
		runtime.ReadMemStats(&msAfter)
		b.ReportMetric(float64(b.Elapsed().Nanoseconds())/float64(b.N), "ns/msg")
		b.ReportMetric(float64(b.N)/b.Elapsed().Seconds(), "msg/s")
		b.ReportMetric(float64(msAfter.TotalAlloc-msBefore.TotalAlloc)/float64(b.N), "B/msg")
		b.ReportMetric(float64(msAfter.Mallocs-msBefore.Mallocs)/float64(b.N), "allocs/msg")
	})
}

// ══════════════════════════════════════════════════════════════════════
// Benchmark: Parquet round-trip (write + read)
// ══════════════════════════════════════════════════════════════════════

func BenchmarkParquetRoundTrip(b *testing.B) {
	b.Run("BidRequest_100rows", func(b *testing.B) {
		md := benchBidRequestDescriptor(b)
		tc, err := New(md, memory.DefaultAllocator)
		if err != nil {
			b.Fatal(err)
		}
		defer tc.Release()
		msgs := generateBidRequestMessages(b, md, 100)
		nMsgs := len(msgs)
		var totalBytes int64
		for _, m := range msgs {
			totalBytes += int64(proto.Size(m))
		}
		b.ReportAllocs()
		b.SetBytes(totalBytes)
		runtime.GC()
		var msBefore runtime.MemStats
		runtime.ReadMemStats(&msBefore)
		b.ResetTimer()
		for b.Loop() {
			for _, m := range msgs {
				tc.Append(m)
			}
			var buf bytes.Buffer
			tc.WriteParquet(&buf)
			reader := bytes.NewReader(buf.Bytes())
			rec, err := tc.ReadParquet(context.Background(), reader, nil)
			if err != nil {
				b.Fatal(err)
			}
			rec.Release()
		}
		b.StopTimer()
		var msAfter runtime.MemStats
		runtime.ReadMemStats(&msAfter)
		totalMsgs := float64(b.N) * float64(nMsgs)
		b.ReportMetric(float64(b.Elapsed().Nanoseconds())/float64(b.N)/float64(nMsgs), "ns/msg")
		b.ReportMetric(float64(b.N*nMsgs)/b.Elapsed().Seconds(), "msg/s")
		b.ReportMetric(float64(msAfter.TotalAlloc-msBefore.TotalAlloc)/totalMsgs, "B/msg")
		b.ReportMetric(float64(msAfter.Mallocs-msBefore.Mallocs)/totalMsgs, "allocs/msg")
	})
}

// ══════════════════════════════════════════════════════════════════════
// Benchmark: End-to-end denorm pipeline
// ══════════════════════════════════════════════════════════════════════

func BenchmarkEndToEnd_DenormPipeline(b *testing.B) {
	const N = 100
	msgs, orderMD := generateDenormOrders(b, N, 5, 3)
	tc, err := New(orderMD, memory.DefaultAllocator,
		WithDenormalizerPlan(
			pbpath.PlanPath("name", pbpath.Alias("order_name")),
			pbpath.PlanPath("items[*].id", pbpath.Alias("item_id")),
			pbpath.PlanPath("items[*].price", pbpath.Alias("item_price")),
			pbpath.PlanPath("tags[*]", pbpath.Alias("tag")),
		),
	)
	if err != nil {
		b.Fatalf("New: %v", err)
	}
	defer tc.Release()
	nMsgs := len(msgs)
	var totalBytes int64
	for _, m := range msgs {
		totalBytes += int64(proto.Size(m))
	}
	b.ReportAllocs()
	b.SetBytes(totalBytes)
	runtime.GC()
	var msBefore runtime.MemStats
	runtime.ReadMemStats(&msBefore)
	b.ResetTimer()
	for b.Loop() {
		for _, m := range msgs {
			tc.AppendDenorm(m)
		}
		rec := tc.NewDenormalizerRecordBatch()
		_ = rec.NumRows()
		rec.Release()
	}
	b.StopTimer()
	var msAfter runtime.MemStats
	runtime.ReadMemStats(&msAfter)
	totalMsgs := float64(b.N) * float64(nMsgs)
	b.ReportMetric(float64(b.Elapsed().Nanoseconds())/float64(b.N)/float64(nMsgs), "ns/msg")
	b.ReportMetric(float64(b.N*nMsgs)/b.Elapsed().Seconds(), "msg/s")
	b.ReportMetric(float64(msAfter.TotalAlloc-msBefore.TotalAlloc)/totalMsgs, "B/msg")
	b.ReportMetric(float64(msAfter.Mallocs-msBefore.Mallocs)/totalMsgs, "allocs/msg")
}

// ══════════════════════════════════════════════════════════════════════
// Benchmark: BidRequest — Custom vs AppendDenorm comparison
// ══════════════════════════════════════════════════════════════════════
//
// These benchmarks compare two approaches for extracting a denormalized flat
// record from BidRequestEvent protobuf messages:
//
//   - Custom:      typed Go getters + manual Arrow builders (modelled on
//                  quacfka-service/bidreq customProtoUnmarshal)
//   - AppendDenorm: Expr-based [Plan] with Coalesce/Cond expressions
//
// Both benchmarks process ALL messages then flush, without alternating.
// The corpus is 500 protorand messages (Seed 42) + 6 hand-crafted edge cases
// serialized to raw proto bytes so that each iteration includes unmarshal cost.

// benchBidRequestCorpus generates 500 random + 6 edge-case BidRequestEvent
// messages and returns them as marshaled proto bytes.
func benchBidRequestCorpus(b *testing.B) [][]byte {
	b.Helper()

	pr := protorand.New()
	pr.Seed(42)
	pr.MaxCollectionElements = 3
	pr.MaxDepth = 5

	raw := make([][]byte, 0, 510)
	for i := 0; i < 500; i++ {
		m, err := pr.Gen(&samples.BidRequestEvent{})
		if err != nil {
			b.Fatalf("protorand gen %d: %v", i, err)
		}
		bs, err := proto.Marshal(m)
		if err != nil {
			b.Fatalf("marshal %d: %v", i, err)
		}
		raw = append(raw, bs)
	}

	// Edge cases — same as testdata/gen_bidrequest/main.go edgeCases().
	edges := []proto.Message{
		// 1. Empty deals
		&samples.BidRequestEvent{
			Id: "edge-empty-deals",
			Imp: []*samples.BidRequestEvent_ImpressionEvent{{
				Id: "imp-ed", Banner: &samples.BidRequestEvent_ImpressionEvent_BannerEvent{W: 300, H: 250},
				Pmp: &samples.BidRequestEvent_ImpressionEvent_PrivateMarketplaceEvent{},
			}},
			User: &samples.BidRequestEvent_UserEvent{Id: "user-ed"},
			Site: &samples.BidRequestEvent_SiteEvent{
				Id:        "site-ed",
				Publisher: &samples.BidRequestEvent_SiteEvent_SitePublisher{Id: proto.String("pub-ed")},
			},
			Timestamp:         &timestamppb.Timestamp{Seconds: 1700000000, Nanos: 500000000},
			Technicalprovider: &samples.BidRequestEvent_TechnicalProviderEvent{Id: 42},
		},
		// 2. Nil banner, populated video
		&samples.BidRequestEvent{
			Id: "edge-nil-banner",
			Imp: []*samples.BidRequestEvent_ImpressionEvent{{
				Id: "imp-nb", Video: &samples.BidRequestEvent_ImpressionEvent_VideoEvent{W: 640, H: 480},
				Pmp: &samples.BidRequestEvent_ImpressionEvent_PrivateMarketplaceEvent{
					Deals: []*samples.BidRequestEvent_ImpressionEvent_PrivateMarketplaceEvent_DealEvent{{Id: "deal-nb"}},
				},
			}},
			User:      &samples.BidRequestEvent_UserEvent{Id: "user-nb"},
			Timestamp: &timestamppb.Timestamp{Seconds: 1700000001},
		},
		// 3. Nil video, populated banner
		&samples.BidRequestEvent{
			Id: "edge-nil-video",
			Imp: []*samples.BidRequestEvent_ImpressionEvent{{
				Id: "imp-nv", Banner: &samples.BidRequestEvent_ImpressionEvent_BannerEvent{W: 728, H: 90},
				Pmp: &samples.BidRequestEvent_ImpressionEvent_PrivateMarketplaceEvent{
					Deals: []*samples.BidRequestEvent_ImpressionEvent_PrivateMarketplaceEvent_DealEvent{{Id: "deal-nv-1"}, {Id: "deal-nv-2"}},
				},
			}},
			Device:    &samples.BidRequestEvent_DeviceEvent{Ifa: "ifa-nv"},
			Timestamp: &timestamppb.Timestamp{Seconds: 1700000002, Nanos: 999999999},
		},
		// 4. Both banner and video nil
		&samples.BidRequestEvent{
			Id: "edge-both-nil",
			Imp: []*samples.BidRequestEvent_ImpressionEvent{{
				Id: "imp-bn",
				Pmp: &samples.BidRequestEvent_ImpressionEvent_PrivateMarketplaceEvent{
					Deals: []*samples.BidRequestEvent_ImpressionEvent_PrivateMarketplaceEvent_DealEvent{{Id: "deal-bn"}},
				},
			}},
			User:      &samples.BidRequestEvent_UserEvent{Id: "user-bn"},
			Timestamp: &timestamppb.Timestamp{Seconds: 1700000003},
		},
		// 5. Zero-value timestamp
		&samples.BidRequestEvent{
			Id: "edge-zero-ts",
			Imp: []*samples.BidRequestEvent_ImpressionEvent{{
				Id: "imp-zt", Banner: &samples.BidRequestEvent_ImpressionEvent_BannerEvent{W: 320, H: 50},
			}},
			User:      &samples.BidRequestEvent_UserEvent{Id: "user-zt"},
			Timestamp: &timestamppb.Timestamp{},
		},
		// 6. Max-depth nesting
		&samples.BidRequestEvent{
			Id: "edge-max-depth",
			User: &samples.BidRequestEvent_UserEvent{
				Id: "user-md",
				Ext: &samples.BidRequestEvent_UserEvent_UserExt{
					Demographic: &samples.BidRequestEvent_UserEvent_UserExt_DemographicEvent{
						Total: &custom.DecimalValue{Units: 12345, Nanos: 678900000},
					},
				},
			},
			Device: &samples.BidRequestEvent_DeviceEvent{
				Ifa: "ifa-md", W: 1920, H: 1080,
				Geo: &samples.BidRequestEvent_DeviceEvent_GeoEvent{Country: "US", Region: "NY"},
			},
			Imp: []*samples.BidRequestEvent_ImpressionEvent{{
				Id:     "imp-md",
				Banner: &samples.BidRequestEvent_ImpressionEvent_BannerEvent{W: 970, H: 250},
				Video:  &samples.BidRequestEvent_ImpressionEvent_VideoEvent{W: 1280, H: 720},
				Pmp: &samples.BidRequestEvent_ImpressionEvent_PrivateMarketplaceEvent{
					PrivateAuction: 1,
					Deals: []*samples.BidRequestEvent_ImpressionEvent_PrivateMarketplaceEvent_DealEvent{
						{
							Id: "d1", Bidfloor: &custom.DecimalValue{Units: 5}, At: 2,
							Ext: &samples.BidRequestEvent_ImpressionEvent_PrivateMarketplaceEvent_DealEvent_DealExtEvent{Adspottype: 1, Guaranteed: 1, MustBid: true},
						},
						{Id: "d2", At: 1},
						{Id: "d3", At: 3},
					},
				},
				Bidfloor: &custom.DecimalValue{Units: 1, Nanos: 250000000},
			}},
			Site: &samples.BidRequestEvent_SiteEvent{
				Id: "site-md", Name: "Example",
				Publisher: &samples.BidRequestEvent_SiteEvent_SitePublisher{Id: proto.String("pub-md")},
			},
			Timestamp:         &timestamppb.Timestamp{Seconds: 1700000099, Nanos: 123456789},
			Technicalprovider: &samples.BidRequestEvent_TechnicalProviderEvent{Id: 1, Name: "MaxDepthTP"},
			Dooh: &samples.BidRequestEvent_BidRequestDoohEvent{
				Id: "dooh-md", Name: "Screen-A",
				Publisher: &samples.BidRequestEvent_BidRequestDoohEvent_PublisherEvent{Id: "dp", Name: "OOH Pub"},
			},
		},
	}
	for _, e := range edges {
		bs, err := proto.Marshal(e)
		if err != nil {
			b.Fatalf("marshal edge case: %v", err)
		}
		raw = append(raw, bs)
	}
	return raw
}

// coalesceStr returns the first non-empty string, or "" if all are empty.
func coalesceStr(vals ...string) string {
	for _, v := range vals {
		if v != "" {
			return v
		}
	}
	return ""
}

// BenchmarkAppendBidRequest_Custom mirrors quacfka-service/bidreq
// customProtoUnmarshal(): unmarshal → typed getters → manual Arrow builders.
// All messages are processed then flushed; the loop does not alternate methods.
func BenchmarkAppendBidRequest_Custom(b *testing.B) {
	corpus := benchBidRequestCorpus(b)

	// Arrow schema matching the 10 columns extracted by customProtoUnmarshal.
	schema := arrow.NewSchema([]arrow.Field{
		{Name: "bidreq_id", Type: arrow.BinaryTypes.String},
		{Name: "device_id", Type: arrow.BinaryTypes.String},
		{Name: "pub_id", Type: arrow.BinaryTypes.String, Nullable: true},
		{Name: "tp_id", Type: arrow.PrimitiveTypes.Uint32},
		{Name: "event_time", Type: arrow.PrimitiveTypes.Int64},
		{Name: "imp_units", Type: arrow.PrimitiveTypes.Int64},
		{Name: "imp_nanos", Type: arrow.PrimitiveTypes.Int32},
		{Name: "width", Type: arrow.PrimitiveTypes.Uint32},
		{Name: "height", Type: arrow.PrimitiveTypes.Uint32},
		{Name: "deal", Type: arrow.BinaryTypes.String, Nullable: true},
	}, nil)
	mem := memory.DefaultAllocator
	builder := array.NewRecordBuilder(mem, schema)
	defer builder.Release()

	bID := builder.Field(0).(*array.StringBuilder)
	bDeviceID := builder.Field(1).(*array.StringBuilder)
	bPubID := builder.Field(2).(*array.StringBuilder)
	bTPID := builder.Field(3).(*array.Uint32Builder)
	bEventTime := builder.Field(4).(*array.Int64Builder)
	bImpUnits := builder.Field(5).(*array.Int64Builder)
	bImpNanos := builder.Field(6).(*array.Int32Builder)
	bWidth := builder.Field(7).(*array.Uint32Builder)
	bHeight := builder.Field(8).(*array.Uint32Builder)
	bDeal := builder.Field(9).(*array.StringBuilder)

	msg := new(samples.BidRequestEvent)
	nMsgs := len(corpus)

	b.ReportAllocs()
	b.SetBytes(int64(totalCorpusBytes(corpus)))

	runtime.GC()
	var msBefore runtime.MemStats
	runtime.ReadMemStats(&msBefore)

	b.ResetTimer()

	for b.Loop() {
		for _, raw := range corpus {
			msg.Reset()
			if err := proto.Unmarshal(raw, msg); err != nil {
				b.Fatal(err)
			}

			id := msg.GetId()
			deviceID := coalesceStr(msg.GetUser().GetId(), msg.GetSite().GetId(), msg.GetDevice().GetIfa())
			pubID := msg.GetSite().GetPublisher().GetId()
			tpID := msg.GetTechnicalprovider().GetId()
			eventTime := msg.GetTimestamp().GetSeconds()
			impUnits := msg.GetUser().GetExt().GetDemographic().GetTotal().GetUnits()
			impNanos := msg.GetUser().GetExt().GetDemographic().GetTotal().GetNanos()

			var width, height uint32
			if len(msg.GetImp()) > 0 {
				imp0 := msg.GetImp()[0]
				if imp0.GetBanner() != nil {
					width = imp0.GetBanner().GetW()
					height = imp0.GetBanner().GetH()
				} else {
					width = imp0.GetVideo().GetW()
					height = imp0.GetVideo().GetH()
				}
			}

			// Fan-out: one row per deal, or one null row if no deals.
			var deals []*samples.BidRequestEvent_ImpressionEvent_PrivateMarketplaceEvent_DealEvent
			if len(msg.GetImp()) > 0 {
				deals = msg.GetImp()[0].GetPmp().GetDeals()
			}
			if len(deals) == 0 {
				bID.Append(id)
				bDeviceID.Append(deviceID)
				bPubID.Append(pubID)
				bTPID.Append(tpID)
				bEventTime.Append(eventTime)
				bImpUnits.Append(impUnits)
				bImpNanos.Append(impNanos)
				bWidth.Append(width)
				bHeight.Append(height)
				bDeal.AppendNull()
			} else {
				for _, d := range deals {
					bID.Append(id)
					bDeviceID.Append(deviceID)
					bPubID.Append(pubID)
					bTPID.Append(tpID)
					bEventTime.Append(eventTime)
					bImpUnits.Append(impUnits)
					bImpNanos.Append(impNanos)
					bWidth.Append(width)
					bHeight.Append(height)
					bDeal.Append(d.GetId())
				}
			}
		}
		rec := builder.NewRecordBatch()
		_ = rec.NumRows()
		rec.Release()
	}
	b.StopTimer()

	var msAfter runtime.MemStats
	runtime.ReadMemStats(&msAfter)
	totalMsgs := float64(b.N) * float64(nMsgs)
	b.ReportMetric(float64(b.Elapsed().Nanoseconds())/float64(b.N)/float64(nMsgs), "ns/msg")
	b.ReportMetric(float64(b.N*nMsgs)/b.Elapsed().Seconds(), "msg/s")
	b.ReportMetric(float64(msAfter.TotalAlloc-msBefore.TotalAlloc)/totalMsgs, "B/msg")
	b.ReportMetric(float64(msAfter.Mallocs-msBefore.Mallocs)/totalMsgs, "allocs/msg")
}

// BenchmarkAppendBidRequest_AppendDenorm uses the Expr-based denormalizer
// plan with Coalesce / Cond expressions equivalent to the Custom benchmark.
// All messages are processed then flushed; the loop does not alternate methods.
func BenchmarkAppendBidRequest_AppendDenorm(b *testing.B) {
	corpus := benchBidRequestCorpus(b)

	md := (&samples.BidRequestEvent{}).ProtoReflect().Descriptor()
	tc, err := New(md, memory.DefaultAllocator,
		WithDenormalizerPlan(
			pbpath.PlanPath("id", pbpath.Alias("bidreq_id")),
			pbpath.PlanPath("device_id",
				pbpath.WithExpr(pbpath.FuncCoalesce(
					pbpath.PathRef("user.id"),
					pbpath.PathRef("site.id"),
					pbpath.PathRef("device.ifa"),
				)),
				pbpath.Alias("device_id"),
			),
			pbpath.PlanPath("site.publisher.id", pbpath.Alias("pub_id")),
			pbpath.PlanPath("technicalprovider.id", pbpath.Alias("tp_id")),
			pbpath.PlanPath("timestamp.seconds", pbpath.Alias("event_time")),
			pbpath.PlanPath("user.ext.demographic.total.units", pbpath.Alias("imp_units")),
			pbpath.PlanPath("user.ext.demographic.total.nanos", pbpath.Alias("imp_nanos")),
			pbpath.PlanPath("width",
				pbpath.WithExpr(pbpath.FuncCoalesce(
					pbpath.PathRef("imp[0].banner.w"),
					pbpath.PathRef("imp[0].video.w"),
				)),
				pbpath.Alias("width"),
			),
			pbpath.PlanPath("height",
				pbpath.WithExpr(pbpath.FuncCoalesce(
					pbpath.PathRef("imp[0].banner.h"),
					pbpath.PathRef("imp[0].video.h"),
				)),
				pbpath.Alias("height"),
			),
			pbpath.PlanPath("imp[0].pmp.deals[*].id", pbpath.Alias("deal")),
		),
	)
	if err != nil {
		b.Fatalf("New with denorm plan: %v", err)
	}
	defer tc.Release()

	msg := new(samples.BidRequestEvent)
	nMsgs := len(corpus)

	b.ReportAllocs()
	b.SetBytes(int64(totalCorpusBytes(corpus)))

	runtime.GC()
	var msBefore runtime.MemStats
	runtime.ReadMemStats(&msBefore)

	b.ResetTimer()

	for b.Loop() {
		for _, raw := range corpus {
			msg.Reset()
			if err := proto.Unmarshal(raw, msg); err != nil {
				b.Fatal(err)
			}
			if err := tc.AppendDenorm(msg); err != nil {
				b.Fatal(err)
			}
		}
		rec := tc.NewDenormalizerRecordBatch()
		_ = rec.NumRows()
		rec.Release()
	}
	b.StopTimer()

	var msAfter runtime.MemStats
	runtime.ReadMemStats(&msAfter)
	totalMsgs := float64(b.N) * float64(nMsgs)
	b.ReportMetric(float64(b.Elapsed().Nanoseconds())/float64(b.N)/float64(nMsgs), "ns/msg")
	b.ReportMetric(float64(b.N*nMsgs)/b.Elapsed().Seconds(), "msg/s")
	b.ReportMetric(float64(msAfter.TotalAlloc-msBefore.TotalAlloc)/totalMsgs, "B/msg")
	b.ReportMetric(float64(msAfter.Mallocs-msBefore.Mallocs)/totalMsgs, "allocs/msg")
}

// ══════════════════════════════════════════════════════════════════════
// Benchmark: BidRequest — HyperpbRaw (no PGO) via AppendDenormRaw
// ══════════════════════════════════════════════════════════════════════
//
// Uses AppendDenormRaw to unmarshal raw proto bytes via hyperpb's compiled
// parser + Shared memory reuse, without profile-guided optimization.
// Runs sub-benchmarks on both the random corpus and the production-realistic
// corpus for comparison with other benchmarks.
func BenchmarkAppendBidRequest_HyperpbRaw(b *testing.B) {
	for _, tt := range []struct {
		name   string
		corpus func(b *testing.B) [][]byte
	}{
		{"Random", func(b *testing.B) [][]byte { return benchBidRequestCorpus(b) }},
		{"Realistic", func(b *testing.B) [][]byte { return benchRealisticBidRequestCorpus(b, 506) }},
	} {
		b.Run(tt.name, func(b *testing.B) {
			corpus := tt.corpus(b)

			md := (&samples.BidRequestEvent{}).ProtoReflect().Descriptor()
			ht := NewHyperType(md)
			tc, err := New(md, memory.DefaultAllocator,
				WithHyperType(ht),
				WithDenormalizerPlan(
					pbpath.PlanPath("id", pbpath.Alias("bidreq_id")),
					pbpath.PlanPath("device_id",
						pbpath.WithExpr(pbpath.FuncCoalesce(
							pbpath.PathRef("user.id"),
							pbpath.PathRef("site.id"),
							pbpath.PathRef("device.ifa"),
						)),
						pbpath.Alias("device_id"),
					),
					pbpath.PlanPath("site.publisher.id", pbpath.Alias("pub_id")),
					pbpath.PlanPath("technicalprovider.id", pbpath.Alias("tp_id")),
					pbpath.PlanPath("timestamp.seconds", pbpath.Alias("event_time")),
					pbpath.PlanPath("user.ext.demographic.total.units", pbpath.Alias("imp_units")),
					pbpath.PlanPath("user.ext.demographic.total.nanos", pbpath.Alias("imp_nanos")),
					pbpath.PlanPath("width",
						pbpath.WithExpr(pbpath.FuncCoalesce(
							pbpath.PathRef("imp[0].banner.w"),
							pbpath.PathRef("imp[0].video.w"),
						)),
						pbpath.Alias("width"),
					),
					pbpath.PlanPath("height",
						pbpath.WithExpr(pbpath.FuncCoalesce(
							pbpath.PathRef("imp[0].banner.h"),
							pbpath.PathRef("imp[0].video.h"),
						)),
						pbpath.Alias("height"),
					),
					pbpath.PlanPath("imp[0].pmp.deals[*].id", pbpath.Alias("deal")),
				),
			)
			if err != nil {
				b.Fatalf("New with denorm plan: %v", err)
			}
			defer tc.Release()

			nMsgs := len(corpus)

			b.ReportAllocs()
			b.SetBytes(int64(totalCorpusBytes(corpus)))

			runtime.GC()
			var msBefore runtime.MemStats
			runtime.ReadMemStats(&msBefore)

			b.ResetTimer()

			for b.Loop() {
				for _, raw := range corpus {
					if err := tc.AppendDenormRaw(raw); err != nil {
						b.Fatal(err)
					}
				}
				rec := tc.NewDenormalizerRecordBatch()
				_ = rec.NumRows()
				rec.Release()
			}
			b.StopTimer()

			var msAfter runtime.MemStats
			runtime.ReadMemStats(&msAfter)
			totalMsgs := float64(b.N) * float64(nMsgs)
			b.ReportMetric(float64(b.Elapsed().Nanoseconds())/float64(b.N)/float64(nMsgs), "ns/msg")
			b.ReportMetric(float64(b.N*nMsgs)/b.Elapsed().Seconds(), "msg/s")
			b.ReportMetric(float64(msAfter.TotalAlloc-msBefore.TotalAlloc)/totalMsgs, "B/msg")
			b.ReportMetric(float64(msAfter.Mallocs-msBefore.Mallocs)/totalMsgs, "allocs/msg")
		})
	}
}

// ══════════════════════════════════════════════════════════════════════
// Benchmark: BidRequest — HyperpbPGO via AppendDenormRaw with profile
// ══════════════════════════════════════════════════════════════════════
//
// Uses a consistent-shape corpus (uniform field population) that gives
// hyperpb's PGO accurate predictions for repeated-field sizes and field
// presence. Profiles the corpus first with 100% sampling, recompiles,
// then benchmarks with the optimized parser.
func BenchmarkAppendBidRequest_HyperpbPGO(b *testing.B) {
	corpus := benchRealisticBidRequestCorpus(b, 506)

	md := (&samples.BidRequestEvent{}).ProtoReflect().Descriptor()
	ht := NewHyperType(md, WithAutoRecompile(0, 1.0)) // manual recompile, 100% sampling

	tc, err := New(md, memory.DefaultAllocator,
		WithHyperType(ht),
		WithDenormalizerPlan(
			pbpath.PlanPath("id", pbpath.Alias("bidreq_id")),
			pbpath.PlanPath("device_id",
				pbpath.WithExpr(pbpath.FuncCoalesce(
					pbpath.PathRef("user.id"),
					pbpath.PathRef("site.id"),
					pbpath.PathRef("device.ifa"),
				)),
				pbpath.Alias("device_id"),
			),
			pbpath.PlanPath("site.publisher.id", pbpath.Alias("pub_id")),
			pbpath.PlanPath("technicalprovider.id", pbpath.Alias("tp_id")),
			pbpath.PlanPath("timestamp.seconds", pbpath.Alias("event_time")),
			pbpath.PlanPath("user.ext.demographic.total.units", pbpath.Alias("imp_units")),
			pbpath.PlanPath("user.ext.demographic.total.nanos", pbpath.Alias("imp_nanos")),
			pbpath.PlanPath("width",
				pbpath.WithExpr(pbpath.FuncCoalesce(
					pbpath.PathRef("imp[0].banner.w"),
					pbpath.PathRef("imp[0].video.w"),
				)),
				pbpath.Alias("width"),
			),
			pbpath.PlanPath("height",
				pbpath.WithExpr(pbpath.FuncCoalesce(
					pbpath.PathRef("imp[0].banner.h"),
					pbpath.PathRef("imp[0].video.h"),
				)),
				pbpath.Alias("height"),
			),
			pbpath.PlanPath("imp[0].pmp.deals[*].id", pbpath.Alias("deal")),
		),
	)
	if err != nil {
		b.Fatalf("New with denorm plan: %v", err)
	}
	defer tc.Release()

	// Profile the entire corpus with 100% sampling rate.
	for _, raw := range corpus {
		if err := tc.AppendDenormRaw(raw); err != nil {
			b.Fatalf("profiling pass: %v", err)
		}
	}
	rec := tc.NewDenormalizerRecordBatch()
	rec.Release()

	// Recompile with the collected profile.
	if err := ht.Recompile(); err != nil {
		b.Fatalf("Recompile: %v", err)
	}

	nMsgs := len(corpus)

	b.ReportAllocs()
	b.SetBytes(int64(totalCorpusBytes(corpus)))

	runtime.GC()
	var msBefore runtime.MemStats
	runtime.ReadMemStats(&msBefore)

	b.ResetTimer()

	for b.Loop() {
		for _, raw := range corpus {
			if err := tc.AppendDenormRaw(raw); err != nil {
				b.Fatal(err)
			}
		}
		rec := tc.NewDenormalizerRecordBatch()
		_ = rec.NumRows()
		rec.Release()
	}
	b.StopTimer()

	var msAfter runtime.MemStats
	runtime.ReadMemStats(&msAfter)
	totalMsgs := float64(b.N) * float64(nMsgs)
	b.ReportMetric(float64(b.Elapsed().Nanoseconds())/float64(b.N)/float64(nMsgs), "ns/msg")
	b.ReportMetric(float64(b.N*nMsgs)/b.Elapsed().Seconds(), "msg/s")
	b.ReportMetric(float64(msAfter.TotalAlloc-msBefore.TotalAlloc)/totalMsgs, "B/msg")
	b.ReportMetric(float64(msAfter.Mallocs-msBefore.Mallocs)/totalMsgs, "allocs/msg")
}

// benchRealisticBidRequestCorpus generates n BidRequestEvent messages that
// match production traffic shape derived from analysis of bidreq_raw.parquet:
//
//   - Imp count: 75% have 2, 18% have 1, 7% have 3+ (production median: 2)
//   - Banner/video: both structs always present; 55% video-primary (banner 0×0),
//     45% banner-primary (video 0×0) — mutually exclusive real dimensions
//   - Deals on imp[0]: 61% have 1, 21% have 2, 6% have 3, 2% have 0, 10% have 4+
//   - All top-level fields always present: user, device, site, tp, timestamp,
//     dooh, geo, user.ext, publisher — 100% non-null in production
//   - Cur: 92% have 1 entry
//   - TP IDs: ~80 distinct values, range 11–530
//
// The distribution is deterministic (seeded) so PGO profiles are stable.
func benchRealisticBidRequestCorpus(b *testing.B, n int) [][]byte {
	b.Helper()
	raw := make([][]byte, n)

	// Fixed pools matching production cardinality
	tpIDs := []uint32{11, 15, 22, 35, 42, 55, 67, 80, 99, 110, 125, 150, 175, 200, 250, 300, 350, 400, 450, 530}
	pubIDs := []string{"pub-001", "pub-002", "pub-003", "pub-004", "pub-005", "pub-010", "pub-020", "pub-050", "pub-100"}
	siteNames := []string{"ExampleNews", "SportsBlog", "TechReview", "WeatherApp", "GamingHub"}
	countries := []string{"US", "CA", "GB", "DE", "FR", "AU", "JP"}
	regions := []string{"CA", "NY", "TX", "IL", "FL", "ON", "BC"}

	// Common ad sizes from production
	bannerSizes := [][2]uint32{{1080, 1920}, {1920, 1080}, {1024, 555}, {1024, 768}, {1400, 400}, {1600, 900}, {840, 400}}
	videoSizes := [][2]uint32{{1080, 1920}, {1920, 1080}, {720, 1280}, {2160, 3840}, {1024, 555}}

	for i := range n {
		// Deterministic distribution based on index
		var impCount int
		switch {
		case i%100 < 75:
			impCount = 2 // 75%
		case i%100 < 93:
			impCount = 1 // 18%
		case i%100 < 99:
			impCount = 3 // 6%
		default:
			impCount = 4 // 1%
		}

		// Build impressions
		imps := make([]*samples.BidRequestEvent_ImpressionEvent, impCount)
		for j := range impCount {
			var banner *samples.BidRequestEvent_ImpressionEvent_BannerEvent
			var video *samples.BidRequestEvent_ImpressionEvent_VideoEvent

			if (i+j)%100 < 55 {
				// Video-primary (55%): real video, zero banner
				vs := videoSizes[(i+j)%len(videoSizes)]
				banner = &samples.BidRequestEvent_ImpressionEvent_BannerEvent{W: 0, H: 0}
				video = &samples.BidRequestEvent_ImpressionEvent_VideoEvent{W: vs[0], H: vs[1]}
			} else {
				// Banner-primary (45%): real banner, zero video
				bs := bannerSizes[(i+j)%len(bannerSizes)]
				banner = &samples.BidRequestEvent_ImpressionEvent_BannerEvent{W: bs[0], H: bs[1]}
				video = &samples.BidRequestEvent_ImpressionEvent_VideoEvent{W: 0, H: 0}
			}

			// Deal count distribution
			var dealCount int
			switch {
			case (i+j)%100 < 2:
				dealCount = 0 // 2%
			case (i+j)%100 < 63:
				dealCount = 1 // 61%
			case (i+j)%100 < 84:
				dealCount = 2 // 21%
			case (i+j)%100 < 90:
				dealCount = 3 // 6%
			default:
				dealCount = 4 // 10%
			}

			deals := make([]*samples.BidRequestEvent_ImpressionEvent_PrivateMarketplaceEvent_DealEvent, dealCount)
			for k := range dealCount {
				deals[k] = &samples.BidRequestEvent_ImpressionEvent_PrivateMarketplaceEvent_DealEvent{
					Id:       fmt.Sprintf("deal-%06d-%d-%d", i, j, k),
					Bidfloor: &custom.DecimalValue{Units: int64(k + 1)},
					At:       uint32(k%3 + 1),
				}
			}

			imps[j] = &samples.BidRequestEvent_ImpressionEvent{
				Id:       fmt.Sprintf("imp-%06d-%d", i, j),
				Banner:   banner,
				Video:    video,
				Bidfloor: &custom.DecimalValue{Units: int64(i%10 + 1), Nanos: 500000000},
				Pmp: &samples.BidRequestEvent_ImpressionEvent_PrivateMarketplaceEvent{
					Deals: deals,
				},
			}
		}

		m := &samples.BidRequestEvent{
			Id: fmt.Sprintf("req-%06d", i),
			User: &samples.BidRequestEvent_UserEvent{
				Id: fmt.Sprintf("user-%06d", i%1000),
				Ext: &samples.BidRequestEvent_UserEvent_UserExt{
					Demographic: &samples.BidRequestEvent_UserEvent_UserExt_DemographicEvent{
						Total: &custom.DecimalValue{Units: int64(i % 100), Nanos: int32(i % 1000000000)},
					},
				},
			},
			Device: &samples.BidRequestEvent_DeviceEvent{
				Ifa: fmt.Sprintf("ifa-%06d", i%5000),
				W:   1920,
				H:   1080,
				Geo: &samples.BidRequestEvent_DeviceEvent_GeoEvent{
					Country: countries[i%len(countries)],
					Region:  regions[i%len(regions)],
				},
			},
			Imp: imps,
			Site: &samples.BidRequestEvent_SiteEvent{
				Id:        fmt.Sprintf("site-%06d", i%500),
				Name:      siteNames[i%len(siteNames)],
				Publisher: &samples.BidRequestEvent_SiteEvent_SitePublisher{Id: proto.String(pubIDs[i%len(pubIDs)])},
			},
			Cur:               []string{"USD"},
			Timestamp:         &timestamppb.Timestamp{Seconds: 1700000000 + int64(i), Nanos: int32(i % 1000000000)},
			Technicalprovider: &samples.BidRequestEvent_TechnicalProviderEvent{Id: tpIDs[i%len(tpIDs)], Name: "TP"},
			Dooh: &samples.BidRequestEvent_BidRequestDoohEvent{
				Id: fmt.Sprintf("dooh-%06d", i%100), Name: "Screen",
			},
		}
		bs, err := proto.Marshal(m)
		if err != nil {
			b.Fatalf("marshal realistic %d: %v", i, err)
		}
		raw[i] = bs
	}
	return raw
}

// totalCorpusBytes returns the sum of all byte slice lengths.
func totalCorpusBytes(corpus [][]byte) int {
	n := 0
	for _, b := range corpus {
		n += len(b)
	}
	return n
}

// ══════════════════════════════════════════════════════════════════════
// Benchmark: AppendRaw (non-denorm, HyperType fast path)
// ══════════════════════════════════════════════════════════════════════
//
// Measures the primary raw-bytes ingestion path used by the Python FFI
// layer: HyperType compiled parser → Arrow record builder → flush.
// No denormalization involved.

func BenchmarkAppendRaw(b *testing.B) {
	for _, tt := range []struct {
		name   string
		corpus func(b *testing.B) [][]byte
	}{
		{"Random", func(b *testing.B) [][]byte { return benchBidRequestCorpus(b) }},
		{"Realistic", func(b *testing.B) [][]byte { return benchRealisticBidRequestCorpus(b, 506) }},
	} {
		b.Run(tt.name, func(b *testing.B) {
			corpus := tt.corpus(b)

			md := (&samples.BidRequestEvent{}).ProtoReflect().Descriptor()
			ht := NewHyperType(md)
			tc, err := New(md, memory.DefaultAllocator, WithHyperType(ht))
			if err != nil {
				b.Fatalf("New: %v", err)
			}
			defer tc.Release()

			nMsgs := len(corpus)

			b.ReportAllocs()
			b.SetBytes(int64(totalCorpusBytes(corpus)))

			runtime.GC()
			var msBefore runtime.MemStats
			runtime.ReadMemStats(&msBefore)

			b.ResetTimer()

			for b.Loop() {
				for _, raw := range corpus {
					if err := tc.AppendRaw(raw); err != nil {
						b.Fatal(err)
					}
				}
				rec := tc.NewRecordBatch()
				_ = rec.NumRows()
				rec.Release()
			}
			b.StopTimer()

			var msAfter runtime.MemStats
			runtime.ReadMemStats(&msAfter)
			totalMsgs := float64(b.N) * float64(nMsgs)
			b.ReportMetric(float64(b.Elapsed().Nanoseconds())/float64(b.N)/float64(nMsgs), "ns/msg")
			b.ReportMetric(float64(b.N*nMsgs)/b.Elapsed().Seconds(), "msg/s")
			b.ReportMetric(float64(msAfter.TotalAlloc-msBefore.TotalAlloc)/totalMsgs, "B/msg")
			b.ReportMetric(float64(msAfter.Mallocs-msBefore.Mallocs)/totalMsgs, "allocs/msg")
		})
	}
}

// ══════════════════════════════════════════════════════════════════════
// Benchmark: AppendRawMerged (base + custom bytes, HyperType & fallback)
// ══════════════════════════════════════════════════════════════════════
//
// Measures the merged-bytes path added for the Python bindings plan.
// BidRequestEvent is the base, CustomFields provides extra columns.
// Compares HyperType fast path vs dynamicpb fallback.

func BenchmarkAppendRawMerged(b *testing.B) {
	protoDir := benchProtoDir()

	// Build custom message descriptor.
	customFD, err := CompileProtoToFileDescriptor("custom_fields.proto", []string{protoDir})
	if err != nil {
		b.Fatalf("compile custom_fields.proto: %v", err)
	}
	customMD, err := GetMessageDescriptorByName(customFD, "CustomFields")
	if err != nil {
		b.Fatalf("get CustomFields: %v", err)
	}

	// Pre-generate custom bytes (same for every message).
	customMsg := dynamicpb.NewMessage(customMD)
	customMsg.Set(customMD.Fields().ByName("event_timestamp"), protoreflect.ValueOfInt64(1700000000))
	customMsg.Set(customMD.Fields().ByName("source_id"), protoreflect.ValueOfString("bench-source"))
	customBytes, err := proto.Marshal(customMsg)
	if err != nil {
		b.Fatalf("marshal custom: %v", err)
	}

	baseMD := (&samples.BidRequestEvent{}).ProtoReflect().Descriptor()
	corpus := benchRealisticBidRequestCorpus(b, 506)

	b.Run("HyperType", func(b *testing.B) {
		mergedMD, err := MergeMessageDescriptors(baseMD, customMD, "BidRequestMerged")
		if err != nil {
			b.Fatalf("merge: %v", err)
		}
		ht := NewHyperType(mergedMD)

		tc, err := New(baseMD, memory.DefaultAllocator,
			WithCustomMessage(customMD),
			WithHyperType(ht),
		)
		if err != nil {
			b.Fatalf("New: %v", err)
		}
		defer tc.Release()

		nMsgs := len(corpus)

		b.ReportAllocs()
		b.SetBytes(int64(totalCorpusBytes(corpus)) + int64(nMsgs*len(customBytes)))

		runtime.GC()
		var msBefore runtime.MemStats
		runtime.ReadMemStats(&msBefore)

		b.ResetTimer()

		for b.Loop() {
			for _, raw := range corpus {
				if err := tc.AppendRawMerged(raw, customBytes); err != nil {
					b.Fatal(err)
				}
			}
			rec := tc.NewRecordBatch()
			_ = rec.NumRows()
			rec.Release()
		}
		b.StopTimer()

		var msAfter runtime.MemStats
		runtime.ReadMemStats(&msAfter)
		totalMsgs := float64(b.N) * float64(nMsgs)
		b.ReportMetric(float64(b.Elapsed().Nanoseconds())/float64(b.N)/float64(nMsgs), "ns/msg")
		b.ReportMetric(float64(b.N*nMsgs)/b.Elapsed().Seconds(), "msg/s")
		b.ReportMetric(float64(msAfter.TotalAlloc-msBefore.TotalAlloc)/totalMsgs, "B/msg")
		b.ReportMetric(float64(msAfter.Mallocs-msBefore.Mallocs)/totalMsgs, "allocs/msg")
	})

	b.Run("Fallback", func(b *testing.B) {
		tc, err := New(baseMD, memory.DefaultAllocator,
			WithCustomMessage(customMD),
		)
		if err != nil {
			b.Fatalf("New: %v", err)
		}
		defer tc.Release()

		nMsgs := len(corpus)

		b.ReportAllocs()
		b.SetBytes(int64(totalCorpusBytes(corpus)) + int64(nMsgs*len(customBytes)))

		runtime.GC()
		var msBefore runtime.MemStats
		runtime.ReadMemStats(&msBefore)

		b.ResetTimer()

		for b.Loop() {
			for _, raw := range corpus {
				if err := tc.AppendRawMerged(raw, customBytes); err != nil {
					b.Fatal(err)
				}
			}
			rec := tc.NewRecordBatch()
			_ = rec.NumRows()
			rec.Release()
		}
		b.StopTimer()

		var msAfter runtime.MemStats
		runtime.ReadMemStats(&msAfter)
		totalMsgs := float64(b.N) * float64(nMsgs)
		b.ReportMetric(float64(b.Elapsed().Nanoseconds())/float64(b.N)/float64(nMsgs), "ns/msg")
		b.ReportMetric(float64(b.N*nMsgs)/b.Elapsed().Seconds(), "msg/s")
		b.ReportMetric(float64(msAfter.TotalAlloc-msBefore.TotalAlloc)/totalMsgs, "B/msg")
		b.ReportMetric(float64(msAfter.Mallocs-msBefore.Mallocs)/totalMsgs, "allocs/msg")
	})
}

// ══════════════════════════════════════════════════════════════════════
// Benchmark: AppendDenormRawMerged (merged bytes through denorm engine)
// ══════════════════════════════════════════════════════════════════════
//
// Combines byte-concatenation merge with denormalization fan-out.
// Uses BidRequestEvent + CustomFields, with a denorm plan that includes
// both base and custom columns.

func BenchmarkAppendDenormRawMerged(b *testing.B) {
	protoDir := benchProtoDir()

	// Build custom message descriptor.
	customFD, err := CompileProtoToFileDescriptor("custom_fields.proto", []string{protoDir})
	if err != nil {
		b.Fatalf("compile custom_fields.proto: %v", err)
	}
	customMD, err := GetMessageDescriptorByName(customFD, "CustomFields")
	if err != nil {
		b.Fatalf("get CustomFields: %v", err)
	}

	// Pre-generate custom bytes.
	customMsg := dynamicpb.NewMessage(customMD)
	customMsg.Set(customMD.Fields().ByName("event_timestamp"), protoreflect.ValueOfInt64(1700000000))
	customMsg.Set(customMD.Fields().ByName("source_id"), protoreflect.ValueOfString("bench-source"))
	customBytes, err := proto.Marshal(customMsg)
	if err != nil {
		b.Fatalf("marshal custom: %v", err)
	}

	baseMD := (&samples.BidRequestEvent{}).ProtoReflect().Descriptor()
	corpus := benchRealisticBidRequestCorpus(b, 506)

	// Merged descriptor for HyperType.
	mergedMD, err := MergeMessageDescriptors(baseMD, customMD, "BidRequestDenormMerged")
	if err != nil {
		b.Fatalf("merge: %v", err)
	}
	ht := NewHyperType(mergedMD)

	tc, err := New(baseMD, memory.DefaultAllocator,
		WithCustomMessage(customMD),
		WithHyperType(ht),
		WithDenormalizerPlan(
			pbpath.PlanPath("id", pbpath.Alias("bidreq_id")),
			pbpath.PlanPath("device_id",
				pbpath.WithExpr(pbpath.FuncCoalesce(
					pbpath.PathRef("user.id"),
					pbpath.PathRef("site.id"),
					pbpath.PathRef("device.ifa"),
				)),
				pbpath.Alias("device_id"),
			),
			pbpath.PlanPath("site.publisher.id", pbpath.Alias("pub_id")),
			pbpath.PlanPath("technicalprovider.id", pbpath.Alias("tp_id")),
			pbpath.PlanPath("timestamp.seconds", pbpath.Alias("event_time")),
			pbpath.PlanPath("event_timestamp", pbpath.Alias("custom_ts")),
			pbpath.PlanPath("source_id", pbpath.Alias("custom_src")),
			pbpath.PlanPath("imp[0].pmp.deals[*].id", pbpath.Alias("deal")),
		),
	)
	if err != nil {
		b.Fatalf("New with denorm plan: %v", err)
	}
	defer tc.Release()

	nMsgs := len(corpus)

	b.ReportAllocs()
	b.SetBytes(int64(totalCorpusBytes(corpus)) + int64(nMsgs*len(customBytes)))

	runtime.GC()
	var msBefore runtime.MemStats
	runtime.ReadMemStats(&msBefore)

	b.ResetTimer()

	for b.Loop() {
		for _, raw := range corpus {
			if err := tc.AppendDenormRawMerged(raw, customBytes); err != nil {
				b.Fatal(err)
			}
		}
		rec := tc.NewDenormalizerRecordBatch()
		_ = rec.NumRows()
		rec.Release()
	}
	b.StopTimer()

	var msAfter runtime.MemStats
	runtime.ReadMemStats(&msAfter)
	totalMsgs := float64(b.N) * float64(nMsgs)
	b.ReportMetric(float64(b.Elapsed().Nanoseconds())/float64(b.N)/float64(nMsgs), "ns/msg")
	b.ReportMetric(float64(b.N*nMsgs)/b.Elapsed().Seconds(), "msg/s")
	b.ReportMetric(float64(msAfter.TotalAlloc-msBefore.TotalAlloc)/totalMsgs, "B/msg")
	b.ReportMetric(float64(msAfter.Mallocs-msBefore.Mallocs)/totalMsgs, "allocs/msg")
}

// ══════════════════════════════════════════════════════════════════════
// Benchmark: AppendWithCustom (merge-via-marshal path)
// ══════════════════════════════════════════════════════════════════════
//
// Measures the most allocation-heavy Append variant: proto.Clone +
// Marshal×2 + Unmarshal×2 per message. This is the only path that
// involves four proto serialization operations per call.

func BenchmarkAppendWithCustom(b *testing.B) {
	protoDir := benchProtoDir()

	customFD, err := CompileProtoToFileDescriptor("custom_fields.proto", []string{protoDir})
	if err != nil {
		b.Fatalf("compile custom_fields.proto: %v", err)
	}
	customMD, err := GetMessageDescriptorByName(customFD, "CustomFields")
	if err != nil {
		b.Fatalf("get CustomFields: %v", err)
	}

	// Use ScalarTypes (no well-known types) to avoid MergeMessageDescriptors
	// resolution failures with google.protobuf.Timestamp.
	baseMD := (&samples.ScalarTypes{}).ProtoReflect().Descriptor()

	const N = 100
	msgs := generateScalarMessages(b, N)

	customMsg := dynamicpb.NewMessage(customMD)
	customMsg.Set(customMD.Fields().ByName("event_timestamp"), protoreflect.ValueOfInt64(1700000000))
	customMsg.Set(customMD.Fields().ByName("source_id"), protoreflect.ValueOfString("bench-source"))

	tc, err := New(baseMD, memory.DefaultAllocator, WithCustomMessage(customMD))
	if err != nil {
		b.Fatalf("New: %v", err)
	}
	defer tc.Release()

	nMsgs := len(msgs)

	b.ReportAllocs()

	runtime.GC()
	var msBefore runtime.MemStats
	runtime.ReadMemStats(&msBefore)

	b.ResetTimer()

	for b.Loop() {
		for _, m := range msgs {
			if err := tc.AppendWithCustom(m, customMsg); err != nil {
				b.Fatal(err)
			}
		}
		rec := tc.NewRecordBatch()
		_ = rec.NumRows()
		rec.Release()
	}
	b.StopTimer()

	var msAfter runtime.MemStats
	runtime.ReadMemStats(&msAfter)
	totalMsgs := float64(b.N) * float64(nMsgs)
	b.ReportMetric(float64(b.Elapsed().Nanoseconds())/float64(b.N)/float64(nMsgs), "ns/msg")
	b.ReportMetric(float64(b.N*nMsgs)/b.Elapsed().Seconds(), "msg/s")
	b.ReportMetric(float64(msAfter.TotalAlloc-msBefore.TotalAlloc)/totalMsgs, "B/msg")
	b.ReportMetric(float64(msAfter.Mallocs-msBefore.Mallocs)/totalMsgs, "allocs/msg")
}

// ══════════════════════════════════════════════════════════════════════
// Benchmark: AppendDenormRaw fallback (dynamicpb, no HyperType)
// ══════════════════════════════════════════════════════════════════════
//
// Measures the dynamicpb fallback path of AppendDenormRaw when no
// HyperType is configured. Each call allocates a dynamicpb.NewMessage
// + proto.Unmarshal. Compared against BenchmarkAppendBidRequest_HyperpbRaw
// to quantify the HyperType/Shared speedup.

func BenchmarkAppendDenormRaw_Fallback(b *testing.B) {
	corpus := benchRealisticBidRequestCorpus(b, 506)

	md := (&samples.BidRequestEvent{}).ProtoReflect().Descriptor()
	tc, err := New(md, memory.DefaultAllocator,
		// No WithHyperType — forces dynamicpb fallback.
		WithDenormalizerPlan(
			pbpath.PlanPath("id", pbpath.Alias("bidreq_id")),
			pbpath.PlanPath("device_id",
				pbpath.WithExpr(pbpath.FuncCoalesce(
					pbpath.PathRef("user.id"),
					pbpath.PathRef("site.id"),
					pbpath.PathRef("device.ifa"),
				)),
				pbpath.Alias("device_id"),
			),
			pbpath.PlanPath("site.publisher.id", pbpath.Alias("pub_id")),
			pbpath.PlanPath("technicalprovider.id", pbpath.Alias("tp_id")),
			pbpath.PlanPath("timestamp.seconds", pbpath.Alias("event_time")),
			pbpath.PlanPath("imp[0].pmp.deals[*].id", pbpath.Alias("deal")),
		),
	)
	if err != nil {
		b.Fatalf("New with denorm plan: %v", err)
	}
	defer tc.Release()

	nMsgs := len(corpus)

	b.ReportAllocs()
	b.SetBytes(int64(totalCorpusBytes(corpus)))

	runtime.GC()
	var msBefore runtime.MemStats
	runtime.ReadMemStats(&msBefore)

	b.ResetTimer()

	for b.Loop() {
		for _, raw := range corpus {
			if err := tc.AppendDenormRaw(raw); err != nil {
				b.Fatal(err)
			}
		}
		rec := tc.NewDenormalizerRecordBatch()
		_ = rec.NumRows()
		rec.Release()
	}
	b.StopTimer()

	var msAfter runtime.MemStats
	runtime.ReadMemStats(&msAfter)
	totalMsgs := float64(b.N) * float64(nMsgs)
	b.ReportMetric(float64(b.Elapsed().Nanoseconds())/float64(b.N)/float64(nMsgs), "ns/msg")
	b.ReportMetric(float64(b.N*nMsgs)/b.Elapsed().Seconds(), "msg/s")
	b.ReportMetric(float64(msAfter.TotalAlloc-msBefore.TotalAlloc)/totalMsgs, "B/msg")
	b.ReportMetric(float64(msAfter.Mallocs-msBefore.Mallocs)/totalMsgs, "allocs/msg")
}

// ══════════════════════════════════════════════════════════════════════
// Benchmark: Concurrent Clone + AppendDenormRaw (goroutine fan-out)
// ══════════════════════════════════════════════════════════════════════
//
// Measures the documented multi-goroutine pattern: Clone a base Transcoder,
// fan out across GOMAXPROCS workers, each processing a shard of the corpus,
// then flush independently. Verifies that Clone + shared HyperType scales
// under concurrent use.

func BenchmarkConcurrent_CloneAppendDenormRaw(b *testing.B) {
	corpus := benchRealisticBidRequestCorpus(b, 506)

	md := (&samples.BidRequestEvent{}).ProtoReflect().Descriptor()
	ht := NewHyperType(md)
	base, err := New(md, memory.DefaultAllocator,
		WithHyperType(ht),
		WithDenormalizerPlan(
			pbpath.PlanPath("id", pbpath.Alias("bidreq_id")),
			pbpath.PlanPath("site.publisher.id", pbpath.Alias("pub_id")),
			pbpath.PlanPath("technicalprovider.id", pbpath.Alias("tp_id")),
			pbpath.PlanPath("timestamp.seconds", pbpath.Alias("event_time")),
			pbpath.PlanPath("imp[0].pmp.deals[*].id", pbpath.Alias("deal")),
		),
	)
	if err != nil {
		b.Fatalf("New: %v", err)
	}
	defer base.Release()

	workers := runtime.GOMAXPROCS(0)
	if workers < 2 {
		workers = 2
	}

	// Shard the corpus evenly across workers.
	shards := make([][][]byte, workers)
	for i, raw := range corpus {
		shards[i%workers] = append(shards[i%workers], raw)
	}

	// Pre-create one long-lived clone per worker (real-world pattern:
	// clones are allocated once at startup, not per-batch).
	clones := make([]*Transcoder, workers)
	for w := range workers {
		c, err := base.Clone(memory.DefaultAllocator)
		if err != nil {
			b.Fatalf("Clone worker %d: %v", w, err)
		}
		clones[w] = c
	}
	defer func() {
		for _, c := range clones {
			c.Release()
		}
	}()

	nMsgs := len(corpus)
	b.ReportAllocs()
	b.SetBytes(int64(totalCorpusBytes(corpus)))

	runtime.GC()
	var msBefore runtime.MemStats
	runtime.ReadMemStats(&msBefore)

	b.ResetTimer()

	for b.Loop() {
		var wg sync.WaitGroup
		wg.Add(workers)
		for w := range workers {
			go func(tc *Transcoder, shard [][]byte) {
				defer wg.Done()
				for _, raw := range shard {
					if err := tc.AppendDenormRaw(raw); err != nil {
						b.Error(err)
						return
					}
				}
				rec := tc.NewDenormalizerRecordBatch()
				_ = rec.NumRows()
				rec.Release()
			}(clones[w], shards[w])
		}
		wg.Wait()
	}
	b.StopTimer()

	var msAfter runtime.MemStats
	runtime.ReadMemStats(&msAfter)
	totalMsgs := float64(b.N) * float64(nMsgs)
	b.ReportMetric(float64(b.Elapsed().Nanoseconds())/float64(b.N)/float64(nMsgs), "ns/msg")
	b.ReportMetric(float64(b.N*nMsgs)/b.Elapsed().Seconds(), "msg/s")
	b.ReportMetric(float64(msAfter.TotalAlloc-msBefore.TotalAlloc)/totalMsgs, "B/msg")
	b.ReportMetric(float64(msAfter.Mallocs-msBefore.Mallocs)/totalMsgs, "allocs/msg")
}

// ══════════════════════════════════════════════════════════════════════
// Benchmark: Batch size scaling (AppendRaw + flush at different sizes)
// ══════════════════════════════════════════════════════════════════════
//
// Reveals per-call overhead vs amortized flush cost across batch sizes.
// Key alignment targets:
//   - 122880: DuckDB default row group size — align flushes to this for
//     zero-copy Arrow→Parquet pipelines without mid-group splits.
//   - 245760: 2× that, useful when downstream consumers read two row groups
//     at a time or when network batching is the limiting factor.
// Production systems should align their flush cadence with whatever size
// the next stage of the pipeline (Parquet writer, DuckDB scanner, object
// storage PUT boundary) consumes most efficiently.

func BenchmarkAppendRaw_BatchSize(b *testing.B) {
	for _, batchSize := range []int{1, 100, 1_000, 10_000, 122_880, 245_760} {
		b.Run(fmt.Sprintf("%d", batchSize), func(b *testing.B) {
			corpus := benchRealisticBidRequestCorpus(b, batchSize)

			md := (&samples.BidRequestEvent{}).ProtoReflect().Descriptor()
			ht := NewHyperType(md)
			tc, err := New(md, memory.DefaultAllocator, WithHyperType(ht))
			if err != nil {
				b.Fatalf("New: %v", err)
			}
			defer tc.Release()

			nMsgs := len(corpus)

			b.ReportAllocs()
			b.SetBytes(int64(totalCorpusBytes(corpus)))

			runtime.GC()
			var msBefore runtime.MemStats
			runtime.ReadMemStats(&msBefore)

			b.ResetTimer()

			for b.Loop() {
				for _, raw := range corpus {
					if err := tc.AppendRaw(raw); err != nil {
						b.Fatal(err)
					}
				}
				rec := tc.NewRecordBatch()
				_ = rec.NumRows()
				rec.Release()
			}
			b.StopTimer()

			var msAfter runtime.MemStats
			runtime.ReadMemStats(&msAfter)
			totalMsgs := float64(b.N) * float64(nMsgs)
			b.ReportMetric(float64(b.Elapsed().Nanoseconds())/float64(b.N)/float64(nMsgs), "ns/msg")
			b.ReportMetric(float64(b.N*nMsgs)/b.Elapsed().Seconds(), "msg/s")
			b.ReportMetric(float64(msAfter.TotalAlloc-msBefore.TotalAlloc)/totalMsgs, "B/msg")
			b.ReportMetric(float64(msAfter.Mallocs-msBefore.Mallocs)/totalMsgs, "allocs/msg")
		})
	}
}

// ══════════════════════════════════════════════════════════════════════
// Benchmark: Batch size × worker count (2-D sweep)
// ══════════════════════════════════════════════════════════════════════
//
// BenchmarkAppendRaw_BatchSize (above) holds workers=1 constant to isolate
// the flush-amortisation effect. This benchmark holds the total corpus size
// at 122880 messages (one DuckDB row group) and sweeps both batch size and
// worker count simultaneously so you can see how the two levers interact:
//
//   - Small batches with many workers: workers idle waiting for tiny shards,
//     goroutine-launch overhead dominates.
//   - Large batches with few workers: approaches single-threaded throughput;
//     flush amortisation is maximal but parallelism is unused.
//   - Target: batch size ≥ 1000, workers ≈ GOMAXPROCS (peak in most cases).
//
// Sub-benchmark name: workers-WW/batch-NNNNNN
func BenchmarkAppendRaw_BatchSizeWorkers(b *testing.B) {
	const totalMsgsTarget = 245_760 // 2 × DuckDB row group; keeps iteration work constant
	batchSizes := []int{100, 1_000, 10_000, 122_880, 245_760}

	md := (&samples.BidRequestEvent{}).ProtoReflect().Descriptor()
	// threshold=0 → manual recompile only; rate=1.0 → sample every message so
	// HyperType accumulates a full profile in the small warm-up pass.
	// After Recompile() the compiled parser is used for all subsequent calls;
	// ongoing sampling (at rate=1.0) then reflects realistic production overhead
	// where HyperType continues to collect profiling data.
	ht := NewHyperType(md, WithAutoRecompile(0, 1.0))

	// Build one corpus per batch size; reuse across worker sub-benchmarks.
	corpora := make(map[int][][]byte, len(batchSizes))
	for _, bs := range batchSizes {
		corpora[bs] = benchRealisticBidRequestCorpus(b, bs)
	}

	// PGO warm-up: 506 messages covers the full BidRequest distribution
	// (all variant fields rotate on i%100; 5× coverage is sufficient).
	// It is a separate corpus so sub-benchmark timing is not affected.
	warmCorpus := benchRealisticBidRequestCorpus(b, 506)

	base, err := New(md, memory.DefaultAllocator, WithHyperType(ht))
	if err != nil {
		b.Fatalf("New: %v", err)
	}
	defer base.Release()
	for _, raw := range warmCorpus {
		if err := base.AppendRaw(raw); err != nil {
			b.Fatalf("PGO warm-up: %v", err)
		}
	}
	warmRec := base.NewRecordBatch()
	warmRec.Release()
	if err := ht.Recompile(); err != nil {
		b.Fatalf("Recompile: %v", err)
	}

	maxProcs := runtime.GOMAXPROCS(0) * 2
	for _, workers := range uniqueSortedWorkerCounts(maxProcs) {
		for _, batchSize := range batchSizes {
			corpus := corpora[batchSize]
			// How many full batches do we need to reach ~totalMsgsTarget per iteration?
			nBatches := totalMsgsTarget / batchSize
			if nBatches < 1 {
				nBatches = 1
			}

			name := fmt.Sprintf("workers-%02d/batch-%07d", workers, batchSize)
			b.Run(name, func(b *testing.B) {
				// Each worker gets nBatches/workers full-batch passes.
				// Skip combinations where batchSize×workers > totalMsgsTarget:
				// promoting workerBatches to 1 breaks the constant-work invariant
				// and causes each worker to accumulate a full over-sized batch in Arrow
				// simultaneously, exhausting available memory (OOM).
				workerBatches := nBatches / workers
				if workerBatches < 1 {
					b.Skipf("batch=%d × workers=%d > totalMsgsTarget=%d; skipped to maintain constant-work invariant",
						batchSize, workers, totalMsgsTarget)
					return
				}

				clones := make([]*Transcoder, workers)
				for w := range workers {
					c, err := base.Clone(memory.DefaultAllocator)
					if err != nil {
						b.Fatalf("Clone worker %d: %v", w, err)
					}
					clones[w] = c
				}
				defer func() {
					for _, c := range clones {
						c.Release()
					}
				}()

				nMsgs := workers * workerBatches * batchSize
				b.ReportAllocs()
				b.SetBytes(int64(totalCorpusBytes(corpus) * workerBatches))

				runtime.GC()
				var msBefore runtime.MemStats
				runtime.ReadMemStats(&msBefore)

				b.ResetTimer()

				for b.Loop() {
					var wg sync.WaitGroup
					wg.Add(workers)
					for w := range workers {
						go func(tc *Transcoder) {
							defer wg.Done()
							for range workerBatches {
								for _, raw := range corpus {
									if err := tc.AppendRaw(raw); err != nil {
										b.Error(err)
										return
									}
								}
								rec := tc.NewRecordBatch()
								_ = rec.NumRows()
								rec.Release()
							}
						}(clones[w])
					}
					wg.Wait()
				}
				b.StopTimer()

				var msAfter runtime.MemStats
				runtime.ReadMemStats(&msAfter)
				totalMsgsFl := float64(b.N) * float64(nMsgs)
				b.ReportMetric(float64(b.Elapsed().Nanoseconds())/float64(b.N)/float64(nMsgs), "ns/msg")
				b.ReportMetric(float64(b.N*nMsgs)/b.Elapsed().Seconds(), "msg/s")
				b.ReportMetric(float64(msAfter.TotalAlloc-msBefore.TotalAlloc)/totalMsgsFl, "B/msg")
				b.ReportMetric(float64(msAfter.Mallocs-msBefore.Mallocs)/totalMsgsFl, "allocs/msg")
				b.ReportMetric(float64(runtime.GOMAXPROCS(0)), "cpu")
			})
		}
	}
}

// ══════════════════════════════════════════════════════════════════════
// Benchmark: Maximum-throughput concurrent AppendRaw
// ══════════════════════════════════════════════════════════════════════
//
// Drives AppendRaw at the highest achievable message-per-second rate by
// combining:
//
//   - HyperType PGO: one full profiling pass over the corpus is run before
//     the timed loop so HyperType compiles a shape-optimised parser.
//   - Worker fan-out: sub-benchmarks scale from 1 worker up to GOMAXPROCS×2
//     (at powers of two plus maxProcs itself) so the scaling curve and CPU
//     saturation point are immediately visible in benchstat output.
//   - Long-lived clones: each worker owns a Transcoder cloned once at setup,
//     not per b.Loop() iteration, mirroring the real production startup pattern.
//   - Large corpus: 5120 realistic messages per iteration keeps goroutine-launch
//     overhead negligible relative to parsing work.
//
// The reported msg/s is the aggregate across all workers:
//
// msg/s = b.N × corpusSize / elapsed
func BenchmarkMaxThroughput_ConcurrentAppendRaw(b *testing.B) {
	const corpusSize = 122880
	corpus := benchRealisticBidRequestCorpus(b, corpusSize)

	md := (&samples.BidRequestEvent{}).ProtoReflect().Descriptor()
	ht := NewHyperType(md, WithAutoRecompile(0, 1.0)) // manual recompile, 100% sampling

	// PGO warm-up: profile the full corpus then compile the optimised parser.
	// Executed once here; all worker-count sub-benchmarks share the same ht.
	base, err := New(md, memory.DefaultAllocator, WithHyperType(ht))
	if err != nil {
		b.Fatalf("New: %v", err)
	}
	defer base.Release()
	for _, raw := range corpus {
		if err := base.AppendRaw(raw); err != nil {
			b.Fatalf("PGO warm-up: %v", err)
		}
	}
	warmRec := base.NewRecordBatch()
	warmRec.Release()
	if err := ht.Recompile(); err != nil {
		b.Fatalf("Recompile: %v", err)
	}

	maxProcs := runtime.GOMAXPROCS(0) * 2
	for _, workers := range uniqueSortedWorkerCounts(maxProcs) {
		b.Run(fmt.Sprintf("workers-%02d", workers), func(b *testing.B) {
			// Distribute corpus round-robin so each worker gets an equal share.
			shards := make([][][]byte, workers)
			for i, raw := range corpus {
				shards[i%workers] = append(shards[i%workers], raw)
			}

			// Clone once at sub-benchmark setup; not per b.Loop() iteration.
			clones := make([]*Transcoder, workers)
			for w := range workers {
				c, err := base.Clone(memory.DefaultAllocator)
				if err != nil {
					b.Fatalf("Clone worker %d: %v", w, err)
				}
				clones[w] = c
			}
			defer func() {
				for _, c := range clones {
					c.Release()
				}
			}()

			nMsgs := len(corpus)
			b.ReportAllocs()
			b.SetBytes(int64(totalCorpusBytes(corpus)))

			runtime.GC()
			var msBefore runtime.MemStats
			runtime.ReadMemStats(&msBefore)

			b.ResetTimer()

			for b.Loop() {
				var wg sync.WaitGroup
				wg.Add(workers)
				for w := range workers {
					go func(tc *Transcoder, shard [][]byte) {
						defer wg.Done()
						for _, raw := range shard {
							if err := tc.AppendRaw(raw); err != nil {
								b.Error(err)
								return
							}
						}
						rec := tc.NewRecordBatch()
						_ = rec.NumRows()
						rec.Release()
					}(clones[w], shards[w])
				}
				wg.Wait()
			}
			b.StopTimer()

			var msAfter runtime.MemStats
			runtime.ReadMemStats(&msAfter)
			totalMsgs := float64(b.N) * float64(nMsgs)
			b.ReportMetric(float64(b.Elapsed().Nanoseconds())/float64(b.N)/float64(nMsgs), "ns/msg")
			b.ReportMetric(float64(b.N*nMsgs)/b.Elapsed().Seconds(), "msg/s")
			b.ReportMetric(float64(msAfter.TotalAlloc-msBefore.TotalAlloc)/totalMsgs, "B/msg")
			b.ReportMetric(float64(msAfter.Mallocs-msBefore.Mallocs)/totalMsgs, "allocs/msg")
			b.ReportMetric(float64(runtime.GOMAXPROCS(0)), "cpu")
		})
	}
}

// uniqueSortedWorkerCounts returns a deduplicated, ascending slice of worker
// counts covering 1, 2, 4, powers-of-two up to maxProcs×2, maxProcs itself,
// and maxProcs×2. This produces a clean linear + log scaling curve in
// benchstat output so that both linear scaling and saturation are visible.
func uniqueSortedWorkerCounts(maxProcs int) []int {
	set := map[int]bool{1: true, 2: true, 4: true}
	for n := 8; n <= maxProcs*2; n *= 2 {
		set[n] = true
	}
	set[maxProcs] = true
	set[maxProcs*2] = true
	counts := make([]int, 0, len(set))
	for n := range set {
		counts = append(counts, n)
	}
	sort.Ints(counts)
	return counts
}

// ══════════════════════════════════════════════════════════════════════
// Benchmark: Maximum-throughput concurrent AppendRawMerged
// ══════════════════════════════════════════════════════════════════════
//
// Same fan-out pattern as BenchmarkMaxThroughput_ConcurrentAppendRaw but
// exercises the merged-bytes path (base BidRequest + constant CustomFields).
// HyperType is compiled on the merged descriptor so the optimised parser
// covers all fields from both messages.
//
// msg/s = b.N × corpusSize / elapsed  (aggregate across all workers)
func BenchmarkMaxThroughput_ConcurrentAppendRawMerged(b *testing.B) {
	const corpusSize = 122880
	corpus := benchRealisticBidRequestCorpus(b, corpusSize)

	protoDir := benchProtoDir()
	customFD, err := CompileProtoToFileDescriptor("custom_fields.proto", []string{protoDir})
	if err != nil {
		b.Fatalf("compile custom_fields.proto: %v", err)
	}
	customMD, err := GetMessageDescriptorByName(customFD, "CustomFields")
	if err != nil {
		b.Fatalf("get CustomFields: %v", err)
	}
	customMsg := dynamicpb.NewMessage(customMD)
	customMsg.Set(customMD.Fields().ByName("event_timestamp"), protoreflect.ValueOfInt64(1700000000))
	customMsg.Set(customMD.Fields().ByName("source_id"), protoreflect.ValueOfString("bench-source"))
	customBytes, err := proto.Marshal(customMsg)
	if err != nil {
		b.Fatalf("marshal custom: %v", err)
	}

	baseMD := (&samples.BidRequestEvent{}).ProtoReflect().Descriptor()
	mergedMD, err := MergeMessageDescriptors(baseMD, customMD, "BidRequestMaxMerged")
	if err != nil {
		b.Fatalf("merge: %v", err)
	}
	ht := NewHyperType(mergedMD, WithAutoRecompile(0, 1.0))

	base, err := New(baseMD, memory.DefaultAllocator,
		WithCustomMessage(customMD),
		WithHyperType(ht),
	)
	if err != nil {
		b.Fatalf("New: %v", err)
	}
	defer base.Release()

	for _, raw := range corpus {
		if err := base.AppendRawMerged(raw, customBytes); err != nil {
			b.Fatalf("PGO warm-up: %v", err)
		}
	}
	warmRec := base.NewRecordBatch()
	warmRec.Release()
	if err := ht.Recompile(); err != nil {
		b.Fatalf("Recompile: %v", err)
	}

	maxProcs := runtime.GOMAXPROCS(0) * 2
	for _, workers := range uniqueSortedWorkerCounts(maxProcs) {
		b.Run(fmt.Sprintf("workers-%02d", workers), func(b *testing.B) {
			shards := make([][][]byte, workers)
			for i, raw := range corpus {
				shards[i%workers] = append(shards[i%workers], raw)
			}

			clones := make([]*Transcoder, workers)
			for w := range workers {
				c, err := base.Clone(memory.DefaultAllocator)
				if err != nil {
					b.Fatalf("Clone worker %d: %v", w, err)
				}
				clones[w] = c
			}
			defer func() {
				for _, c := range clones {
					c.Release()
				}
			}()

			nMsgs := len(corpus)
			b.ReportAllocs()
			b.SetBytes(int64(totalCorpusBytes(corpus)) + int64(nMsgs*len(customBytes)))

			runtime.GC()
			var msBefore runtime.MemStats
			runtime.ReadMemStats(&msBefore)

			b.ResetTimer()

			for b.Loop() {
				var wg sync.WaitGroup
				wg.Add(workers)
				for w := range workers {
					go func(tc *Transcoder, shard [][]byte) {
						defer wg.Done()
						for _, raw := range shard {
							if err := tc.AppendRawMerged(raw, customBytes); err != nil {
								b.Error(err)
								return
							}
						}
						rec := tc.NewRecordBatch()
						_ = rec.NumRows()
						rec.Release()
					}(clones[w], shards[w])
				}
				wg.Wait()
			}
			b.StopTimer()

			var msAfter runtime.MemStats
			runtime.ReadMemStats(&msAfter)
			totalMsgs := float64(b.N) * float64(nMsgs)
			b.ReportMetric(float64(b.Elapsed().Nanoseconds())/float64(b.N)/float64(nMsgs), "ns/msg")
			b.ReportMetric(float64(b.N*nMsgs)/b.Elapsed().Seconds(), "msg/s")
			b.ReportMetric(float64(msAfter.TotalAlloc-msBefore.TotalAlloc)/totalMsgs, "B/msg")
			b.ReportMetric(float64(msAfter.Mallocs-msBefore.Mallocs)/totalMsgs, "allocs/msg")
			b.ReportMetric(float64(runtime.GOMAXPROCS(0)), "cpu")
		})
	}
}

// ══════════════════════════════════════════════════════════════════════
// Benchmark: Maximum-throughput concurrent AppendDenormRaw
// ══════════════════════════════════════════════════════════════════════
//
// Same fan-out pattern as BenchmarkMaxThroughput_ConcurrentAppendRaw but
// exercises the denormalizer path: HyperType fast parse → Expr plan fan-out
// → independent per-worker Arrow denorm record batches.
//
// msg/s = b.N × corpusSize / elapsed  (aggregate across all workers)
func BenchmarkMaxThroughput_ConcurrentAppendDenormRaw(b *testing.B) {
	const corpusSize = 122880
	corpus := benchRealisticBidRequestCorpus(b, corpusSize)

	md := (&samples.BidRequestEvent{}).ProtoReflect().Descriptor()
	ht := NewHyperType(md, WithAutoRecompile(0, 1.0))

	base, err := New(md, memory.DefaultAllocator,
		WithHyperType(ht),
		WithDenormalizerPlan(
			pbpath.PlanPath("id", pbpath.Alias("bidreq_id")),
			pbpath.PlanPath("device_id",
				pbpath.WithExpr(pbpath.FuncCoalesce(
					pbpath.PathRef("user.id"),
					pbpath.PathRef("site.id"),
					pbpath.PathRef("device.ifa"),
				)),
				pbpath.Alias("device_id"),
			),
			pbpath.PlanPath("site.publisher.id", pbpath.Alias("pub_id")),
			pbpath.PlanPath("technicalprovider.id", pbpath.Alias("tp_id")),
			pbpath.PlanPath("timestamp.seconds", pbpath.Alias("event_time")),
			pbpath.PlanPath("user.ext.demographic.total.units", pbpath.Alias("imp_units")),
			pbpath.PlanPath("user.ext.demographic.total.nanos", pbpath.Alias("imp_nanos")),
			pbpath.PlanPath("width",
				pbpath.WithExpr(pbpath.FuncCoalesce(
					pbpath.PathRef("imp[0].banner.w"),
					pbpath.PathRef("imp[0].video.w"),
				)),
				pbpath.Alias("width"),
			),
			pbpath.PlanPath("height",
				pbpath.WithExpr(pbpath.FuncCoalesce(
					pbpath.PathRef("imp[0].banner.h"),
					pbpath.PathRef("imp[0].video.h"),
				)),
				pbpath.Alias("height"),
			),
			pbpath.PlanPath("imp[0].pmp.deals[*].id", pbpath.Alias("deal")),
		),
	)
	if err != nil {
		b.Fatalf("New: %v", err)
	}
	defer base.Release()

	for _, raw := range corpus {
		if err := base.AppendDenormRaw(raw); err != nil {
			b.Fatalf("PGO warm-up: %v", err)
		}
	}
	warmRec := base.NewDenormalizerRecordBatch()
	warmRec.Release()
	if err := ht.Recompile(); err != nil {
		b.Fatalf("Recompile: %v", err)
	}

	maxProcs := runtime.GOMAXPROCS(0) * 2
	for _, workers := range uniqueSortedWorkerCounts(maxProcs) {
		b.Run(fmt.Sprintf("workers-%02d", workers), func(b *testing.B) {
			shards := make([][][]byte, workers)
			for i, raw := range corpus {
				shards[i%workers] = append(shards[i%workers], raw)
			}

			clones := make([]*Transcoder, workers)
			for w := range workers {
				c, err := base.Clone(memory.DefaultAllocator)
				if err != nil {
					b.Fatalf("Clone worker %d: %v", w, err)
				}
				clones[w] = c
			}
			defer func() {
				for _, c := range clones {
					c.Release()
				}
			}()

			nMsgs := len(corpus)
			b.ReportAllocs()
			b.SetBytes(int64(totalCorpusBytes(corpus)))

			runtime.GC()
			var msBefore runtime.MemStats
			runtime.ReadMemStats(&msBefore)

			b.ResetTimer()

			for b.Loop() {
				var wg sync.WaitGroup
				wg.Add(workers)
				for w := range workers {
					go func(tc *Transcoder, shard [][]byte) {
						defer wg.Done()
						for _, raw := range shard {
							if err := tc.AppendDenormRaw(raw); err != nil {
								b.Error(err)
								return
							}
						}
						rec := tc.NewDenormalizerRecordBatch()
						_ = rec.NumRows()
						rec.Release()
					}(clones[w], shards[w])
				}
				wg.Wait()
			}
			b.StopTimer()

			var msAfter runtime.MemStats
			runtime.ReadMemStats(&msAfter)
			totalMsgs := float64(b.N) * float64(nMsgs)
			b.ReportMetric(float64(b.Elapsed().Nanoseconds())/float64(b.N)/float64(nMsgs), "ns/msg")
			b.ReportMetric(float64(b.N*nMsgs)/b.Elapsed().Seconds(), "msg/s")
			b.ReportMetric(float64(msAfter.TotalAlloc-msBefore.TotalAlloc)/totalMsgs, "B/msg")
			b.ReportMetric(float64(msAfter.Mallocs-msBefore.Mallocs)/totalMsgs, "allocs/msg")
			b.ReportMetric(float64(runtime.GOMAXPROCS(0)), "cpu")
		})
	}
}

// ══════════════════════════════════════════════════════════════════════
// Helpers: TestMsg corpus (matches Python python/tests/conftest.py)
// ══════════════════════════════════════════════════════════════════════

// benchSimpleTestMsgProtoDir returns the directory containing test_msg.proto.
// This is the same file used by the Python test suite so that
// BenchmarkMaxThroughput_ConcurrentAppendRaw_TestMsg and the Python
// TestBenchmarkMaxThroughputConcurrent::test_concurrent_append_raw benchmarks
// transcode the identical schema with an identical corpus shape.
func benchSimpleTestMsgProtoDir() string {
	_, f, _, _ := runtime.Caller(0)
	return filepath.Join(filepath.Dir(f), "python", "tests", "fixtures")
}

// benchAppendVarint appends a protobuf varint encoding of v to buf.
func benchAppendVarint(buf []byte, v uint64) []byte {
	for v >= 0x80 {
		buf = append(buf, byte(v&0x7F)|0x80)
		v >>= 7
	}
	return append(buf, byte(v))
}

// benchEncodeTestMsg hand-encodes a TestMsg in protobuf wire format.
// The encoding is identical to python/tests/conftest.py:encode_test_msg so
// both language benchmarks feed the same byte sequences to the library.
//
//	field 1 (string name):   tag=0x0A, len-delimited
//	field 2 (int32  age):    tag=0x10, varint
//	field 3 (double score):  tag=0x19, fixed64 little-endian
//	field 4 (bool   active): tag=0x20, varint
func benchEncodeTestMsg(name string, age int32, score float64, active bool) []byte {
	buf := make([]byte, 0, 32+len(name))
	if name != "" {
		buf = append(buf, 0x0A)
		buf = benchAppendVarint(buf, uint64(len(name)))
		buf = append(buf, name...)
	}
	if age != 0 {
		buf = append(buf, 0x10)
		buf = benchAppendVarint(buf, uint64(age))
	}
	if score != 0 {
		bits := math.Float64bits(score)
		var b8 [8]byte
		binary.LittleEndian.PutUint64(b8[:], bits)
		buf = append(buf, 0x19)
		buf = append(buf, b8[:]...)
	}
	if active {
		buf = append(buf, 0x20, 0x01)
	}
	return buf
}

// benchSimpleMsgCorpus returns n TestMsg messages encoded as protobuf bytes.
// The generation pattern mirrors Python's TestBenchmarkMaxThroughputConcurrent
// fixture: name="user-{i%1000}", age=i%100, score=float(i)*1.1, active=i%2==0.
func benchSimpleMsgCorpus(b *testing.B, n int) [][]byte {
	b.Helper()
	raw := make([][]byte, n)
	for i := range n {
		raw[i] = benchEncodeTestMsg(
			fmt.Sprintf("user-%d", i%1000),
			int32(i%100),
			float64(i)*1.1,
			i%2 == 0,
		)
	}
	return raw
}

// ══════════════════════════════════════════════════════════════════════
// Benchmark: Maximum-throughput concurrent AppendRaw — TestMsg schema
// ══════════════════════════════════════════════════════════════════════
//
// Direct Go counterpart of the Python benchmark:
//
//	python/tests/test_benchmark.py::TestBenchmarkMaxThroughputConcurrent::test_concurrent_append_raw
//
// Both benchmarks use:
//   - Schema:      TestMsg (4 scalar fields — name string, age int32, score double, active bool)
//   - Proto file:  python/tests/fixtures/test_msg.proto (same file, same descriptor)
//   - Corpus:      5120 messages per iteration (i%1000 names, i%100 ages, i*1.1 scores)
//   - Worker fan-out: sub-benchmarks over uniqueSortedWorkerCounts(GOMAXPROCS×2)
//   - HyperType PGO: one full profiling pass before the timed loop
//
// The msg/s values are directly comparable to the Python benchmark output.
// For the production-realistic BidRequest (100+ fields) numbers, see
// BenchmarkMaxThroughput_ConcurrentAppendRaw.
func BenchmarkMaxThroughput_ConcurrentAppendRaw_TestMsg(b *testing.B) {
	const corpusSize = 5120
	corpus := benchSimpleMsgCorpus(b, corpusSize)
	protoDir := benchSimpleTestMsgProtoDir()

	fd, err := CompileProtoToFileDescriptor("test_msg.proto", []string{protoDir})
	if err != nil {
		b.Fatalf("CompileProtoToFileDescriptor: %v", err)
	}
	md, err := GetMessageDescriptorByName(fd, "TestMsg")
	if err != nil {
		b.Fatalf("GetMessageDescriptorByName TestMsg: %v", err)
	}
	ht := NewHyperType(md, WithAutoRecompile(0, 1.0))

	base, err := NewFromFile("test_msg.proto", "TestMsg", []string{protoDir}, memory.DefaultAllocator,
		WithHyperType(ht),
	)
	if err != nil {
		b.Fatalf("NewFromFile TestMsg: %v", err)
	}
	defer base.Release()

	// PGO warm-up: profile the full corpus then compile the optimised parser.
	for _, raw := range corpus {
		if err := base.AppendRaw(raw); err != nil {
			b.Fatalf("PGO warm-up: %v", err)
		}
	}
	warmRec := base.NewRecordBatch()
	warmRec.Release()
	if err := ht.Recompile(); err != nil {
		b.Fatalf("Recompile: %v", err)
	}

	maxProcs := runtime.GOMAXPROCS(0) * 2
	for _, workers := range uniqueSortedWorkerCounts(maxProcs) {
		b.Run(fmt.Sprintf("workers-%02d", workers), func(b *testing.B) {
			shards := make([][][]byte, workers)
			for i, raw := range corpus {
				shards[i%workers] = append(shards[i%workers], raw)
			}

			clones := make([]*Transcoder, workers)
			for w := range workers {
				c, err := base.Clone(memory.DefaultAllocator)
				if err != nil {
					b.Fatalf("Clone worker %d: %v", w, err)
				}
				clones[w] = c
			}
			defer func() {
				for _, c := range clones {
					c.Release()
				}
			}()

			nMsgs := len(corpus)
			b.ReportAllocs()
			b.SetBytes(int64(totalCorpusBytes(corpus)))

			runtime.GC()
			var msBefore runtime.MemStats
			runtime.ReadMemStats(&msBefore)

			b.ResetTimer()

			for b.Loop() {
				var wg sync.WaitGroup
				wg.Add(workers)
				for w := range workers {
					go func(tc *Transcoder, shard [][]byte) {
						defer wg.Done()
						for _, raw := range shard {
							if err := tc.AppendRaw(raw); err != nil {
								b.Error(err)
								return
							}
						}
						rec := tc.NewRecordBatch()
						_ = rec.NumRows()
						rec.Release()
					}(clones[w], shards[w])
				}
				wg.Wait()
			}
			b.StopTimer()

			var msAfter runtime.MemStats
			runtime.ReadMemStats(&msAfter)
			totalMsgs := float64(b.N) * float64(nMsgs)
			b.ReportMetric(float64(b.Elapsed().Nanoseconds())/float64(b.N)/float64(nMsgs), "ns/msg")
			b.ReportMetric(float64(b.N*nMsgs)/b.Elapsed().Seconds(), "msg/s")
			b.ReportMetric(float64(msAfter.TotalAlloc-msBefore.TotalAlloc)/totalMsgs, "B/msg")
			b.ReportMetric(float64(msAfter.Mallocs-msBefore.Mallocs)/totalMsgs, "allocs/msg")
			b.ReportMetric(float64(runtime.GOMAXPROCS(0)), "cpu")
		})
	}
}
