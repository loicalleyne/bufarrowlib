package bufarrowlib

// ══════════════════════════════════════════════════════════════════════
// Benchmarks: bufarrowlib vs arrow/util.ProtobufMessageReflection (PMR)
// ══════════════════════════════════════════════════════════════════════
//
// Side-by-side comparison of three approaches:
//   - PMR:                 upstream ProtobufMessageReflection (per-message record)
//   - bufarrowlib/Append:  decoded proto.Message → batched RecordBatch
//   - bufarrowlib/AppendRaw: raw wire bytes → batched RecordBatch (HyperType)
//
// PMR has no batch mode — each message produces its own RecordBatch,
// so the PMR sub-benchmarks call NewProtobufMessageReflection + Record
// per message. bufarrowlib accumulates and flushes once per batch.

import (
	"fmt"
	"runtime"
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/memory"
	arrowutil "github.com/apache/arrow-go/v18/arrow/util"
	"github.com/loicalleyne/bufarrowlib/gen/go/samples"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// pmrRecordOrSkip calls PMR.Record and returns the result. If PMR panics
// (known issue with certain message shapes), it skips the benchmark.
func pmrRecordOrSkip(b *testing.B, msg proto.Message, mem memory.Allocator) arrow.RecordBatch {
	b.Helper()
	var rec arrow.RecordBatch
	panicked := true
	func() {
		defer func() {
			if r := recover(); r != nil {
				b.Skipf("PMR.Record() panicked: %v — skipping this sub-benchmark", r)
			}
		}()
		pmr := arrowutil.NewProtobufMessageReflection(msg)
		rec = pmr.Record(mem)
		panicked = false
	}()
	if panicked {
		return nil
	}
	return rec
}

// ══════════════════════════════════════════════════════════════════════
// Benchmark: Single-message overhead — ScalarTypes
// ══════════════════════════════════════════════════════════════════════
//
// Batch size = 1, no amortization — pure per-message overhead comparison.
// All three methods process the same 100-message corpus; each message is
// flushed immediately so the cost structure is strictly per-record.

func BenchmarkVsPMR_SingleMessage_ScalarTypes(b *testing.B) {
	const n = 100
	msgs := generateScalarMessages(b, n)
	mem := memory.DefaultAllocator

	// Pre-marshal for the AppendRaw sub-benchmark.
	rawCorpus := make([][]byte, n)
	for i, m := range msgs {
		raw, err := proto.Marshal(m)
		if err != nil {
			b.Fatalf("marshal: %v", err)
		}
		rawCorpus[i] = raw
	}

	md := (&samples.ScalarTypes{}).ProtoReflect().Descriptor()

	b.Run("PMR", func(b *testing.B) {
		// Canary: skip if PMR.Record() panics on this message type.
		canary := pmrRecordOrSkip(b, msgs[0], mem)
		if canary != nil {
			canary.Release()
		}

		nMsgs := len(msgs)
		b.ReportAllocs()

		runtime.GC()
		var msBefore runtime.MemStats
		runtime.ReadMemStats(&msBefore)
		b.ResetTimer()

		for b.Loop() {
			for _, m := range msgs {
				pmr := arrowutil.NewProtobufMessageReflection(m)
				rec := pmr.Record(mem)
				_ = rec.NumRows()
				rec.Release()
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

	b.Run("bufarrowlib/Append", func(b *testing.B) {
		tc, err := New(md, mem)
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
				tc.Append(m)
				rec := tc.NewRecordBatch()
				_ = rec.NumRows()
				rec.Release()
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

	b.Run("bufarrowlib/AppendRaw", func(b *testing.B) {
		ht := NewHyperType(md)
		tc, err := New(md, mem, WithHyperType(ht))
		if err != nil {
			b.Fatalf("New: %v", err)
		}
		defer tc.Release()

		nMsgs := len(rawCorpus)
		b.ReportAllocs()

		runtime.GC()
		var msBefore runtime.MemStats
		runtime.ReadMemStats(&msBefore)
		b.ResetTimer()

		for b.Loop() {
			for _, raw := range rawCorpus {
				if err := tc.AppendRaw(raw); err != nil {
					b.Fatalf("AppendRaw: %v", err)
				}
				rec := tc.NewRecordBatch()
				_ = rec.NumRows()
				rec.Release()
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
// Benchmark: Batch-size scaling — ScalarTypes
// ══════════════════════════════════════════════════════════════════════
//
// Three methods × three batch sizes. PMR has no batch accumulation —
// each message is its own RecordBatch. bufarrowlib accumulates across
// the batch and flushes once, showing amortization advantage.

func BenchmarkVsPMR_Batch_ScalarTypes(b *testing.B) {
	const corpusSize = 1000
	msgs := generateScalarMessages(b, corpusSize)
	mem := memory.DefaultAllocator

	rawCorpus := make([][]byte, corpusSize)
	for i, m := range msgs {
		raw, err := proto.Marshal(m)
		if err != nil {
			b.Fatalf("marshal: %v", err)
		}
		rawCorpus[i] = raw
	}

	md := (&samples.ScalarTypes{}).ProtoReflect().Descriptor()

	for _, batchSize := range []int{1, 100, 1000} {
		b.Run(fmt.Sprintf("PMR/batch-%d", batchSize), func(b *testing.B) {
			// Canary: skip if PMR.Record() panics on this message type.
			canary := pmrRecordOrSkip(b, msgs[0], mem)
			if canary != nil {
				canary.Release()
			}

			corpus := msgs[:batchSize]
			nMsgs := len(corpus)
			b.ReportAllocs()

			runtime.GC()
			var msBefore runtime.MemStats
			runtime.ReadMemStats(&msBefore)
			b.ResetTimer()

			for b.Loop() {
				for _, m := range corpus {
					pmr := arrowutil.NewProtobufMessageReflection(m)
					rec := pmr.Record(mem)
					_ = rec.NumRows()
					rec.Release()
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

		b.Run(fmt.Sprintf("bufarrowlib/Append/batch-%d", batchSize), func(b *testing.B) {
			tc, err := New(md, mem)
			if err != nil {
				b.Fatalf("New: %v", err)
			}
			defer tc.Release()

			corpus := msgs[:batchSize]
			nMsgs := len(corpus)
			b.ReportAllocs()

			runtime.GC()
			var msBefore runtime.MemStats
			runtime.ReadMemStats(&msBefore)
			b.ResetTimer()

			for b.Loop() {
				for _, m := range corpus {
					tc.Append(m)
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

		b.Run(fmt.Sprintf("bufarrowlib/AppendRaw/batch-%d", batchSize), func(b *testing.B) {
			ht := NewHyperType(md)
			tc, err := New(md, mem, WithHyperType(ht))
			if err != nil {
				b.Fatalf("New: %v", err)
			}
			defer tc.Release()

			corpus := rawCorpus[:batchSize]
			nMsgs := len(corpus)
			b.ReportAllocs()

			runtime.GC()
			var msBefore runtime.MemStats
			runtime.ReadMemStats(&msBefore)
			b.ResetTimer()

			for b.Loop() {
				for _, raw := range corpus {
					if err := tc.AppendRaw(raw); err != nil {
						b.Fatalf("AppendRaw: %v", err)
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
// Benchmark: Batch-size scaling — BidRequest (complex nested message)
// ══════════════════════════════════════════════════════════════════════
//
// Three-way comparison on a production-realistic 506-message BidRequest
// corpus. Batch sizes: 1 (per-message overhead) and 506 (full batch).
// PMR requires proto.Unmarshal first; AppendRaw skips deserialization.

func BenchmarkVsPMR_Batch_BidRequest(b *testing.B) {
	corpus := benchRealisticBidRequestCorpus(b, 506)
	mem := memory.DefaultAllocator

	md := (&samples.BidRequestEvent{}).ProtoReflect().Descriptor()

	for _, batchSize := range []int{1, 506} {
		b.Run(fmt.Sprintf("PMR/batch-%d", batchSize), func(b *testing.B) {
			// Canary: unmarshal first message, check if PMR.Record() panics.
			canaryMsg := &samples.BidRequestEvent{}
			if err := proto.Unmarshal(corpus[0], canaryMsg); err != nil {
				b.Fatalf("Unmarshal canary: %v", err)
			}
			canary := pmrRecordOrSkip(b, canaryMsg, mem)
			if canary != nil {
				canary.Release()
			}

			slice := corpus[:batchSize]
			nMsgs := len(slice)
			b.ReportAllocs()
			b.SetBytes(int64(totalCorpusBytes(slice)))

			runtime.GC()
			var msBefore runtime.MemStats
			runtime.ReadMemStats(&msBefore)
			b.ResetTimer()

			for b.Loop() {
				for _, raw := range slice {
					msg := &samples.BidRequestEvent{}
					if err := proto.Unmarshal(raw, msg); err != nil {
						b.Fatalf("Unmarshal: %v", err)
					}
					pmr := arrowutil.NewProtobufMessageReflection(msg)
					rec := pmr.Record(mem)
					_ = rec.NumRows()
					rec.Release()
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

		b.Run(fmt.Sprintf("bufarrowlib/Append/batch-%d", batchSize), func(b *testing.B) {
			tc, err := New(md, mem)
			if err != nil {
				b.Fatalf("New: %v", err)
			}
			defer tc.Release()

			slice := corpus[:batchSize]
			nMsgs := len(slice)
			b.ReportAllocs()
			b.SetBytes(int64(totalCorpusBytes(slice)))

			runtime.GC()
			var msBefore runtime.MemStats
			runtime.ReadMemStats(&msBefore)
			b.ResetTimer()

			for b.Loop() {
				for _, raw := range slice {
					msg := &samples.BidRequestEvent{}
					if err := proto.Unmarshal(raw, msg); err != nil {
						b.Fatalf("Unmarshal: %v", err)
					}
					tc.Append(msg)
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

		b.Run(fmt.Sprintf("bufarrowlib/AppendRaw/batch-%d", batchSize), func(b *testing.B) {
			ht := NewHyperType(md)
			tc, err := New(md, mem, WithHyperType(ht))
			if err != nil {
				b.Fatalf("New: %v", err)
			}
			defer tc.Release()

			slice := corpus[:batchSize]
			nMsgs := len(slice)
			b.ReportAllocs()
			b.SetBytes(int64(totalCorpusBytes(slice)))

			runtime.GC()
			var msBefore runtime.MemStats
			runtime.ReadMemStats(&msBefore)
			b.ResetTimer()

			for b.Loop() {
				for _, raw := range slice {
					if err := tc.AppendRaw(raw); err != nil {
						b.Fatalf("AppendRaw: %v", err)
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
// Benchmark: Schema construction cost
// ══════════════════════════════════════════════════════════════════════
//
// PMR derives a new schema on every NewProtobufMessageReflection call.
// bufarrowlib derives the schema once in New() and reuses it across
// all Append calls. This benchmark measures the one-time cost.

func BenchmarkVsPMR_SchemaConstruction(b *testing.B) {
	mem := memory.DefaultAllocator

	b.Run("ScalarTypes/PMR", func(b *testing.B) {
		msg := &samples.ScalarTypes{Double: 1.0}
		b.ReportAllocs()
		b.ResetTimer()
		for b.Loop() {
			pmr := arrowutil.NewProtobufMessageReflection(msg)
			_ = pmr.Schema()
		}
	})

	b.Run("ScalarTypes/bufarrowlib", func(b *testing.B) {
		md := (&samples.ScalarTypes{}).ProtoReflect().Descriptor()
		b.ReportAllocs()
		b.ResetTimer()
		for b.Loop() {
			tc, err := New(md, mem)
			if err != nil {
				b.Fatalf("New: %v", err)
			}
			_ = tc.Schema()
			tc.Release()
		}
	})

	b.Run("BidRequestEvent/PMR", func(b *testing.B) {
		msg := &samples.BidRequestEvent{Id: "bench"}
		b.ReportAllocs()
		b.ResetTimer()
		for b.Loop() {
			pmr := arrowutil.NewProtobufMessageReflection(msg)
			_ = pmr.Schema()
		}
	})

	b.Run("BidRequestEvent/bufarrowlib", func(b *testing.B) {
		md := (&samples.BidRequestEvent{}).ProtoReflect().Descriptor()
		b.ReportAllocs()
		b.ResetTimer()
		for b.Loop() {
			tc, err := New(md, mem)
			if err != nil {
				b.Fatalf("New: %v", err)
			}
			_ = tc.Schema()
			tc.Release()
		}
	})
}

// ══════════════════════════════════════════════════════════════════════
// Benchmark: Known type (Timestamp + Duration) — PMR works here
// ══════════════════════════════════════════════════════════════════════
//
// The Known message type contains Timestamp and Duration well-known
// types with NO proto float fields. This is the only message type in the
// test suite where PMR.Record() actually works (see FLOAT32 bug above).
// This benchmark provides the only actual side-by-side PMR vs bufarrowlib
// record-building numbers.

func generateKnownMessages(b *testing.B, n int) []*samples.Known {
	b.Helper()
	msgs := make([]*samples.Known, n)
	for i := range n {
		msgs[i] = &samples.Known{
			Ts:       &timestamppb.Timestamp{Seconds: int64(1718000000 + i), Nanos: int32(i % 1000000000)},
			Duration: &durationpb.Duration{Seconds: int64(i % 86400), Nanos: int32(i % 1000000000)},
		}
	}
	return msgs
}

func BenchmarkVsPMR_Known(b *testing.B) {
	const corpusSize = 1000
	msgs := generateKnownMessages(b, corpusSize)
	mem := memory.DefaultAllocator

	rawCorpus := make([][]byte, corpusSize)
	for i, m := range msgs {
		raw, err := proto.Marshal(m)
		if err != nil {
			b.Fatalf("marshal: %v", err)
		}
		rawCorpus[i] = raw
	}

	md := (&samples.Known{}).ProtoReflect().Descriptor()

	for _, batchSize := range []int{1, 100, 1000} {
		b.Run(fmt.Sprintf("PMR/batch-%d", batchSize), func(b *testing.B) {
			corpus := msgs[:batchSize]
			nMsgs := len(corpus)
			b.ReportAllocs()

			runtime.GC()
			var msBefore runtime.MemStats
			runtime.ReadMemStats(&msBefore)
			b.ResetTimer()

			for b.Loop() {
				for _, m := range corpus {
					pmr := arrowutil.NewProtobufMessageReflection(m)
					rec := pmr.Record(mem)
					_ = rec.NumRows()
					rec.Release()
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

		b.Run(fmt.Sprintf("bufarrowlib/Append/batch-%d", batchSize), func(b *testing.B) {
			tc, err := New(md, mem)
			if err != nil {
				b.Fatalf("New: %v", err)
			}
			defer tc.Release()

			corpus := msgs[:batchSize]
			nMsgs := len(corpus)
			b.ReportAllocs()

			runtime.GC()
			var msBefore runtime.MemStats
			runtime.ReadMemStats(&msBefore)
			b.ResetTimer()

			for b.Loop() {
				for _, m := range corpus {
					tc.Append(m)
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

		b.Run(fmt.Sprintf("bufarrowlib/AppendRaw/batch-%d", batchSize), func(b *testing.B) {
			ht := NewHyperType(md)
			tc, err := New(md, mem, WithHyperType(ht))
			if err != nil {
				b.Fatalf("New: %v", err)
			}
			defer tc.Release()

			corpus := rawCorpus[:batchSize]
			nMsgs := len(corpus)
			b.ReportAllocs()

			runtime.GC()
			var msBefore runtime.MemStats
			runtime.ReadMemStats(&msBefore)
			b.ResetTimer()

			for b.Loop() {
				for _, raw := range corpus {
					if err := tc.AppendRaw(raw); err != nil {
						b.Fatalf("AppendRaw: %v", err)
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
