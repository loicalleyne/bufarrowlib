package bufarrowlib

import (
	"context"
	"fmt"
	"runtime"
	"sync"
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/loicalleyne/bufarrowlib/gen/go/samples"
	"github.com/loicalleyne/bufarrowlib/proto/pbpath"
	"github.com/loicalleyne/couac"
)

// ══════════════════════════════════════════════════════════════════════
// E2E Pipeline benchmarks: proto bytes → chan []byte → N workers →
// chan arrow.Record → DuckDB ADBC sink (couac)
// ══════════════════════════════════════════════════════════════════════
//
// These benchmarks model the full quacfka-service channel architecture:
//
//   1. A buffered mChan (chan []byte, cap=corpusSize) is pre-filled with
//      serialised protobuf messages and closed — simulating Kafka consumers
//      feeding the pipeline.
//   2. N worker goroutines compete on mChan (natural load-balancing, not
//      pre-sharded), calling AppendRaw or AppendDenormRaw per message.
//      Each worker flushes one Arrow RecordBatch when the channel drains.
//   3. Flushed RecordBatches are sent through rChan (chan arrow.Record,
//      cap=5) to a single DuckDB sink goroutine.
//   4. The sink goroutine calls couac.Conn.Ingest (ADBC Create-or-Append)
//      for each RecordBatch, then releases it.
//
// This measures the end-to-end cost including goroutine scheduling, channel
// synchronisation, Arrow builder flush, and DuckDB ADBC write overhead.
//
// If DuckDB is not installed (couac.WithDriverLookup fails), the benchmarks
// are skipped gracefully via b.Skipf.
//
// Compare with BenchmarkMaxThroughput_ConcurrentAppendRaw (pre-sharded,
// no channels, no DuckDB) to isolate the channel + DuckDB overhead.

// benchOpenDuckDB opens an in-memory DuckDB database and a single connection
// via couac. Returns db, conn, or calls b.Skipf on failure (no DuckDB driver).
func benchOpenDuckDB(b *testing.B) (*couac.DB, *couac.Conn) {
	b.Helper()
	db, err := couac.NewDuck(couac.WithDriverLookup())
	if err != nil {
		b.Skipf("DuckDB driver unavailable (install with `dbc install duckdb`): %v", err)
	}
	conn, err := db.Connect()
	if err != nil {
		db.Close()
		b.Skipf("DuckDB connection failed: %v", err)
	}
	return db, conn
}

// BenchmarkE2EPipeline_ConcurrentAppendRaw measures end-to-end throughput
// for the non-denormalised AppendRaw path through the full channel pipeline
// into DuckDB.
func BenchmarkE2EPipeline_ConcurrentAppendRaw(b *testing.B) {
	const corpusSize = 122880
	corpus := benchRealisticBidRequestCorpus(b, corpusSize)

	md := (&samples.BidRequestEvent{}).ProtoReflect().Descriptor()
	ht := NewHyperType(md, WithAutoRecompile(0, 1.0))

	base, err := New(md, memory.DefaultAllocator, WithHyperType(ht))
	if err != nil {
		b.Fatalf("New: %v", err)
	}
	defer base.Release()

	// PGO warm-up.
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

	db, conn := benchOpenDuckDB(b)
	defer db.Close()
	defer conn.Close()
	ctx := context.Background()

	maxProcs := runtime.GOMAXPROCS(0) * 2
	for _, workers := range uniqueSortedWorkerCounts(maxProcs) {
		b.Run(fmt.Sprintf("workers-%02d", workers), func(b *testing.B) {
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
				mChan := make(chan []byte, len(corpus))
				for _, raw := range corpus {
					mChan <- raw
				}
				close(mChan)

				rChan := make(chan arrow.Record, 5)
				sinkDone := make(chan struct{})

				// DuckDB sink goroutine.
				go func() {
					defer close(sinkDone)
					for rec := range rChan {
						if _, err := conn.Ingest(ctx, "bidreq", rec); err != nil {
							b.Errorf("Ingest: %v", err)
						}
						rec.Release()
					}
				}()

				// Worker goroutines drain mChan.
				var wg sync.WaitGroup
				wg.Add(workers)
				for w := range workers {
					go func(tc *Transcoder) {
						defer wg.Done()
						for raw := range mChan {
							if err := tc.AppendRaw(raw); err != nil {
								b.Error(err)
								return
							}
						}
						rec := tc.NewRecordBatch()
						rChan <- rec
					}(clones[w])
				}

				wg.Wait()
				close(rChan)
				<-sinkDone
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

// BenchmarkE2EPipeline_ConcurrentAppendDenormRaw measures end-to-end
// throughput for the denormalised AppendDenormRaw path through the full
// channel pipeline into DuckDB. Uses the same 10-field denorm plan as
// BenchmarkMaxThroughput_ConcurrentAppendDenormRaw for direct comparison.
func BenchmarkE2EPipeline_ConcurrentAppendDenormRaw(b *testing.B) {
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

	// PGO warm-up.
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

	db, conn := benchOpenDuckDB(b)
	defer db.Close()
	defer conn.Close()
	ctx := context.Background()

	maxProcs := runtime.GOMAXPROCS(0) * 2
	for _, workers := range uniqueSortedWorkerCounts(maxProcs) {
		b.Run(fmt.Sprintf("workers-%02d", workers), func(b *testing.B) {
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
				mChan := make(chan []byte, len(corpus))
				for _, raw := range corpus {
					mChan <- raw
				}
				close(mChan)

				rChan := make(chan arrow.Record, 5)
				sinkDone := make(chan struct{})

				// DuckDB sink goroutine.
				go func() {
					defer close(sinkDone)
					for rec := range rChan {
						if _, err := conn.Ingest(ctx, "bidreq_denorm", rec); err != nil {
							b.Errorf("Ingest: %v", err)
						}
						rec.Release()
					}
				}()

				// Worker goroutines drain mChan.
				var wg sync.WaitGroup
				wg.Add(workers)
				for w := range workers {
					go func(tc *Transcoder) {
						defer wg.Done()
						for raw := range mChan {
							if err := tc.AppendDenormRaw(raw); err != nil {
								b.Error(err)
								return
							}
						}
						rec := tc.NewDenormalizerRecordBatch()
						rChan <- rec
					}(clones[w])
				}

				wg.Wait()
				close(rChan)
				<-sinkDone
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
