# bufarrowlib Performance Guide

> **Who this is for:** Engineers evaluating bufarrowlib for a production pipeline, deciding which API to use, or tuning worker counts on existing code.

**Test machines:** Intel i7-13700H / GOMAXPROCS=20 / WSL2 · AMD EPYC 7B13 / GOMAXPROCS=8 / Linux  
**Go:** 1.26.1 · **Python:** 3.13.1 (i7) · 3.13.5 (EPYC) · **pytest-benchmark:** 5.2.3  
Raw data: `docs/<cpu_slug>-benchmark-results.txt` / `docs/<cpu_slug>-benchmark-results-python.json`  
Reproduce: `make bench` · compare across runs: `make bench-compare`

---

## Schemas used in this guide

Two schemas appear throughout. Choosing the right row to read in each table requires knowing which one matches your workload.

**TestMsg** — 4 scalar fields, no nesting, no imports.
```protobuf
message TestMsg { string name = 1; int32 age = 2; double score = 3; bool active = 4; }
```
This is the library's unit-test schema. It represents the absolute best case: minimal wire size, no nested messages, no repeated fields. Use the TestMsg rows to understand the FFI overhead floor and the ceiling for simple events (e.g. metrics, counters, flat log lines).

**BidRequestEvent** — realistic ad-tech schema derived from production traffic analysis.
- 14 top-level fields (string, uint32, Timestamp, repeated, nested messages)
- Up to 6 levels of nesting: `imp[*].pmp.deals[*].ext.adspottype`
- `imp[]` (repeated): 75% of messages have 2 imps, 18% have 1, 7% have 3+
- `pmp.deals[]` (repeated): 61% have 1 deal, 21% have 2, 6% have 3, 10% have 4+
- `DecimalValue` sub-messages on `bidfloor`, `lat`, `lon`, demographic totals
- All top-level objects always present (user, device, site, technicalprovider, dooh, geo)
- Distribution is deterministic and seeded; representative of ~80 distinct TP IDs on production traffic

Encoded wire sizes average **350–600 bytes/message** depending on imp/deal count. This is the realistic production baseline: if your workload is more complex, your throughput will be lower; if less complex, higher. Go and Python benchmarks generate this schema from the same distribution so the numbers are directly comparable.

---

## TL;DR — numbers that matter

| Use case | Schema | Throughput | Cost per message |
|---|---|---|---|
| Go — single-threaded | BidRequest | 121 k msg/s | 8.2 µs |
| Go — concurrent, up to 4×GOMAXPROCS workers | BidRequest | **463 k msg/s** (i7) · **192 k** (EPYC) | 2.2 µs · 5.2 µs |
| Python — single Transcoder, large batch (122 k msgs) | BidRequest | _(run `make bench`)_ | — |
| Python — Pool, optimal workers | BidRequest | _(run `make bench`)_ | — |
| Go — single-threaded | TestMsg | **1.9 M msg/s** | 0.52 µs |
| Go — concurrent, GOMAXPROCS workers | TestMsg | _(run `make bench`)_ | — |
| Python — single Transcoder, large batch (122 k msgs) | TestMsg | 726 k msg/s | 1.4 µs |
| Python — Pool, optimal workers (i7) | TestMsg | **914 k msg/s** | 1.1 µs |
| Python — ThreadPoolExecutor, 4 workers | TestMsg | 46–56 k msg/s | 18–22 µs |

> All BidRequest rows — Go and Python — use the same schema and corpus distribution. TestMsg rows are included for FFI-overhead analysis only; they do not represent production workloads. Run `make bench` to populate missing cells with numbers from your hardware.

**Bottom line:** A single Python `Transcoder` with large batches already saturates most ingestion pipelines. If you need more, use `Pool`. Do not use `ThreadPoolExecutor` — the GIL makes multi-threaded FFI actively harmful.

---

## Which API should I use?

### Go or Python?

Both expose the same pipeline: protobuf bytes → Apache Arrow `RecordBatch`. Choose the language that fits your application; throughput is comparable once batch sizes are large enough.

- **Go** `Transcoder`/`Pool` — zero FFI overhead, lowest latency, best sustained throughput
- **Python** `pybufarrow` — thin `ctypes` wrapper, all parsing runs in Go; the FFI boundary cost disappears at batches ≥ 100 messages

### Which append method?

| Method | When to use | Single-threaded throughput |
|---|---|---|
| `AppendRaw(bytes)` | Pre-serialised protobuf (Kafka, file, network) | ~110 k msg/s |
| `AppendRaw` + HyperType | Same, with a compiled `HyperType` parser (see below) | ~151 k msg/s (+37%) |
| `AppendRawMerged(base, custom)` | Two-schema merge at ingest (base + per-event extras) | ~106 k msg/s |
| `AppendDenormRaw(bytes)` | One message fans out to multiple rows (nested repeated) | 55–535 k msg/s (fan-out dependent) |
| `AppendDenormRawMerged(base, custom)` | Denorm + merged fields in one pass | ~204 k msg/s |

> **HyperType is important for production:** Without it the library falls back to `dynamicpb` reflection-based parsing — consistently **2.9–3.0× slower** (AppendRaw) or **4.3–4.7×** (AppendDenorm). See the [HyperType](#hypertype) section for what it is and how to create one.

### `Pool` vs `ThreadPoolExecutor`? (Python only)

**Use `Pool`. Never use `ThreadPoolExecutor` to call Transcoder concurrently** — it is fast only at `workers=1`.

```
                            workers=1   w=2     w=4     w=20    w=40
ThreadPoolExecutor (i7):    856k/s    224k    56k     45k     45k   ← GIL collapse
Pool (i7):                  844k/s    914k    903k    739k    645k  ← scales cleanly
```

At `workers=4`, `Pool` is **16× faster** than `ThreadPoolExecutor`. The root cause is that every FFI call acquires and releases the GIL, serialising all Python threads. `Pool` keeps Python single-threaded — it submits messages synchronously and lets Go goroutines do all the parallel work.

---

## Batch size: the single biggest lever

Before tuning workers, ensure your batch size is large enough. The table below covers the full range from naïve (1 msg) to DuckDB-aligned (122,880 = DuckDB's default row group size).

_Run: `make bench-go BENCH_FILTER=BenchmarkAppendRaw_BatchSize` (single worker) or `BENCH_FILTER=BenchmarkAppendRaw_BatchSizeWorkers` (all workers × all batch sizes) — BidRequest corpus, HyperType, i7-13700H._

**Single-worker sweep** (`BenchmarkAppendRaw_BatchSize`):

| Batch size | ns/msg | msg/s | allocs/msg | Notes |
|---|---|---|---|---|
| 1 | 147,628 | 6,774 | 2,446 | Per-flush fixed cost dominates |
| 100 | 9,749 | 102,577 | 47 | **Minimum viable** — 15.1× cheaper than batch=1 |
| 1,000 | 8,243 | 121,315 | 26 | 18% more than 100; sweet spot for most pipelines |
| 10,000 | 8,181 | 122,228 | 24 | Marginal gain over 1,000 |
| 122,880 | 7,711 | 129,689 | 24 | **DuckDB row group boundary** |
| 245,760 | 8,171 | 122,383 | 24 | 2× DuckDB row group |

_EPYC 7B13 (GOMAXPROCS=8), same benchmark:_

| Batch size | ns/msg | msg/s | allocs/msg |
|---|---|---|---|
| 1 | 240,721 | 4,154 | 2,446 |
| 100 | 15,733 | 63,559 | 47 |
| 1,000 | 13,798 | 72,477 | 26 |
| 10,000 | 13,155 | 76,019 | 24 |
| 122,880 | 12,925 | 77,371 | 24 |
| 245,760 | 12,663 | 78,970 | 24 |

**2-D sweep** (`BenchmarkAppendRaw_BatchSizeWorkers`): sweeps batch sizes {100, 1000, 10000, 122880} × all worker counts up to GOMAXPROCS×2. Shows how batch size and parallelism interact — small batches starve workers; the optimal is ≥1000 msgs/batch at ≈GOMAXPROCS workers. Run with `make bench-go BENCH_FILTER=BenchmarkAppendRaw_BatchSizeWorkers`.

### Key findings

- **Batch size 1 → 100**: 15.1× cheaper per message, 52× fewer allocations — the single largest gain available.
- **Batch size 100 → 1,000**: diminishing returns (+18% msg/s). Parser throughput (~122 k msg/s i7, ~72 k EPYC realistic BidRequest) is now the ceiling, not flush overhead.
- **At 1,000+ messages**, `allocs/msg` is 26 and flat — RecordBatch builder overhead is fully amortised.

### Aligning batch size with the next pipeline stage

High throughput alone is not enough if the downstream consumer has to re-chunk your output:

- **DuckDB / Parquet:** DuckDB's default row group size is **122,880 rows**. Flushing exactly 122,880 messages per batch produces Arrow `RecordBatch`es that map 1:1 to Parquet row groups — no splitting or padding in the writer. This eliminates a memory copy and avoids partial row groups that degrade read performance.
- **2× row group (245,760):** Useful when you are writing multiple row groups per file or when your object-storage PUT size is the bottleneck. Keeps Arrow builder working memory in L3 rather than spilling to DRAM.
- **Network / Kafka consumers:** Align to the consumer's `max.partition.fetch.bytes` or gRPC stream chunk size so each `flush()` produces one wire message.

The rule is: **match your flush cadence to whatever granularity the next stage reads most efficiently**. Misalignment forces unnecessary copies at every stage boundary.

---

## Worker count tuning

### Go (pure Go pipeline)

Scale workers up to **GOMAXPROCS**. Beyond 2×GOMAXPROCS there is no gain.

| Workers | AppendRaw msg/s · i7 (GOMAXPROCS=20) | AppendRaw msg/s · EPYC (GOMAXPROCS=8) |
|---|---|---|
| 1 | 79,350 | 52,564 |
| 2 | 144,205 | 96,295 |
| 4 | 227,212 | 149,736 |
| 8 | 295,638 | 183,341 |
| 16 | 405,831 | **191,609** ← peak (2×GOMAXPROCS) |
| 32 | 454,293 | 188,379 (−2%) |
| 40 | 440,788 | — |
| 64 | 452,519 | — |
| 80 | **463,324** ← peak (4×GOMAXPROCS) | — |

_AppendRaw numbers from `BenchmarkMaxThroughput_ConcurrentAppendRaw`, BidRequest realistic corpus. Denorm (`BenchmarkMaxThroughput_ConcurrentAppendDenormRaw`) peaks at workers=16 on both i7 (481 k msg/s) and EPYC (226 k msg/s), staying flat from there._

### Python Pool

The single-threaded Python `submit()` loop becomes the bottleneck before Go's goroutine scheduler does. Optimal worker counts are lower than GOMAXPROCS.

**i7-13700H (GOMAXPROCS=20):**

| workers | AppendRaw | AppendMerged | AppendDenorm |
|---|---|---|---|
| 1 | 844k | 511k | 363k |
| **2** | **914k ← ✓ best** | **627k ← ✓ best** | 543k |
| **4** | 903k | 596k | **526k ← ✓ best** |
| 20 | 739k | 505k | 434k |
| 40 | 645k | 477k | 388k |

**AMD EPYC 7B13 (GOMAXPROCS=8):**

| workers | AppendRaw | AppendMerged | AppendDenorm |
|---|---|---|---|
| **1** | **422k ← ✓ best** | 248k | 162k |
| 2 | 378k | **274k ← ✓ best** | 227k |
| **4** | 350k | 244k | **236k ← ✓ best** |
| 8 | 338k | 232k | 219k |

**Practical rule:** Start with `workers=2` for AppendRaw and AppendMerged, `workers=4` for AppendDenorm. On cloud VMs with limited vCPU (GOMAXPROCS ≤ 4), try `workers=1` — the submit loop serialisation matters more at low core counts. Pass `workers=0` to auto-select `runtime.NumCPU()/2`.

---

## E2E pipeline throughput (proto → channel → transcode → DuckDB)

These benchmarks model the full [quacfka-service](https://github.com/loicalleyne/quacfka-service) channel architecture end-to-end:

1. **`mChan`** (`chan []byte`, cap=122,880) — pre-filled with serialised protobuf messages, simulating Kafka consumers feeding the pipeline.
2. **N worker goroutines** compete on `mChan` (natural load-balancing, not pre-sharded), calling `AppendRaw` or `AppendDenormRaw` per message. Each worker flushes one Arrow `RecordBatch` when the channel drains.
3. **`rChan`** (`chan arrow.Record`, cap=5) — flushed RecordBatches forwarded to the DuckDB sink.
4. **DuckDB sink goroutine** — calls `couac.Conn.Ingest` (ADBC Create-or-Append) for each `RecordBatch`, then releases it.

This measures end-to-end cost including goroutine scheduling, channel synchronisation, Arrow builder flush, and DuckDB ADBC write overhead. Compare with `BenchmarkMaxThroughput_ConcurrentAppendRaw` (pre-sharded, no channels, no DuckDB) to isolate the channel + DuckDB overhead.

_Run: `make bench-e2e` or `make bench-compare-e2e` — BidRequest corpus, HyperType PGO, i7-13700H (GOMAXPROCS=20)._

**AppendRaw E2E** (`BenchmarkE2EPipeline_ConcurrentAppendRaw`):

| Workers | ns/msg | msg/s | allocs/msg | vs MaxThroughput |
|---|---|---|---|---|
| 1 | 16,482 | 60,671 | 35 | — |
| 2 | 11,368 | 87,965 | 35 | — |
| 4 | 13,446 | 74,371 | 35 | — |
| 8 | 6,702 | **149,215** | 35 | 151 k → 149 k (−1%) |
| 16 | 13,394 | 74,658 | 36 | — |
| 32 | 6,386 | **156,597** | 36 | 454 k → 157 k (−65%) |
| 40 | 9,361 | 106,825 | 37 | — |
| 64 | 10,624 | 94,123 | 37 | — |
| 80 | 11,413 | 87,619 | 38 | — |

**AppendDenormRaw E2E** (`BenchmarkE2EPipeline_ConcurrentAppendDenormRaw`):

| Workers | ns/msg | msg/s | allocs/msg | vs MaxThroughput |
|---|---|---|---|---|
| 1 | 11,635 | 85,949 | 109 | — |
| 2 | 7,694 | 129,964 | 109 | — |
| 4 | 4,708 | 212,402 | 109 | — |
| 8 | 3,770 | **265,232** | 109 | — |
| 16 | 3,853 | **259,523** | 109 | 481 k → 260 k (−46%) |
| 32 | 4,547 | 219,907 | 109 | — |
| 40 | 4,347 | 230,067 | 109 | — |
| 64 | 5,036 | 198,568 | 109 | — |
| 80 | 4,689 | 213,280 | 109 | — |

### Key observations

- **DuckDB ADBC write is the dominant overhead.** AppendRaw peaks at 157 k msg/s E2E vs 463 k in pure transcoding — the DuckDB sink serialises all RecordBatch writes through a single connection, making it the pipeline bottleneck beyond ~8 workers.
- **AppendDenormRaw scales better** because each denorm RecordBatch has more rows (fan-out from `deals[*]`), amortising the per-batch ADBC overhead more effectively. Peak at 265 k msg/s (8 workers) vs 481 k pure transcoding (−46%).
- **Optimal worker count is lower** for E2E (8 workers) than pure transcoding (32–80 workers). Beyond 8 workers, the sink goroutine is saturated and additional workers just add channel contention.
- **`allocs/msg` is stable** at 35–38 (AppendRaw) and 109 (AppendDenormRaw); the channel ring-buffer allocation per `b.Loop()` iteration is negligible at 122,880 messages.
- **Production takeaway:** With a single DuckDB ADBC connection, 8 transcoding workers is optimal. File rotation in production pipelines serves a different purpose: it decouples heavy read workloads (aggregation queries, exports) from the live ingestion connection by running them asynchronously on a closed file while ingestion continues into the next one. Ingestion throughput is bounded by the single ADBC connection regardless of how many files are in rotation.

---

## HyperType

### What it is

`HyperType` is a Go type that wraps a compiled [`hyperpb.MessageType`](https://pkg.go.dev/buf.build/go/hyperpb) — a high-performance protobuf parser from [buf.build/go/hyperpb](https://github.com/bufbuild/hyperpb-go). Unlike `proto.Unmarshal` or `dynamicpb`, which must introspect the message descriptor at parse time for every message, `hyperpb` compiles the field layout once, up front, into a fast specialised parser. Subsequent parses skip all reflection overhead.

Without a `HyperType`, `AppendRaw` and `AppendDenormRaw` fall back to `dynamicpb` — reflection-based parsing with full field-by-field introspection on every message.

### How to create and use it

**Go:**
```go
// 1. Compile once from a protoreflect.MessageDescriptor.
//    Get md from generated code (MyMessage.ProtoReflect().Descriptor())
//    or from protoregistry, protodesc.NewFile, etc.
ht := bufarrowlib.NewHyperType(md)

// 2. Pass it to New() and/or Clone().
tc, _ := bufarrowlib.New(md, allocator, bufarrowlib.WithHyperType(ht))
worker := tc.Clone() // clone also inherits the HyperType

// 3. AppendRaw/AppendDenormRaw now use the compiled parser automatically.
tc.AppendRaw(rawBytes)
```

The `HyperType` is safe for concurrent use — all clones share it and parse in parallel without contention.

**Optional: profile-guided recompilation (PGO).** After collecting parse-time profiling data, the parser can be recompiled for the actual field distribution seen in your traffic:
```go
// Auto-recompile every 100,000 messages, sampling 1%.
ht := bufarrowlib.NewHyperType(md, bufarrowlib.WithAutoRecompile(100_000, 0.01))
// Or trigger manually when traffic is representative:
ht.Recompile()
```
Recompilation is atomic — all Transcoders sharing the `HyperType` pick up the new parser on their next call without any restarts or locks.

**Python (`pybufarrow`):**
```python
# hyper_type accepts a path to a compiled .pb descriptor file or a
# HyperType object returned by pybufarrow.compile_hyper_type().
tc = Transcoder.from_proto_file(
    "myschema.proto", "MyMessage",
    hyper_type=hyper_type,  # enables hyperpb fast path
)
```

### HyperType vs dynamicpb: the performance gap

The penalty for not providing a `HyperType` is consistent regardless of architecture:

| Path | With HyperType | Without (dynamicpb) | Penalty |
|---|---|---|---|
| `AppendRawMerged` (i7) | 106 k msg/s | 36 k msg/s | **3.0×** |
| `AppendRawMerged` (EPYC) | 70 k msg/s | 24 k msg/s | **2.9×** |
| `AppendDenormRawMerged` (i7) | 204 k msg/s | 47 k msg/s | **4.3×** |
| `AppendDenormRawMerged` (EPYC) | 130 k msg/s | 28 k msg/s | **4.7×** |

This overhead is purely from descriptor reflection at parse time — it is architecture-independent and cannot be tuned away at runtime. For any schema that is stable at startup, always provide a `HyperType`.

---

## Pre-creating clones vs creating Transcoders on demand

`Clone()` is the right way to spin up worker goroutines. It is significantly cheaper than `New()`:

| Schema | `New()` | `Clone()` | Ratio |
|---|---|---|---|
| ScalarTypes (i7) | 57 µs | 34 µs | 1.7× |
| BidRequest+denorm (i7) | 497 µs | 272 µs | 1.8× |
| ScalarTypes (EPYC) | 93 µs | 54 µs | 1.7× |
| BidRequest+denorm (EPYC) | 838 µs | 425 µs | 2.0× |

A pre-created pool of 20 clones achieves **617k denorm msg/s** (i7) · **369k** (EPYC) vs 79k (i7) single-threaded — a **7.8× speedup** from parallelism alone (`BenchmarkConcurrent_CloneAppendDenormRaw`).
Never create a Transcoder or Pool inside a message-processing loop.

---

## Python FFI overhead (fixed costs)

These costs are independent of message count — they are paid once per session or per flush:

| Operation | i7-13700H | EPYC 7B13 |
|---|---|---|
| `Transcoder()` / `Pool()` create + close | 2.1 ms | 0.33 ms |
| `flush()` on empty batch | 15 µs | 29 µs |
| `.schema` property access (cached) | ~0 ns | ~0.3 µs |

The `flush()` round-trip cost (~15–30 µs) is fully amortised at any reasonable batch size (≥ 100 msgs).

---

## Parquet write cost

Parquet I/O cost is additive on top of ingestion. Plan for it separately:

| Operation | i7-13700H | EPYC 7B13 |
|---|---|---|
| Write 100-row BidRequest to Parquet | 6.7 ms | 10.0 ms |
| Read 100-row BidRequest from Parquet | 5.7 ms | 8.0 ms |
| Round-trip (write + read) | 14.0 ms | 20.5 ms |
| End-to-end denorm pipeline (append + flush + write) | 552 µs/op | 0.9 ms/op |

At throughputs above 100 k msg/s, Parquet becomes the pipeline bottleneck — buffer larger batches (1,000–10,000 rows) before writing.

---

## Quick-start decision table

| I want to… | Use this | Notes |
|---|---|---|
| Ingest protobuf in Go, maximum throughput | `Transcoder.AppendRaw` + HyperType, workers = GOMAXPROCS | Pre-clone workers; batch 500–5,000 msgs/flush |
| Ingest in Python, simple single-threaded | `Transcoder.append_raw` | 1.4 µs/msg at 122 k batch; no tuning needed |
| Ingest in Python with parallelism | `Pool(workers=2)` for AppendRaw/Merged, `Pool(workers=4)` for Denorm | **Not ThreadPoolExecutor** |
| Merge base + custom fields at ingest | `AppendRawMerged` + HyperType | 3× faster than fallback; mandatory for > 100 k msg/s |
| Fan-out nested repeated fields to rows | `AppendDenormRaw` + HyperType | Cost scales with output column count, not input size |
| Custom fields + denorm together | `AppendDenormRawMerged` | ~204 k msg/s single-threaded; Pool[4] → ~500 k+ |
| Schema is unknown at startup | Any `AppendRaw*` without HyperType | Expect ~3× throughput penalty; add HyperType when schema stabilises |
| Write to Parquet | Built-in writer after flush | Buffer ≥ 1,000 rows before writing to hide I/O latency |

---

## Full benchmark tables

### Go — i7-13700H / GOMAXPROCS=20

<details>
<summary>Expand raw numbers</summary>

**Construction**

| Benchmark | ns/op | B/op | allocs/op |
|---|---|---|---|
| `BenchmarkNew/ScalarTypes` | 57,426 | 66,786 | 694 |
| `BenchmarkNew/MetricsData` | 738,518 | 831,848 | 8,449 |
| `BenchmarkNew/BidRequest` | 496,612 | 509,712 | 5,913 |
| `BenchmarkNewFromFile/ScalarTypes` | 2,564,676 | 482,582 | 7,564 |
| `BenchmarkNewFromFile/BidRequest` | 9,288,728 | 936,384 | 11,293 |
| `BenchmarkClone/ScalarTypes` | 34,398 | 42,536 | 417 |
| `BenchmarkClone/BidRequest_with_denorm` | 271,505 | 289,315 | 3,421 |

**Append (full schema)**

| Benchmark | throughput | msg/s |
|---|---|---|
| `BenchmarkAppend/ScalarTypes` | 194 MB/s | 1,681,063 |
| `BenchmarkAppend/MetricsData` | 41.9 MB/s | 42,934 |
| `BenchmarkAppend/BidRequest` | 73.1 MB/s | 64,474 |

**Raw / HyperType ingestion (BidRequest corpus)**

| Benchmark | ns/msg | msg/s | B/msg | allocs/msg |
|---|---|---|---|---|
| `BenchmarkAppendBidRequest_HyperpbRaw/Random` | 8,047 | 124,273 | 7,895 | 108.0 |
| `BenchmarkAppendBidRequest_HyperpbRaw/Realistic` | 6,606 | 151,381 | 6,190 | 97.52 |
| `BenchmarkAppendBidRequest_HyperpbPGO` | 10,601 | 94,328 | 6,204 | 107.4 |
| `BenchmarkAppendRaw/Random` | 13,479 | 74,190 | 6,149 | 140.2 |
| `BenchmarkAppendRaw/Realistic` | 9,088 | 110,030 | 2,865 | 27.85 |

**Merged**

| Benchmark | ns/msg | msg/s | B/msg | allocs/msg |
|---|---|---|---|---|
| `BenchmarkAppendRawMerged/HyperType` | 9,418 | 106,178 | 3,502 | 29.92 |
| `BenchmarkAppendRawMerged/Fallback` | 28,165 | 35,505 | 17,285 | 251.3 |
| `BenchmarkAppendDenormRawMerged` | 4,899 | 204,131 | 5,264 | 65.52 |
| `BenchmarkAppendWithCustom` | 5,446 | 183,620 | 2,842 | 45.83 |

**Denormalization**

| Benchmark | ns/msg | msg/s | B/msg | allocs/msg |
|---|---|---|---|---|
| `BenchmarkAppendBidRequest_Custom` | 6,736 | 148,450 | 5,562 | 131.2 |
| `BenchmarkAppendBidRequest_AppendDenorm` | 13,662 | 73,193 | 10,938 | 230.2 |
| `BenchmarkAppendDenorm/2x2` | 1,868 | 535,448 | 2,289 | 25.74 |
| `BenchmarkAppendDenorm/5x5` | 7,707 | 129,750 | 10,171 | 43.06 |
| `BenchmarkAppendDenorm/10x10` | 18,314 | 54,604 | 26,135 | 62.28 |
| `BenchmarkAppendDenormRaw_Fallback` | 21,094 | 47,407 | 17,723 | 275.1 |

**MaxThroughput Concurrent (122,880-msg corpus)**

| Workers | AppendRaw | AppendRawMerged | AppendDenormRaw |
|---|---|---|---|
| 1 | 79,350 | 73,415 | 95,382 |
| 2 | 144,205 | 132,945 | 170,263 |
| 4 | 227,212 | 231,050 | 275,520 |
| 8 | 295,638 | 332,156 | 362,059 |
| 16 | 405,831 | 397,536 | **481,409** |
| 32 | 454,293 | 423,603 | 437,068 |
| 40 | 440,788 | **432,053** | 421,850 |
| 64 | 452,519 | 409,537 | 420,793 |
| 80 | **463,324** | 406,125 | 406,577 |

**Batch size scaling (AppendRaw)**

| Batch | ns/msg | msg/s | allocs/msg |
|---|---|---|---|
| 1 | 147,628 | 6,774 | 2,446 |
| 100 | 9,749 | 102,577 | 47 |
| 1,000 | 8,243 | 121,315 | 26 |

**Parquet I/O**

| Benchmark | throughput | ns/op |
|---|---|---|
| `BenchmarkWriteParquet/BidRequest_100rows` | 15.4 MB/s | 6,694,504 |
| `BenchmarkReadParquet/BidRequest_100rows` | 18.2 MB/s | 5,680,994 |
| `BenchmarkParquetRoundTrip/BidRequest_100rows` | 8.09 MB/s | 14,017,939 |
| `BenchmarkProto/BidRequest_100rows` | 38.9 MB/s | 2,912,539 |
| `BenchmarkEndToEnd_DenormPipeline` | 19.8 MB/s | 551,548 |

</details>

---

### Go — AMD EPYC 7B13 / GOMAXPROCS=8

<details>
<summary>Expand raw numbers</summary>

**Construction**

| Benchmark | ns/op | B/op | allocs/op |
|---|---|---|---|
| `BenchmarkNew/ScalarTypes` | 92,712 | 66,787 | 694 |
| `BenchmarkNew/BidRequest` | 837,799 | 509,715 | 5,913 |
| `BenchmarkClone/ScalarTypes` | 54,373 | 42,536 | 417 |
| `BenchmarkClone/BidRequest_with_denorm` | 424,750 | 289,314 | 3,421 |

**Raw ingestion**

| Benchmark | ns/msg | msg/s | allocs/msg |
|---|---|---|---|
| `BenchmarkAppendBidRequest_HyperpbRaw/Random` | 12,520 | 79,871 | 108.0 |
| `BenchmarkAppendBidRequest_HyperpbRaw/Realistic` | 9,768 | 102,379 | 97.52 |
| `BenchmarkAppendRaw/Random` | 20,321 | 49,210 | 140.2 |
| `BenchmarkAppendRaw/Realistic` | 13,254 | 75,447 | 27.85 |

**Merged / Denorm**

| Benchmark | ns/msg | msg/s |
|---|---|---|
| `BenchmarkAppendRawMerged/HyperType` | 14,280 | 70,030 |
| `BenchmarkAppendRawMerged/Fallback` | 42,081 | 23,764 |
| `BenchmarkAppendDenormRawMerged` | 7,666 | 130,449 |

**MaxThroughput Concurrent (122,880-msg corpus)**

| Workers | AppendRaw | AppendRawMerged | AppendDenormRaw |
|---|---|---|---|
| 1 | 52,564 | 50,812 | 68,090 |
| 2 | 96,295 | 92,595 | 121,091 |
| 4 | 149,736 | 143,939 | 178,840 |
| 8 | 183,341 | 186,504 | 222,377 |
| 16 | **191,609** | 175,971 | 225,221 |
| 32 | 188,379 | 170,336 | **226,354** |

</details>

---

### Python — i7-13700H / GOMAXPROCS=20

<details>
<summary>Expand raw numbers</summary>

**Baseline (single Transcoder, 122,880-msg corpus)**

| Benchmark | mean | per-msg |
|---|---|---|
| `test_append_flush_122880` | 169.2 ms | 1.38 µs |
| `test_append_denorm_122880` | 148.0 ms | 1.21 µs |
| `test_append_merged_122880` | 365.4 ms | 2.97 µs |
| `test_create_close_cycle` | 2.07 ms | — |
| `test_flush_empty` | 15 µs | — |

**ThreadPoolExecutor (5,120-msg corpus per flush)**

| Path | w=1 | w=2 | w=4 | w=20 | w=40 |
|---|---|---|---|---|---|
| AppendRaw msg/s | 856k | 224k | 56k | 45k | 45k |
| AppendMerged msg/s | 371k | 101k | 53k | 43k | 41k |
| AppendDenorm msg/s | 298k | 162k | 46k | 37k | 38k |

**Pool (5,120-msg corpus per flush)**

| Path | w=1 | w=2 | w=4 | w=20 | w=40 |
|---|---|---|---|---|---|
| AppendRaw msg/s | 844k | **914k** | 903k | 739k | 645k |
| AppendMerged msg/s | 511k | **627k** | 596k | 505k | 477k |
| AppendDenorm msg/s | 363k | 543k | **526k** | 434k | 388k |

</details>

---

### Python — AMD EPYC 7B13 / GOMAXPROCS=8

<details>
<summary>Expand raw numbers</summary>

**Baseline (single Transcoder, 122,880-msg corpus)**

| Benchmark | mean | per-msg |
|---|---|---|
| `test_append_flush_122880` | 290.3 ms | 2.36 µs |
| `test_append_denorm_122880` | 256.4 ms | 2.09 µs |
| `test_append_merged_122880` | 557.1 ms | 4.53 µs |
| `test_create_close_cycle` | 0.33 ms | — |
| `test_flush_empty` | 29 µs | — |

**ThreadPoolExecutor (5,120-msg corpus per flush)**

| Path | w=1 | w=2 | w=4 | w=8 | w=16 |
|---|---|---|---|---|---|
| AppendRaw msg/s | 367k | 213k | 100k | 67k | 67k |
| AppendMerged msg/s | 196k | 127k | 70k | 55k | 51k |
| AppendDenorm msg/s | 137k | 111k | 72k | 57k | 58k |

**Pool (5,120-msg corpus per flush)**

| Path | w=1 | w=2 | w=4 | w=8 | w=16 |
|---|---|---|---|---|---|
| AppendRaw msg/s | **422k** | 378k | 350k | 338k | 320k |
| AppendMerged msg/s | 248k | **274k** | 244k | 232k | 243k |
| AppendDenorm msg/s | 162k | 227k | **236k** | 219k | 208k |

</details>

