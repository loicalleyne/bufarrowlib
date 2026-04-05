# bufarrowlib vs. ProtobufMessageReflection — Benchmark & Correctness Report

> Comparison of bufarrowlib (`github.com/loicalleyne/bufarrowlib`) against the upstream
> `ProtobufMessageReflection` (PMR) from `github.com/apache/arrow-go/v18/arrow/util`.
>
> **Date:** 2025-04-05 (updated 2025-04-05)
> **Go:** 1.26.1 · **arrow-go:** v18.5.2 · **CPU:** 13th Gen Intel Core i7-13700H (20 threads)

---

## 1. What is ProtobufMessageReflection?

PMR is a per-message utility in `arrow-go/v18/arrow/util`:

```go
pmr := arrowutil.NewProtobufMessageReflection(msg)
schema := pmr.Schema()        // *arrow.Schema
rec    := pmr.Record(mem)      // arrow.RecordBatch (one row)
```

It takes an already-decoded `proto.Message`, derives an Arrow schema via reflection,
and produces a **single-row RecordBatch** per call. There is no batch accumulation,
no raw-bytes path, no denormalization, and no concurrent sharing model.

---

## 2. Upstream Bug: Missing `arrow.FLOAT32` in PMR

### Root Cause

PMR's `AppendValueOrNull` method (`arrow/util/protobuf_reflect.go:744`) is missing
a `case arrow.FLOAT32:` in its type switch. The switch handles `FLOAT64`, `INT32`,
`INT64`, `UINT32`, `UINT64`, `BOOL`, `STRING`, `BINARY`, `STRUCT`, `LIST`, `MAP`,
`DENSE_UNION`, and `DICTIONARY` — but **not `FLOAT32`**.

When a protobuf message contains a `float` field (proto `float` → Arrow `float32`):

1. The schema is derived correctly (`Schema()` works)
2. During `Record()`, `AppendValueOrNull` hits the `default` case and returns an error
3. `Record()` **silently ignores** the error (`f.AppendValueOrNull(...)` return value discarded)
4. The `Float32Builder` gets 0 appended values while all other builders get 1
5. `array.NewStructArray(arrays, fieldNames)` returns `nil` (array length mismatch)
6. The error is again silently ignored (`structArray, _ := ...`)
7. `array.RecordFromStructArray(nil, schema)` panics with nil-pointer dereference

### Impact

Any protobuf message with a `float` field will crash PMR's `Record()`. This affects:
- `ScalarTypes` (has `float` field → Arrow `float32`)
- `BidRequestEvent` (has `audience float` in nested `BidRequestDoohEvent`)
- Any production schema using proto `float`

PMR's `Schema()` always works. Only `Record()` is broken.

**Workaround:** Use message types that have no proto `float` fields (only `double`).
The `Known` message type (Timestamp + Duration, no floats) works correctly.

### Verification

```
arrow/util/protobuf_reflect.go:744  AppendValueOrNull switch:
  case arrow.STRING:    ✓ handled
  case arrow.BINARY:    ✓ handled
  case arrow.INT32:     ✓ handled
  case arrow.INT64:     ✓ handled
  case arrow.FLOAT64:   ✓ handled
  case arrow.FLOAT32:   ✗ MISSING
  case arrow.UINT32:    ✓ handled
  case arrow.UINT64:    ✓ handled
  case arrow.BOOL:      ✓ handled
  ...
  default: return error  (silently ignored by Record())
```

The fix is a one-line addition:
```go
case arrow.FLOAT32:
    b.(*array.Float32Builder).Append(float32(pv.Float()))
```

---

## 3. Correctness Tests

### TestSchema_VsPMR_ScalarTypes — PASS

Both libraries produce identical field names and compatible types for all 15
proto3 scalar fields (`double`, `float`, `int32`, …, `string`, `bytes`).

**Minor difference:** PMR marks all fields `nullable`; bufarrowlib marks
scalar fields non-nullable (except `bytes`). Both annotate the same underlying
Arrow type (`float64`, `int32`, `uint32`, `utf8`, `binary`, etc.).

bufarrowlib also attaches `PARQUET:field_id` and `path` metadata to each field
for round-trip Parquet support.

### TestSchema_VsPMR_TypeDifferences — PASS

For well-known types (`google.protobuf.Timestamp`, `google.protobuf.Duration`),
both libraries currently map to the same Arrow representation:

| Protobuf type | PMR | bufarrowlib |
|---|---|---|
| `Timestamp` | `Struct<seconds: int64, nanos: int32>` | `Struct<seconds: int64, nanos: int32>` |
| `Duration` | `Struct<seconds: int64, nanos: int32>` | `Struct<seconds: int64, nanos: int32>` |
| `repeated Timestamp` | `List<Struct<…>, nullable>` | `List<Struct<…>, nullable>` |
| `repeated Duration` | `List<Struct<…>, nullable>` | `List<Struct<…>, nullable>` |

### TestValues_VsPMR_ScalarTypes — SKIP (PMR FLOAT32 bug)

PMR's `Record()` panics on `ScalarTypes` due to the missing `FLOAT32` case
(see Section 2). bufarrowlib produces a valid 1-row, 15-column RecordBatch.

### TestValues_VsPMR_Known — PASS ✓

Full column-by-column value comparison using the `Known` message type
(Timestamp + Duration, no `float` fields). **All 4 fields match exactly:**

```
field "ts":           values match ([{"nanos":123456789,"seconds":1718000000}])
field "duration":     values match ([{"nanos":500000000,"seconds":3600}])
field "ts_rep":       values match ([[]])
field "duration_rep": values match ([[]])
```

---

## 4. Benchmark Results

### 4.1 Schema Construction

One-time cost to derive an Arrow schema from a protobuf descriptor.

| Message | Method | ns/op | B/op | allocs/op |
|---|---|---:|---:|---:|
| **ScalarTypes** (15 fields) | PMR | **21,800** | **15,928** | **104** |
| | bufarrowlib | 63,800 | 66,784 | 694 |
| **BidRequestEvent** (100+ fields) | PMR | **161,900** | **84,200** | **653** |
| | bufarrowlib | 564,100 | 509,697 | 5,913 |

PMR schema derivation is **~3× faster** for ScalarTypes and **~3.5× faster**
for BidRequestEvent. This is expected: PMR does lightweight field reflection
without building the full Parquet schema, nested node tree, or record builder
infrastructure that bufarrowlib constructs once and amortizes across all
subsequent Append calls.

**Key point:** Schema construction is a one-time cost (~0.5 ms for BidRequest).
In a streaming pipeline processing millions of messages, this cost is negligible.

### 4.2 Known Type: Three-Way Comparison (PMR works)

The `Known` message (Timestamp + Duration, nested structs, repeated fields)
is the only test type where PMR's `Record()` works (no `float` fields).
This provides the **only actual side-by-side comparison** of all three approaches.

| Batch | Method | ns/msg | B/msg | allocs/msg | msg/s |
|---:|---|---:|---:|---:|---:|
| **1** | PMR | 49,243 | 39,353 | 427 | 20,308 |
| 1 | bufarrowlib Append | **11,628** | **16,552** | **223** | **85,999** |
| 1 | bufarrowlib AppendRaw | 11,660 | 16,835 | 225 | 85,761 |
| **100** | PMR | 50,660 | 39,352 | 427 | 19,739 |
| 100 | bufarrowlib Append | **571** | **331** | **4.3** | **1,750,756** |
| 100 | bufarrowlib AppendRaw | 691 | 313 | 6.3 | 1,447,416 |
| **1000** | PMR | 54,139 | 39,352 | 427 | 18,471 |
| 1000 | bufarrowlib Append | **468** | **188** | **2.3** | **2,135,213** |
| 1000 | bufarrowlib AppendRaw | 672 | 206 | 8.8 | 1,488,502 |

**Key findings:**
- **Batch-1:** bufarrowlib is already **4.2× faster** than PMR (11.6 µs vs 49.2 µs)
  with 48% fewer allocations (223 vs 427) and 58% less memory (16.5 KB vs 39.4 KB).
- **Batch-100:** bufarrowlib/Append is **88× faster** than PMR (571 ns vs 50,660 ns)
  with **99× fewer allocations** (4.3 vs 427). PMR cannot batch — its cost stays constant.
- **Batch-1000:** bufarrowlib/Append is **115× faster** than PMR (468 ns vs 54,139 ns)
  with **186× fewer allocations** (2.3 vs 427) and **209× less memory** (188 B vs 39,352 B).
- PMR throughput stays flat at ~19–20K msg/s regardless of batch size.
  bufarrowlib scales from 86K msg/s (batch-1) to **2.1M msg/s** (batch-1000).

### 4.3 ScalarTypes: Batch-Size Scaling (PMR skipped)

PMR sub-benchmarks were skipped (FLOAT32 bug). bufarrowlib-only results:

| Batch | Method | ns/msg | B/msg | allocs/msg | msg/s |
|---:|---|---:|---:|---:|---:|
| 1 | Append | 7,570 | 11,712 | 112.0 | 132,100 |
| 1 | AppendRaw | 8,400 | 12,340 | 134.1 | 119,100 |
| **100** | **Append** | **653** | **349** | **1.5** | **1,531,000** |
| 100 | AppendRaw | 1,508 | 750 | 23.6 | 663,600 |
| **1,000** | **Append** | **608** | **275** | **0.2** | **1,646,300** |
| 1,000 | AppendRaw | 1,372 | 673 | 22.3 | 729,000 |

At batch=100, per-message cost drops 11.6× (Append) and 5.6× (AppendRaw)
compared to batch=1.

### 4.4 BidRequest: Batch-Size Scaling (PMR skipped)

Production-realistic corpus: 506 BidRequestEvent messages (~220 bytes avg),
75% with 2 impressions, nested deals, mixed banner/video.

| Batch | Method | ns/msg | B/msg | allocs/msg | msg/s | MB/s |
|---:|---|---:|---:|---:|---:|---:|
| 1 | Append | 167,700 | 201,800 | 2,477 | 5,960 | 1.31 |
| 1 | AppendRaw | 155,500 | 203,500 | 2,446 | 6,430 | 1.41 |
| **506** | **Append** | **12,190** | **5,498** | **71** | **82,000** | **24.5** |
| **506** | **AppendRaw** | **9,420** | **2,910** | **28** | **106,300** | **31.7** |

At batch=506, `AppendRaw` is **1.30× faster** than `Append` with **47% fewer
bytes** and **61% fewer allocations**. The HyperType compiled parser advantage
becomes dominant on complex nested messages.

---

## 5. Summary

| Dimension | PMR | bufarrowlib |
|---|---|---|
| **Schema derivation** | 3–3.5× faster (lightweight) | Slower (builds full infrastructure), but one-time cost |
| **Record production** | **Broken for float fields** (FLOAT32 bug) | Works for all message types |
| **Per-message (batch=1)** | 49 µs, 39 KB, 427 allocs | **12 µs, 17 KB, 223 allocs** (4.2× faster) |
| **Batched (batch=1000)** | 54 µs (no batching) | **0.47 µs** (Append), **0.67 µs** (AppendRaw) — **115× faster** |
| **Throughput at scale** | ~20K msg/s (flat) | **2.1M msg/s** (Append, batch=1000) |
| **Allocations at scale** | 427/msg (constant) | **2.3/msg** (batch=1000) — **186× fewer** |
| **Raw bytes path** | None (requires decoded `proto.Message`) | `AppendRaw` skips deserialization |
| **Denormalization** | Not supported | Fan-out + expressions + YAML config |
| **Concurrency** | No sharing model | `HyperType` shared, `Clone` per goroutine |
| **Parquet metadata** | Not attached | `PARQUET:field_id` on every field |

**For one-shot schema inspection**, PMR is lighter (when `Schema()` works).
**For production data pipelines**, bufarrowlib is the only viable option — it can
actually produce records, batches efficiently, handles raw bytes without
deserialization, and scales across goroutines.

---

## 6. Reproduce

```sh
# Correctness tests
go test -v -run 'TestSchema_VsPMR|TestValues_VsPMR' -count=1 .

# PMR bug diagnosis
go test -v -run 'TestPMR_Debug' -count=1 .

# All comparison benchmarks (includes Known which has actual PMR numbers)
go test -bench 'BenchmarkVsPMR' -benchmem -count=3 -timeout 300s -benchtime=200ms .

# Known benchmarks only (PMR works here)
go test -bench 'BenchmarkVsPMR_Known' -benchmem -count=1 -benchtime=2s .

# Schema construction only (fast)
go test -bench 'BenchmarkVsPMR_SchemaConstruction' -benchmem -count=5 .
```

Test files: `comparison_test.go`, `bench_comparison_test.go`, `pmr_debug_test.go`
