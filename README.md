bufarrowLib 🦬
===================
[![Go Reference](https://pkg.go.dev/badge/github.com/loicalleyne/bufarrowlib.svg)](https://pkg.go.dev/github.com/loicalleyne/bufarrowlib)

Go library to build Apache Arrow records from Protocol Buffers — with high-performance raw-bytes ingestion, automatic denormalization, Parquet I/O, and an embedded expression engine.

## Features

### Schema generation
- Derive Arrow and Parquet schemas automatically from any protobuf message descriptor, including deeply-nested and recursive messages.
- Construct a `Transcoder` from compiled Go types (`protoreflect.MessageDescriptor`) or from `.proto` files at runtime via `NewFromFile`.
- Merge extra "custom" fields into the base schema with `WithCustomMessage` / `WithCustomMessageFile` — useful for enriching data with sidecar metadata.

### Full-fidelity transcoding
- **Proto → Arrow** — `Append` / `AppendWithCustom` populate an Arrow `RecordBuilder` that mirrors the full protobuf structure (structs, lists, maps, oneofs).
- **Arrow → Proto** — `Proto` reconstructs typed protobuf messages from Arrow record batches.
- **Proto → Parquet → Arrow** — `WriteParquet` / `WriteParquetRecords` write Arrow records to Parquet; `ReadParquet` reads them back.

### High-performance raw-bytes ingestion

`AppendRaw` and `AppendDenormRaw` accept raw protobuf wire bytes and decode them using [hyperpb](https://buf.build/docs/bsr/remote-packages/go#hyperpb) — a TDP-based parser that is 2–3× faster than generated code. Combined with `hyperpb.Shared` arena reuse and optional profile-guided optimization (PGO), this is the fastest path for streaming protobuf data into Arrow.

| Method | What it does | When to use |
|---|---|---|
| `AppendRaw([]byte)` | Unmarshal → full Arrow record | You have raw bytes from Kafka/gRPC and want the full protobuf structure in Arrow |
| `AppendDenormRaw([]byte)` | Unmarshal → flat denormalized record | You have raw bytes and want a flat analytics table (selected columns, fan-out) |

Both methods require a `HyperType` coordinator (see below).

### HyperType — shared compiled parser with online PGO

`HyperType` wraps a compiled `hyperpb.MessageType` in a thread-safe coordinator that can be shared across multiple `Transcoder` instances (including clones in separate goroutines). It supports optional **online profile-guided optimization** (PGO): all transcoders contribute profiling data, and a recompile atomically upgrades the parser for all of them.

```go
// Create once, share across goroutines.
ht := bufarrowlib.NewHyperType(md,
    bufarrowlib.WithAutoRecompile(10_000, 0.01), // recompile every 10K messages, 1% sampling
)

// Each goroutine gets its own Transcoder, sharing the same HyperType.
tc, _ := bufarrowlib.New(md, mem, bufarrowlib.WithHyperType(ht), /* ... */)
clone, _ := tc.Clone(mem)

// Feed raw bytes — profiling happens automatically.
tc.AppendDenormRaw(rawBytes)

// Manual recompile (if not using auto-recompile):
ht.Recompile()      // synchronous — blocks until done
ht.RecompileAsync()  // non-blocking — returns a channel
```

**Key HyperType concepts:**

| Concept | Description |
|---|---|
| `NewHyperType(md)` | Compile a `hyperpb.MessageType` from a message descriptor |
| `WithAutoRecompile(threshold, rate)` | Automatically recompile after `threshold` messages; `rate` is the profiling sample fraction (e.g. 0.01 = 1%) |
| `ht.Type()` | Load the current compiled parser (atomic, lock-free) |
| `ht.Recompile()` | Recompile using collected profile data (CAS-guarded — safe to call concurrently) |
| `ht.RecompileAsync()` | Non-blocking recompile in a background goroutine |

### Well-known & common type support

The denormalizer automatically maps [protobuf well-known types and common types](https://protobuf.dev/best-practices/dos-donts/#well-known-common) to flat Arrow scalars — no manual conversion needed.

| Protobuf type | Arrow type |
|---|---|
| `google.protobuf.Timestamp` | `Timestamp(ms, UTC)` |
| `google.protobuf.Duration` | `Duration(ms)` |
| `google.protobuf.FieldMask` | `String` (comma-joined paths) |
| `google.protobuf.*Value` wrappers | unwrapped scalar (`BoolValue` → `Boolean`, `Int64Value` → `Int64`, etc.) |
| `google.type.Date` | `Date32` |
| `google.type.TimeOfDay` | `Time64(µs)` |
| `google.type.Money` | `String` (protojson) |
| `google.type.LatLng` | `String` (protojson) |
| `google.type.Color` | `String` (protojson) |
| `google.type.PostalAddress` | `String` (protojson) |
| `google.type.Interval` | `String` (protojson) |
| OpenTelemetry `AnyValue` | `Binary` (proto-marshalled) |

### Denormalization

Use `WithDenormalizerPlan` to project selected protobuf field paths into a flat Arrow record — like SQL `SELECT … FROM msg CROSS JOIN UNNEST(msg.items)`.

- Paths are specified with the [`pbpath`](#pbpath) path language: `"items[*].id"`, `"tags[*]"`, `"imp[0:3].banner.w"`, `"name"`, etc.
- Column aliases via `pbpath.Alias("col_name")`.
- Independent repeated-field fan-outs are **cross-joined**; empty groups emit a single null row (**left-join** semantics).
- Fixed-index paths (`items[0].id`) broadcast as scalars; ranges and wildcards fan out.
- Computed columns via the [Expr engine](#expression-engine): `FuncCoalesce`, `FuncCond`, `FuncConcat`, arithmetic, string ops, timestamp parsing, and more.
- All type mapping and append closures are compiled once at construction time for minimal per-message overhead.

```go
tc, _ := bufarrowlib.New(md, mem,
    bufarrowlib.WithDenormalizerPlan(
        pbpath.PlanPath("name",            pbpath.Alias("order_name")),
        pbpath.PlanPath("items[*].id",     pbpath.Alias("item_id")),
        pbpath.PlanPath("items[*].price",  pbpath.Alias("item_price")),
        pbpath.PlanPath("tags[*]",         pbpath.Alias("tag")),
    ),
)
tc.AppendDenorm(msg) // 2 items × 3 tags → 6 rows
rec := tc.NewDenormalizerRecordBatch()
```

### Expression engine

The denormalizer's Plan API supports computed columns through a composable `Expr` tree. Expressions reference protobuf field paths via `PathRef` and apply functions to produce derived values — all evaluated inline during plan traversal with zero extra passes over the data.

```go
// Coalesce: first non-zero value from multiple paths
pbpath.PlanPath("device_id",
    pbpath.WithExpr(pbpath.FuncCoalesce(
        pbpath.PathRef("user.id"),
        pbpath.PathRef("site.id"),
        pbpath.PathRef("device.ifa"),
    )),
    pbpath.Alias("device_id"),
)

// Conditional: banner dimensions if present, else video
pbpath.PlanPath("width",
    pbpath.WithExpr(pbpath.FuncCond(
        pbpath.FuncHas(pbpath.PathRef("imp[0].banner.w")),
        pbpath.PathRef("imp[0].banner.w"),
        pbpath.PathRef("imp[0].video.w"),
    )),
    pbpath.Alias("width"),
)
```

**Available expression functions:**

| Category | Functions |
|---|---|
| Control flow | `FuncCoalesce`, `FuncDefault`, `FuncCond` |
| Predicates | `FuncHas`, `FuncLen`, `FuncEq`, `FuncNe`, `FuncLt`, `FuncLe`, `FuncGt`, `FuncGe` |
| Arithmetic | `FuncAdd`, `FuncSub`, `FuncMul`, `FuncDiv`, `FuncMod` |
| Math | `FuncAbs`, `FuncCeil`, `FuncFloor`, `FuncRound`, `FuncMin`, `FuncMax` |
| String | `FuncUpper`, `FuncLower`, `FuncTrim`, `FuncTrimPrefix`, `FuncTrimSuffix`, `FuncConcat` |
| Cast | `FuncCastInt`, `FuncCastFloat`, `FuncCastString` |
| Timestamp | `FuncStrptime`, `FuncTryStrptime`, `FuncAge`, `FuncExtractYear`/`Month`/`Day`/`Hour`/`Minute`/`Second` |
| ETL | `FuncHash`, `FuncEpochToDate`, `FuncDatePart`, `FuncBucket`, `FuncMask`, `FuncCoerce`, `FuncEnumName` |
| Aggregates | `FuncSum`, `FuncDistinct`, `FuncListConcat` |

### Cloning

`Transcoder.Clone` creates an independent copy (separate builders and stencils) that can be used in another goroutine. All options — including denormalization plans and `HyperType` references — carry over. The immutable `Plan` is shared; only the mutable Arrow builders and scratch buffers are freshly allocated.

```go
// Original transcoder
tc, _ := bufarrowlib.New(md, mem, bufarrowlib.WithHyperType(ht), /* ... */)

// Clone for another goroutine — shares HyperType + Plan, fresh builders
clone, _ := tc.Clone(mem)

go func() {
    defer clone.Release()
    for raw := range ch {
        clone.AppendDenormRaw(raw)
    }
    rec := clone.NewDenormalizerRecordBatch()
    // ... process rec
}()
```

### Protobuf Editions support
bufarrowlib works at the `protoreflect` descriptor level, so it is inherently compatible with [Protobuf Editions](https://protobuf.dev/editions/overview/) (Edition 2023+) as well as proto2 and proto3. Editions features such as `features.field_presence` are resolved by the protobuf runtime into the same descriptor properties (`HasPresence`, `Kind`, `Cardinality`, etc.) that bufarrowlib already uses — no special configuration is needed. The `CompileProtoToFileDescriptor` / `NewFromFile` path uses `protocompile`, which supports Edition 2023.

## pbpath

The [`proto/pbpath`](proto/pbpath) subpackage is a standalone protobuf field-path engine that can be used independently of the rest of bufarrowlib.

- **Parse** a dot-delimited path string against a message descriptor, with support for list wildcards (`[*]`), Python-style slices (`[1:3]`, `[-2:]`, `[::2]`), ranges, and negative indices.
- **Evaluate** a compiled path against a live `proto.Message` to extract values, including fan-out across repeated fields.
- **Plan API** — compile multiple paths into a trie-based execution plan that traverses shared prefixes only once, ideal for hot-path extraction of many fields from the same message type.
- **Expr engine** — composable expression trees for computed columns: `Coalesce`, `Cond`, arithmetic, string ops, timestamp parsing, and 30+ built-in functions.

See the full [pbpath README](proto/pbpath/README.md), [Architecture Guide](proto/pbpath/ARCHITECTURE.md), and [pkg.go.dev reference](https://pkg.go.dev/github.com/loicalleyne/bufarrowlib/proto/pbpath) for details.

## 🚀 Install

```sh
go get -u github.com/loicalleyne/bufarrowlib@latest
```

## 💡 Usage

```go
import "github.com/loicalleyne/bufarrowlib"
```

### Quick start — full record

```go
tc, err := bufarrowlib.New(md, memory.DefaultAllocator)
tc.Append(msg)
rec := tc.NewRecordBatch()
defer rec.Release()

// Arrow schema & Parquet schema are available:
_ = tc.Schema()   // *arrow.Schema
_ = tc.Parquet()  // *schema.Schema
```

### Quick start — denormalized record

```go
tc, err := bufarrowlib.New(md, memory.DefaultAllocator,
    bufarrowlib.WithDenormalizerPlan(
        pbpath.PlanPath("items[*].id",    pbpath.Alias("item_id")),
        pbpath.PlanPath("items[*].price", pbpath.Alias("item_price")),
    ),
)
tc.AppendDenorm(msg)
rec := tc.NewDenormalizerRecordBatch()
defer rec.Release()
```

### Quick start — high-performance raw-bytes ingestion

```go
// 1. Create a shared HyperType (once per message type, thread-safe).
ht := bufarrowlib.NewHyperType(md)

// 2. Create a Transcoder with HyperType + denormalization plan.
tc, err := bufarrowlib.New(md, memory.DefaultAllocator,
    bufarrowlib.WithHyperType(ht),
    bufarrowlib.WithDenormalizerPlan(
        pbpath.PlanPath("id",           pbpath.Alias("request_id")),
        pbpath.PlanPath("imp[*].id",    pbpath.Alias("imp_id")),
        pbpath.PlanPath("device.geo.country", pbpath.Alias("country")),
    ),
)

// 3. Feed raw protobuf bytes (e.g. from Kafka).
for _, raw := range messages {
    tc.AppendDenormRaw(raw)
}

// 4. Flush to Arrow.
rec := tc.NewDenormalizerRecordBatch()
defer rec.Release()
```

### Quick start — Parquet round-trip

```go
// Write
var buf bytes.Buffer
tc.Append(msg)
tc.WriteParquet(&buf)

// Read
rec, err := tc.ReadParquet(ctx, bytes.NewReader(buf.Bytes()), nil)
```

### Quick start — .proto file at runtime

```go
tc, err := bufarrowlib.NewFromFile(
    "path/to/schema.proto", "MyMessage",
    []string{"path/to/imports"},
    memory.DefaultAllocator,
)
```

### Quick start — computed columns with expressions

```go
tc, err := bufarrowlib.New(md, memory.DefaultAllocator,
    bufarrowlib.WithDenormalizerPlan(
        pbpath.PlanPath("id", pbpath.Alias("request_id")),

        // Coalesce: first non-empty ID
        pbpath.PlanPath("device_id",
            pbpath.WithExpr(pbpath.FuncCoalesce(
                pbpath.PathRef("user.id"),
                pbpath.PathRef("device.ifa"),
            )),
            pbpath.Alias("device_id"),
        ),

        // Conditional: banner width or video width
        pbpath.PlanPath("width",
            pbpath.WithExpr(pbpath.FuncCoalesce(
                pbpath.PathRef("imp[0].banner.w"),
                pbpath.PathRef("imp[0].video.w"),
            )),
            pbpath.Alias("width"),
        ),

        // Fan-out across deals
        pbpath.PlanPath("imp[0].pmp.deals[*].id", pbpath.Alias("deal_id")),
    ),
)
```

## 📊 Benchmarks

Benchmarks use a realistic 506-message corpus shaped to match sampled production ad-tech traffic (OpenRTB BidRequest): 75% have 2 imp objects, 61% have 1 deal object per impression, banner and video mutually exclusive dimensions, all top-level fields populated.

### BidRequest denormalization — method comparison

| Method | ns/msg | msg/s | B/msg | allocs/msg | Description |
|---|---|---|---|---|---|
| **Custom** (hand-written getters) | 5,544 | 180K | 5,562 | 131.2 | Manual Arrow builders with typed getters |
| **AppendDenorm** (proto.Message) | 8,921 | 112K | 7,022 | 184.6 | Plan-based denorm from deserialized proto |
| **AppendDenormRaw** (random data) | 4,939 | 202K | 3,951 | 62.4 | hyperpb + Shared arena, random corpus |
| **AppendDenormRaw** (realistic data) | **3,376** | **296K** | **2,476** | **56.9** | hyperpb + Shared arena, production-shaped corpus |
| **AppendDenormRaw + PGO** | 7,046 | 142K | 2,508 | 66.7 | With profile-guided recompilation |

> **AppendDenormRaw with realistic data is 39% faster than hand-written code** — with 57% fewer allocations per message — while being fully declarative (no manual Arrow builders to maintain).

### Other benchmarks

| Benchmark | Time | Note |
|---|---|---|
| `New/BidRequest` | ~220 µs | Schema construction from descriptor |
| `NewFromFile/BidRequest` | ~5.5 ms | Compile .proto + construct schema |
| `Append/BidRequest` (100 msgs) | ~2.8 ms | Full-fidelity proto → Arrow |
| `Clone/BidRequest_with_denorm` | ~110 µs | Clone transcoder + denorm plan |
| `WriteParquet/BidRequest` (100 rows) | ~1.5 ms | Arrow → Parquet |
| `ReadParquet/BidRequest` (100 rows) | ~760 µs | Parquet → Arrow |
| `AppendDenorm/2x2` (100 msgs) | ~1.5 ms | Denorm with 2 items × 2 tags |
| `AppendDenorm/10x10` (100 msgs) | ~16 ms | Denorm with 10 items × 10 tags |

### Running benchmarks

```sh
# Run all benchmarks
go test -bench=. -benchmem -count=3

# Run only the BidRequest comparison benchmarks
go test -bench='BenchmarkAppendBidRequest' -benchmem -count=3

# Run with longer duration for more stable results
go test -bench='BenchmarkAppendBidRequest' -benchmem -benchtime=5s -count=5

# Run a specific sub-benchmark
go test -bench='BenchmarkAppendBidRequest_HyperpbRaw/Realistic' -benchmem

# Profile CPU
go test -bench='BenchmarkAppendBidRequest_HyperpbRaw/Realistic' -cpuprofile=cpu.prof
go tool pprof cpu.prof

# Profile memory allocations
go test -bench='BenchmarkAppendBidRequest_HyperpbRaw/Realistic' -memprofile=mem.prof
go tool pprof -alloc_objects mem.prof
```

**Flags reference:**

| Flag | Default | Description |
|---|---|---|
| `-bench=<regex>` | (none) | Run benchmarks matching the regex |
| `-benchmem` | off | Report allocations (B/op, allocs/op) |
| `-benchtime=<d>` | `1s` | Target wall-clock time per benchmark; Go auto-adjusts `b.N` |
| `-count=<n>` | `1` | Repeat each benchmark `n` times for statistical comparison |
| `-cpuprofile=<file>` | (none) | Write CPU profile to file |
| `-memprofile=<file>` | (none) | Write memory profile to file |

## Choosing the right ingestion method

| Scenario | Method | Why |
|---|---|---|
| You have `proto.Message` objects (from generated code or gRPC) | `Append` / `AppendDenorm` | No marshal/unmarshal overhead — pass the message directly |
| You have raw `[]byte` from Kafka, a file, or a network stream | `AppendRaw` / `AppendDenormRaw` | Avoids double-unmarshal; hyperpb parser is faster than `proto.Unmarshal` |
| You need the full protobuf structure as nested Arrow | `Append` / `AppendRaw` | Produces a hierarchical Arrow record matching the proto schema |
| You need a flat analytics table with selected columns | `AppendDenorm` / `AppendDenormRaw` | Declarative path selection, automatic fan-out, computed columns via Exprs |
| Multi-goroutine pipeline | `Clone` with shared `HyperType` | Each goroutine gets independent builders; profiling data is aggregated |
| Evolving traffic shape | `WithAutoRecompile` | PGO adapts the parser as field-presence patterns change |

## 💫 Show your support

Give a ⭐️ if this project helped you!
Feedback and PRs welcome.

## Licence

bufarrowLib is released under the Apache 2.0 license. See [LICENCE.txt](LICENCE.txt)