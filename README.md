# bufarrowLib 🦬

<p align="center">
  <img src="assets/bufarrowlib-logo.png" alt="bufarrowlib-logo" width="800"/>
</p>

<p align="center">
  <a href="https://pkg.go.dev/github.com/loicalleyne/bufarrowlib"><img src="https://pkg.go.dev/badge/github.com/loicalleyne/bufarrowlib.svg" alt="Go Reference"></a>
  <a href="https://goreportcard.com/report/github.com/loicalleyne/bufarrowlib"><img src="https://goreportcard.com/badge/github.com/loicalleyne/bufarrowlib" alt="Go Report Card"></a>
</p>

**Protobuf → Apache Arrow. Raw bytes in, RecordBatches out. No codegen. No deserialization. No copies.**

Give bufarrowlib a protobuf descriptor and a stream of wire-format bytes. Get back Arrow `RecordBatch`es ready for DuckDB, Parquet, or any Arrow-native tool. Replace hundreds of lines of hand-written builder code with a YAML file or a path list. On production-shaped ad-tech traffic, `AppendDenormRaw` delivers **~296K msg/s — 39% faster than hand-written Arrow builders** with 57% fewer allocations.

Python bindings ([pybufarrow](python/)) give zero-copy access via the Arrow C Data Interface — all parsing runs in Go.

---

## Use cases

| Scenario | Why bufarrowlib |
|---|---|
| Kafka / gRPC → columnar analytics | Parse raw wire bytes directly; zero deserialization cost |
| ETL to Parquet / DuckDB / ClickHouse | Declarative denormalization via YAML or path list |
| OpenRTB / ad-tech event flattening | Fan-out across nested repeated fields in one pass |
| Schema-driven services | Runtime `.proto` compilation — no `protoc`, no codegen |
| Multi-goroutine stream processors | `Clone` + shared `HyperType`: independent builders, aggregated PGO |

---

## Install

```sh
go get -u github.com/loicalleyne/bufarrowlib@latest
```

---

## Quick Start

### Raw bytes → flat Arrow (fastest path)

```go
import (
    ba    "github.com/loicalleyne/bufarrowlib"
    "github.com/loicalleyne/bufarrowlib/proto/pbpath"
    "github.com/apache/arrow-go/v18/arrow/memory"
)

// Create once per message type — thread-safe, shared across goroutines.
ht := ba.NewHyperType(md, ba.WithAutoRecompile(10_000, 0.01))

tc, _ := ba.New(md, memory.DefaultAllocator,
    ba.WithHyperType(ht),
    ba.WithDenormalizerPlan(
        pbpath.PlanPath("id",                 pbpath.Alias("request_id")),
        pbpath.PlanPath("imp[*].id",          pbpath.Alias("imp_id")),
        pbpath.PlanPath("imp[*].bidfloor",    pbpath.Alias("floor")),
        pbpath.PlanPath("device.geo.country", pbpath.Alias("country")),
    ),
)
defer tc.Release()

for _, raw := range kafkaMessages {
    tc.AppendDenormRaw(raw)
}
rec := tc.NewDenormalizerRecordBatch()
defer rec.Release()
```

### YAML-driven config (no Go for the plan)

```yaml
# denorm.yaml
proto:
  file: schema/bidrequest.proto
  message: BidRequest
  import_paths: [./schema]

denormalizer:
  columns:
    - name: request_id
      path: id
    - name: imp_id
      path: imp[*].id
    - name: floor_price
      path: imp[*].bidfloor
    - name: imp_type
      expr:
        func: cond
        args:
          - expr: {func: has, args: [{path: imp[*].video.id}]}
          - literal: "video"
          - literal: "display"
```

```go
tc, _ := ba.NewTranscoderFromConfigFile("denorm.yaml", memory.DefaultAllocator)
```

### Full-fidelity proto → Arrow

```go
tc, _ := ba.New(md, memory.DefaultAllocator)
defer tc.Release()
tc.Append(msg)
rec := tc.NewRecordBatch()
defer rec.Release()
_ = tc.Schema()  // *arrow.Schema
_ = tc.Parquet() // *schema.Schema
```

### Multi-goroutine pipeline

```go
// Clone is ~2× cheaper than New. Shares the immutable plan + HyperType.
for i := 0; i < numWorkers; i++ {
    clone, _ := tc.Clone(memory.NewGoAllocator())
    go func(w *ba.Transcoder) {
        defer w.Release()
        for raw := range ch {
            w.AppendDenormRaw(raw)
        }
        rec := w.NewDenormalizerRecordBatch()
        // send rec downstream
    }(clone)
}
```

---

## Output modes

Two modes, one `Transcoder`. Use them individually or together.

| Mode | Append methods | Flush | Output |
|---|---|---|---|
| **Full-fidelity** | `Append`, `AppendRaw`, `AppendWithCustom`, `AppendRawMerged` | `NewRecordBatch()` | Nested Arrow schema mirroring the full protobuf structure |
| **Denormalization** | `AppendDenorm`, `AppendDenormRaw`, `AppendDenormRawMerged` | `NewDenormalizerRecordBatch()` | Flat Arrow schema from declared paths; fan-out across repeated fields |

---

## Features

### Ingestion API

| Method | Input | Speed | Notes |
|---|---|---|---|
| `Append(msg)` | `proto.Message` | baseline | Full-fidelity |
| `AppendRaw(b)` | `[]byte` | 110–151 k/s | Requires `HyperType` |
| `AppendRawMerged(base, custom)` | `[]byte, []byte` | 106 k/s | Field-safe wire merge |
| `AppendDenorm(msg)` | `proto.Message` | 73–535 k/s | Plan-based; fan-out dependent |
| `AppendDenormRaw(b)` | `[]byte` | **121–296 k/s** | Fastest; + `HyperType` recommended |
| `AppendDenormRawMerged(base, custom)` | `[]byte, []byte` | 204 k/s | Merge + denorm in one pass |

> Throughputs are single-threaded, realistic BidRequest corpus, i7-13700H. Scale linearly with `Clone` workers — see [Performance](#performance).

### HyperType — compiled parser + online PGO

`HyperType` wraps a [`hyperpb`](https://buf.build/docs/bsr/remote-packages/go#hyperpb) compiled message parser. Without it, `AppendRaw*` falls back to `dynamicpb` — consistently **2.9–4.7× slower**.

```go
// Create once per message type. Thread-safe. Share across all goroutines.
ht := ba.NewHyperType(md,
    ba.WithAutoRecompile(100_000, 0.01), // recompile every 100K msgs; 1% sampling
)

tc, _ := ba.New(md, mem, ba.WithHyperType(ht), ...)

// Manual recompile when traffic is representative:
ht.Recompile()         // synchronous
ht.RecompileAsync()    // background goroutine; returns <-chan struct{}
```

Recompilation is atomic — all `Transcoder`s sharing the `HyperType` pick up the new parser on their next call, no restarts.

### Denormalization + fan-out

```go
// 2 items × 3 tags → 6 output rows per message
tc, _ := ba.New(md, mem,
    ba.WithDenormalizerPlan(
        pbpath.PlanPath("order_id"),
        pbpath.PlanPath("items[*].id",    pbpath.Alias("item_id")),   // group A
        pbpath.PlanPath("items[*].price", pbpath.Alias("price")),
        pbpath.PlanPath("tags[*]",        pbpath.Alias("tag")),       // group B (cross-joined)
    ),
)
```

**Path syntax:**

| Syntax | Behaviour |
|---|---|
| `field` / `a.b.c` | scalar or nested message path |
| `repeated[*]` | wildcard fan-out — one row per element |
| `repeated[N]` | fixed index — scalar, no fan-out |
| `repeated[1:3]` | Python-style slice — fan-out over elements 1–2 |
| `repeated[-2:]` | negative indices supported |
| `repeated[::2]` | step-only slice |

Columns sharing the same wildcard steps are in the same **fan-out group** (lockstep). Different groups are **cross-joined**: `totalRows = ∏ groupSizes`. Empty groups emit one null row (left-join semantics).

### Expression engine

Computed columns, evaluated inline during plan traversal — no extra pass over the data.

```go
pbpath.PlanPath("buyer",
    pbpath.WithExpr(pbpath.FuncCoalesce(
        pbpath.PathRef("user.id"),
        pbpath.PathRef("device.ifa"),
    )),
    pbpath.Alias("buyer_id"),
),
```

**Available expression functions:**

| Category | Functions |
|---|---|
| Control flow | `cond`, `coalesce`, `default` |
| Predicates | `has`, `eq`, `ne`, `lt`, `le`, `gt`, `ge`, `and`, `or`, `not` |
| Arithmetic | `add`, `sub`, `mul`, `div`, `mod`, `abs`, `ceil`, `floor`, `round`, `min`, `max` |
| String | `concat`, `upper`, `lower`, `trim`, `trim_prefix`, `trim_suffix`, `len` |
| Cast | `cast_int`, `cast_float`, `cast_string` |
| Timestamp | `strptime`, `try_strptime`, `age`, `epoch_to_date`, `extract_year`…`extract_second`, `date_part` |
| ETL | `hash`, `bucket`, `mask`, `coerce`, `enum_name`, `sum`, `distinct`, `list_concat` |

Auxiliary YAML fields: `sep`, `literal` / `literal2`, `param`.

### Schema merging

Add fields from a second `.proto` to the base schema. Fields are renumbered above the base's max field number so raw wire bytes from both messages can be safely concatenated.

```go
tc, _ := ba.New(baseMD, mem, ba.WithCustomMessage(customMD))
tc.AppendRawMerged(baseBytes, customBytes)
```

### Parquet I/O

```go
var buf bytes.Buffer
tc.Append(msg)
tc.WriteParquet(&buf)

rec, _ := tc.ReadParquet(ctx, bytes.NewReader(buf.Bytes()), nil /* all cols */)
```

At > 100 k msg/s, Parquet I/O becomes the bottleneck. Buffer ≥ 1,000 rows before writing.

### Arrow → Protobuf back-decode

```go
msgs := tc.Proto(rec, nil)         // all rows
msgs := tc.Proto(rec, []int{0, 2}) // rows 0 and 2 only
```

### Protobuf Editions & runtime compilation

Works at the `protoreflect` level — compatible with proto2, proto3, and Edition 2023+.

```go
// No protoc required:
tc, _ := ba.NewFromFile("schema/event.proto", "EventMessage", []string{"./schema"}, mem)

// Or compile the descriptor yourself:
fd, _ := ba.CompileProtoToFileDescriptor("event.proto", []string{"./schema"})
md, _ := ba.GetMessageDescriptorByName(fd, "EventMessage")
```

---

## Performance

Benchmarks use a 506-message realistic BidRequest corpus (75% 2-imp messages, all fields populated). Raw data: [`docs/`](docs/). Reproduce: `make bench`.

### Single-threaded (i7-13700H, realistic corpus)

| Method | msg/s | ns/msg | allocs/msg |
|---|---|---|---|
| Hand-written Arrow getters | 180 k | 5,544 | 131 |
| `AppendDenorm` (proto.Message) | 73 k | 13,662 | 230 |
| `AppendRaw` (HyperType) | 151 k | 6,606 | 98 |
| `AppendDenormRaw` (HyperType, realistic) | **296 k** | **3,376** | **57** |
| `AppendDenormRaw` (no HyperType) | 47 k | 21,094 | 275 |
| `AppendRawMerged` (HyperType) | 106 k | 9,418 | 30 |
| `AppendDenormRawMerged` (HyperType) | 204 k | 4,899 | 66 |

**`AppendDenormRaw` with a HyperType beats hand-written code by 39% and uses 57% fewer allocations.**

### Concurrent scaling (AppendRaw, i7-13700H, GOMAXPROCS=20)

| Workers | msg/s |
|---|---|
| 1 | 79 k |
| 4 | 227 k |
| 8 | 296 k |
| 16 | 406 k |
| **80** | **463 k** |

`AppendDenormRaw` peaks at workers=16 → **481 k msg/s**.

### Batch size impact (AppendRaw, BidRequest, single worker)

| Batch | msg/s | allocs/msg |
|---|---|---|
| 1 | 6,774 | 2,446 |
| 100 | 102,577 | 47 |
| **1,000** | **121,315** | **26** |
| 122,880 | 129,689 | 24 |

**Use batch size ≥ 100. Align to 122,880 (DuckDB row group size) for direct Parquet ingest.**

### Clone vs New

`Clone` is ~2× cheaper than `New`. Never create a `Transcoder` inside a message loop.

| Schema | `New()` | `Clone()` |
|---|---|---|
| ScalarTypes | 57 µs | 34 µs |
| BidRequest + denorm | 497 µs | 272 µs |

Full benchmark tables and Python numbers: [`docs/benchmark-results.md`](docs/benchmark-results.md).

---

## Python Bindings

[**pybufarrow**](python/) — zero-copy Go→Python via the Arrow C Data Interface.

```sh
pip install pybufarrow
```

```python
from pybufarrow import HyperType, Transcoder

ht = HyperType("events.proto", "UserEvent")

with Transcoder.from_proto_file("events.proto", "UserEvent", hyper_type=ht) as tc:
    for raw in kafka_consumer:
        tc.append(raw)
    batch = tc.flush()

df = batch.to_pandas()
```

Denorm, streaming helpers, `Pool` for multi-worker throughput — see [python/README.md](python/README.md).

> **Use `Pool`, not `ThreadPoolExecutor`**. At 4 workers, `Pool` is 16× faster due to GIL behaviour.

---

## pbpath

[`proto/pbpath`](proto/pbpath) — standalone protobuf field-path engine, usable independently.

- Dot-path parsing with wildcards, Python-style slices, ranges, negative indices
- Trie-based `Plan` API: shared-prefix traversal for extracting many fields from the same message
- Full expression engine (30+ functions)

### pbpath-playground

Interactive web UI for testing paths and YAML denorm configs against real data before deployment:

```sh
go run ./cmd/pbpath-playground --proto path/to/schema.proto
```

Opens at `localhost:4195`. Two modes: **Pipeline** (live path evaluation against proto messages) and **Denorm** (live YAML config → Arrow table preview).

| Flag | Default | Description |
|---|---|---|
| `--proto` | required | `.proto` file(s), repeatable |
| `--import-path` | — | proto import directories, repeatable |
| `--corpus` | — | length-prefixed binary file for real-data testing |
| `--port` | `4195` | HTTP port |
| `--seed` | random | protorand seed for reproducible test messages |

---

## Well-known type mapping

| Protobuf | Arrow |
|---|---|
| `bool` | `Boolean` |
| `int32 / sint32 / sfixed32 / enum` | `Int32` |
| `uint32 / fixed32` | `Uint32` |
| `int64 / sint64 / sfixed64` | `Int64` |
| `uint64 / fixed64` | `Uint64` |
| `float` | `Float32` |
| `double` | `Float64` |
| `string` | `Utf8` |
| `bytes` | `Binary` |
| `google.protobuf.Timestamp` | `Timestamp(ms, UTC)` |
| `google.protobuf.Duration` | `Duration(ms)` |
| `google.protobuf.FieldMask` | `Utf8` |
| `google.protobuf.*Value` wrappers | unwrapped scalar |
| `google.type.Date` | `Date32` |
| `google.type.TimeOfDay` | `Time64(µs)` |
| `google.type.Money / LatLng / Color / PostalAddress / Interval` | `Utf8` (protojson) |
| OpenTelemetry `AnyValue` | `Binary` (proto-marshalled) |
| repeated field | `List<T>` |
| map field | `Map<K,V>` |
| embedded message | `Struct{...}` |

---

## Development

### Make targets

| Target | Description |
|---|---|
| `make test` | Run Go + Python tests |
| `make test-go` | Go tests only (`go test -timeout 180s ./...`) |
| `make test-python` | Python tests (uv-managed venv, pytest) |
| `make bench` | Run Go + Python benchmarks |
| `make bench-go` | Go benchmarks; filter with `BENCH_FILTER=BenchmarkFoo` |
| `make bench-python` | Python benchmarks (pytest-benchmark, outputs JSON) |
| `make bench-throughput` | Concurrent max-throughput benchmarks only |
| `make bench-compare` | Rotate previous results → `.old`, run, diff with `benchstat` |
| `make libbufarrow` | Build C shared library (`cbinding/libbufarrow.so`) |
| `make python` | Build `pybufarrow` wheel (requires `libbufarrow`) |
| `make python-dev` | Editable Python install for development |
| `make venv-sync` | Create/update uv-managed venv in `python/` |

**Benchmark variables:**

```sh
make bench-go BENCH_FILTER=BenchmarkAppendRaw BENCH_TIME=10s BENCH_COUNT=3
make bench-go BENCH_FILTER=BenchmarkMaxThroughput_ConcurrentAppendDenormRaw
```

| Variable | Default | Description |
|---|---|---|
| `BENCH_FILTER` | `.` (all) | `-bench` regex filter |
| `BENCH_TIME` | `3s` | `-benchtime` per benchmark |
| `BENCH_COUNT` | `1` | `-count` repetitions |
| `BENCH_OUT` | `docs/<cpu>-benchmark-results.txt` | Go output file |
| `BENCH_OUT_PYTHON` | `docs/<cpu>-benchmark-results-python.json` | Python output file |

`bench-compare` automatically detects the CPU model and writes per-machine result files. Run it twice to get a `benchstat` delta.

### Reference

- Full API: [pkg.go.dev/github.com/loicalleyne/bufarrowlib](https://pkg.go.dev/github.com/loicalleyne/bufarrowlib)
- pbpath reference: [proto/pbpath/README.md](proto/pbpath/README.md)
- Architecture guide: [proto/pbpath/ARCHITECTURE.md](proto/pbpath/ARCHITECTURE.md)
- Performance guide: [docs/benchmark-results.md](docs/benchmark-results.md)
- LLM-optimized reference: [llms.txt](llms.txt)
- Example YAML config: [testdata/example_denorm.yaml](testdata/example_denorm.yaml)

---

## Licence

bufarrowLib is released under the Apache 2.0 license. See [LICENCE.txt](LICENCE.txt)

