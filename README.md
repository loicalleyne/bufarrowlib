bufarrowLib 🦬
===================
[![Go Reference](https://pkg.go.dev/badge/github.com/loicalleyne/bufarrowlib.svg)](https://pkg.go.dev/github.com/loicalleyne/bufarrowlib)

Go library to build Apache Arrow records from Protocol Buffers.

## Features

### Schema generation
- Derive Arrow and Parquet schemas automatically from any protobuf message descriptor, including deeply-nested and recursive messages.
- Construct a `Transcoder` from compiled Go types (`protoreflect.MessageDescriptor`) or from `.proto` files at runtime via `NewFromFile`.
- Merge extra "custom" fields into the base schema with `WithCustomMessage` / `WithCustomMessageFile` — useful for enriching data with sidecar metadata.

### Full-fidelity transcoding
- **Proto → Arrow** — `Append` / `AppendWithCustom` populate an Arrow `RecordBuilder` that mirrors the full protobuf structure (structs, lists, maps, oneofs).
- **Arrow → Proto** — `Proto` reconstructs typed protobuf messages from Arrow record batches.
- **Proto → Parquet → Arrow** — `WriteParquet` / `WriteParquetRecords` write Arrow records to Parquet; `ReadParquet` reads them back.

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

### Cloning
`Transcoder.Clone` creates an independent copy (separate builders and stencils) that can be used in another goroutine. All options — including denormalization plans — carry over.

### Protobuf Editions support
bufarrowlib works at the `protoreflect` descriptor level, so it is inherently compatible with [Protobuf Editions](https://protobuf.dev/editions/overview/) (Edition 2023+) as well as proto2 and proto3. Editions features such as `features.field_presence` are resolved by the protobuf runtime into the same descriptor properties (`HasPresence`, `Kind`, `Cardinality`, etc.) that bufarrowlib already uses — no special configuration is needed. The `CompileProtoToFileDescriptor` / `NewFromFile` path uses `protocompile`, which supports Edition 2023.

## pbpath

The [`proto/pbpath`](proto/pbpath) subpackage is a standalone protobuf field-path engine that can be used independently of the rest of bufarrowlib.

- **Parse** a dot-delimited path string against a message descriptor, with support for list wildcards (`[*]`), Python-style slices (`[1:3]`, `[-2:]`, `[::2]`), ranges, and negative indices.
- **Evaluate** a compiled path against a live `proto.Message` to extract values, including fan-out across repeated fields.
- **Plan API** — compile multiple paths into a trie-based execution plan that traverses shared prefixes only once, ideal for hot-path extraction of many fields from the same message type.

See the full [pbpath README](proto/pbpath/README.md) and [pkg.go.dev reference](https://pkg.go.dev/github.com/loicalleyne/bufarrowlib/proto/pbpath) for details.

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

### Quick start — Parquet round-trip

```go
// Write
var buf bytes.Buffer
tc.Append(msg)
tc.WriteParquet(&buf)

// Read
rec, err := tc.ReadParquet(ctx, bytes.NewReader(buf.Bytes()), nil)
```

### Quick start — proto .proto file at runtime

```go
tc, err := bufarrowlib.NewFromFile(
    "path/to/schema.proto", "MyMessage",
    []string{"path/to/imports"},
    memory.DefaultAllocator,
)
```

## 💫 Show your support

Give a ⭐️ if this project helped you!
Feedback and PRs welcome.

## Licence

bufarrowLib is released under the Apache 2.0 license. See [LICENCE.txt](LICENCE.txt)