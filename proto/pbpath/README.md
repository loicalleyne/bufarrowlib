# pbpath

`pbpath` lets you address any value inside a protobuf message using a
compact, human-readable string path. Given a `.proto` schema and a message
descriptor you can **parse** a path string, then **traverse** a live message to
extract the values at that location — including fan-out across repeated fields
via wildcards, ranges, and Python-style slices.

For hot paths that evaluate **multiple paths** against many messages of the same
type, the **Plan API** compiles paths into a trie-based execution plan that
traverses shared prefixes only once.

For a deep dive into the internal object model, trie structure, expression
system, and performance recommendations, see the [Architecture Guide](ARCHITECTURE.md).

## Core API

```go
// Parse a path string against a message descriptor.
path, err := pbpath.ParsePath(md, "device.geo.country")

// Walk the path through a concrete message to collect values at every step.
// PathValues returns a slice — one Values per matching branch when the path
// contains wildcards, ranges, or slices.
results, err := pbpath.PathValues(path, msg)

// For a scalar path (no fan-out) there is exactly one result.
last := results[0].Index(-1)
fmt.Println(last.Value.String()) // e.g. "US"
```

### PathValues

```go
func PathValues(p Path, m proto.Message, opts ...PathOption) ([]Values, error)
```

`PathValues` walks the given `Path` through message `m` and returns every
matching `Values` branch. When the path contains **fan-out steps** —
`ListWildcardStep`, `ListRangeStep`, or a `ListIndexStep` with a negative
index — the function produces one `Values` per matching element. Nested
fan-out steps produce the **cartesian product** of all matches.

Each `Values` carries:

- **`Path`** — the concrete path taken (wildcards/ranges are replaced with the
  actual `ListIndex` of each element).
- **`Values`** — the `protoreflect.Value` at every step along that path.

#### Options

| Option | Effect |
|---|---|
| `Strict()` | Return an error when a negative index or range bound resolves out-of-bounds. Without it, out-of-range accesses are silently clamped or the branch is skipped. |

### Values helpers

```go
// Index returns the (step, value) pair at position i (supports negative indices).
pair := vals.Index(-1)   // last step+value
pair.Step                // the Step
pair.Value               // the protoreflect.Value

// ListIndices returns the concrete list indices visited along the path.
indices := vals.ListIndices() // e.g. [0, 2] for repeats[0].inner[2]

// String returns a human-readable "path = value" representation.
fmt.Println(vals.String())
// (pkg.Msg).items[0].name = "widget"
```

## Multi-Path Plan API

When you need to extract several values from every message in a stream, the
**Plan API** avoids redundant work by sharing traversal across paths with common
prefixes.

### Quick Start

```go
// Compile once.
plan, err := pbpath.NewPlan(md,
    pbpath.PlanPath("device.geo.country",       pbpath.Alias("country")),
    pbpath.PlanPath("device.geo.city",           pbpath.Alias("city")),
    pbpath.PlanPath("imp[*].id",                 pbpath.Alias("imp_id")),
    pbpath.PlanPath("imp[*].pmp.deals[*].id",    pbpath.Alias("deal_id")),
    pbpath.PlanPath("imp[0:3].banner.w",         pbpath.StrictPath()),
)
if err != nil {
    log.Fatal(err)
}

// Evaluate many times.
for _, msg := range messages {
    results, err := plan.Eval(msg)
    if err != nil {
        log.Fatal(err)
    }
    // results[0] = country  (1 branch)
    // results[1] = city     (1 branch)
    // results[2] = imp_id   (N branches, one per impression)
    // results[3] = deal_id  (N×M branches, impressions × deals)
    // results[4] = banner.w (up to 3 branches, strict)
}
```

`plan.Eval` returns `[][]Values` — one `[]Values` slot per path, in the order
the paths were provided to `NewPlan`. Each slot may contain multiple `Values`
branches when the path fans out via wildcards, ranges, or slices.

### NewPlan

```go
func NewPlan(md protoreflect.MessageDescriptor, paths ...PlanPathSpec) (*Plan, error)
```

Compiles path strings against `md`, builds a trie of shared prefixes, and
returns an immutable `Plan`. All parse errors are bundled into a single
returned error. The `Plan` is safe for concurrent use by multiple goroutines.

### PlanPath and PlanOption

```go
func PlanPath(path string, opts ...PlanOption) PlanPathSpec
```

Pairs a raw path string with per-path options. Available options:

| Option | Effect |
|---|---|
| `Alias(name)` | Give the entry a human-readable name (returned by `Plan.Entries`). Defaults to the raw path string. |
| `StrictPath()` | Return an error from `Eval` if any range or index on this path was clamped due to the list being shorter than the bound. |

### Plan.Eval

```go
func (p *Plan) Eval(m proto.Message) ([][]Values, error)
```

Traverses `m` along all compiled paths simultaneously. Paths sharing a prefix
are walked once through the shared segment, then forked. Returns
`[][]Values` indexed by entry position.

The `Eval` method always traverses **leniently** — out-of-bounds indices and
range bounds are clamped or skipped rather than returning errors. If a path was
compiled with `StrictPath()`, the clamped flag is checked at the leaf and an
error is returned only for that path.

### Plan.Entries

```go
func (p *Plan) Entries() []PlanEntry
```

Returns metadata for each compiled path:

```go
type PlanEntry struct {
    Name string   // alias or raw path string
    Path Path     // the compiled Path
}
```

Useful for mapping result slots to output column names.

```go
for i, e := range plan.Entries() {
    fmt.Printf("slot %d: name=%s  path=%s\n", i, e.Name, e.Path)
}
```

### PathValuesMulti (Convenience)

```go
func PathValuesMulti(
    md protoreflect.MessageDescriptor,
    m proto.Message,
    paths ...PlanPathSpec,
) ([][]Values, error)
```

One-shot wrapper that compiles a `Plan` and immediately evaluates it. Handy for
tests and one-off extractions. For repeated evaluation of the same paths against
many messages, prefer `NewPlan` + `Plan.Eval`.

```go
results, err := pbpath.PathValuesMulti(md, msg,
    pbpath.PlanPath("nested.stringfield", pbpath.Alias("greeting")),
    pbpath.PlanPath("repeats[*].nested.stringfield"),
)
```

### Trie-Based Shared-Prefix Optimization

Paths are inserted into a trie keyed by step equality. Two steps are merged
when they have the same kind and kind-specific parameters:

| Step kind | Equality criterion |
|---|---|
| `FieldAccessStep` | Same field number |
| `ListIndexStep` | Same index (including negatives) |
| `MapIndexStep` | Same key value |
| `ListWildcardStep` | Always equal |
| `ListRangeStep` | Same start, end, step, and omitted flags |
| `AnyExpandStep` | Same message full name |

Different step types on the same field (e.g. `imp[*]` vs `imp[0:3]`) **fork**
into separate trie branches — they produce independent fan-out groups.

## Path String Syntax

A path string is a dot-separated chain of **field names**, optionally prefixed
by an explicit **root** and suffixed with **index**, **map-key**, **wildcard**,
**range**, or **slice** accessors.

### Grammar

```
path         = [ root ] { accessor }
root         = "(" full_message_name ")"
accessor     = field_access | index_access
field_access = "." field_name
index_access = "[" key "]"
key          = integer | string_literal | "true" | "false"
             | "*"                           ← wildcard
             | [ start ] ":" [ end ]         ← range
             | [ start ] ":" [ end ] ":" [ step ]  ← slice
```

### Obtaining the Message Descriptor

`ParsePath` requires a `protoreflect.MessageDescriptor`. You can get one from:

- **Generated code** – `(*pb.MyMessage)(nil).ProtoReflect().Descriptor()`
- **Dynamic descriptors** – via `protodesc.NewFile` from a `FileDescriptorProto`
- **protocompile / buf** – parse `.proto` files at runtime

### Protobuf Editions

`pbpath` operates entirely on `protoreflect` descriptors, so it works with proto2, proto3, and [Protobuf Editions](https://protobuf.dev/editions/overview/) (Edition 2023+) without any changes. Editions features are resolved by the protobuf runtime before `pbpath` sees the descriptors.

### Identifying the Root Message

Every path implicitly starts at a **root message** – the outermost message type
whose descriptor you pass to `ParsePath`.

Given this `.proto` file:

```protobuf
// file: BidRequest.proto
package bidrequest;

message BidRequestEvent {
  string id = 1;
  DeviceEvent device = 3;
  repeated ImpressionEvent imp = 4;
  // ...
}
```

The root message is `BidRequestEvent`. You may write the root explicitly or
omit it:

| Path string | Equivalent explicit form |
|---|---|
| `id` | `(bidrequest.BidRequestEvent).id` |
| `device.ip` | `(bidrequest.BidRequestEvent).device.ip` |

The explicit `(package.MessageName)` form is optional and only required when
you want to be unambiguous in documentation or tooling.

### Field Access

Use the **text name** of the field (the `snake_case` name from the `.proto`
file), separated by dots:

```
field_name
parent.child
parent.child.grandchild
```

Field names correspond exactly to the names in your `.proto` definition.
For example, given:

```protobuf
message DeviceEvent {
  string ip = 3;
  GeoEvent geo = 4;

  message GeoEvent {
    string country = 4;
    string city = 6;
  }
}
```

| Goal | Path |
|---|---|
| Device IP | `device.ip` |
| Geo country | `device.geo.country` |
| Geo city | `device.geo.city` |

### Repeated Field (List) Indexing

Append `[index]` after a repeated field name, where `index` is an integer.
Zero-based, and **negative indices are allowed** (resolved at traversal time
relative to the list length: `-1` is the last element, `-2` is second-to-last,
etc.).

```
repeated_field[0]     // first element
repeated_field[-1]    // last element
repeated_field[-2]    // second-to-last element
```

Index literals may be decimal, octal (`0`-prefixed), or hex (`0x`-prefixed):

| Literal | Decimal value |
|---|---|
| `0` | 0 |
| `12` | 12 |
| `0x1F` | 31 |
| `010` | 8 (octal) |

### Map Field Indexing

Append `[key]` after a map field, where the key literal matches the map's
key type:

| Map key type | Syntax | Example |
|---|---|---|
| `string` | `["value"]` or `['value']` | `strkeymap["hello"]` |
| `bool` | `true` / `false` | `boolkeymap[true]` |
| `int32` / `int64` | signed integer | `int32keymap[-6]` |
| `uint32` / `uint64` | unsigned integer | `uint64keymap[0xffffffffffffffff]` |

String keys support the same escape sequences as protobuf text format
(`\n`, `\t`, `\"`, `\\`, hex `\xHH`, unicode `\uHHHH` / `\UHHHHHHHH`, and
octal `\ooo`).

After indexing a map you can continue traversing into the value type:

```
strkeymap["mykey"].stringfield
```

### Wildcards, Ranges, and Slices

These step types cause `PathValues` to **fan out**, producing one result per
matching element. They may only be applied to repeated (list) fields; using
them on a map field is a parse error.

#### Wildcard — `[*]` or `[:]` or `[::]`

Selects **every** element in the list:

```
repeats[*]              // all elements
repeats[:]              // same (normalizes to [*])
repeats[::]             // same (normalizes to [*])
```

#### Range — `[start:end]`

Selects a half-open range of elements `[start, end)` with stride 1.
Both start and end may be negative:

```
repeats[0:3]            // elements 0, 1, 2
repeats[1:3]            // elements 1, 2
repeats[-3:-1]          // 3rd-to-last through 2nd-to-last
repeats[2:]             // element 2 through the end
repeats[:2]             // elements 0, 1
```

#### Slice — `[start:end:step]` (Python semantics)

Full Python-style slice with an explicit stride/step. Any of `start`, `end`,
and `step` may be omitted — omitted bounds default based on the step sign:

| When step > 0 | Default start | Default end |
|---|---|---|
| omitted start | `0` (beginning) | — |
| omitted end | — | `len` (past the end) |

| When step < 0 | Default start | Default end |
|---|---|---|
| omitted start | `len - 1` (last) | — |
| omitted end | — | before index 0 |

```
repeats[::2]            // every other element: 0, 2, 4, …
repeats[1::2]           // odd-indexed: 1, 3, 5, …
repeats[::-1]           // all elements in reverse
repeats[3:0:-1]         // elements 3, 2, 1 (reverse, half-open)
repeats[-1::-1]         // reverse from last element
repeats[0:10:3]         // elements 0, 3, 6, 9
repeats[:3:2]           // elements 0, 2
```

A step of `0` is always a parse error (`[::0]`).

### Chaining Steps

Steps can be freely chained. After a field access you can index, after an
index you can access a field, and so on:

```
field.subfield[0].deeper_field["key"].leaf
repeats[*].nested.stringfield
repeats[::2].nested.int32repeats[0]
```

## Fan-Out and Nested Fan-Outs

When a path contains one or more wildcard, range, or slice steps, `PathValues`
fans out and returns **multiple `Values`** — one per matching list element.
When *multiple* fan-out steps appear in a single path the result is the
**cartesian product** of all expansions.

### Single Fan-Out

Given a message with `repeats = [A, B, C]`:

```go
path, _ := pbpath.ParsePath(md, "repeats[*].nested.stringfield")
results, _ := pbpath.PathValues(path, msg)

// results has 3 entries:
// [0] → (pkg.Test).repeats[0].nested.stringfield = "alpha"
// [1] → (pkg.Test).repeats[1].nested.stringfield = "beta"
// [2] → (pkg.Test).repeats[2].nested.stringfield = "gamma"
```

Each result's `Path` contains the **concrete** `ListIndex` (not the wildcard),
so you always know exactly which element produced the value.

### Nested Fan-Out (Cartesian Product)

Consider a schema where a repeated field contains another repeated field:

```protobuf
message Outer {
  repeated Middle items = 1;
}
message Middle {
  repeated Inner sub = 1;
}
message Inner {
  string value = 1;
}
```

With `items` containing two `Middle` messages, each with three `Inner`
messages:

```go
path, _ := pbpath.ParsePath(md, "items[*].sub[*].value")
results, _ := pbpath.PathValues(path, msg)
```

This produces **2 × 3 = 6** results — every combination of outer and inner
index:

```
items[0].sub[0].value = "a"
items[0].sub[1].value = "b"
items[0].sub[2].value = "c"
items[1].sub[0].value = "d"
items[1].sub[1].value = "e"
items[1].sub[2].value = "f"
```

You can use `Values.ListIndices()` to recover the indices for each level:

```go
for _, r := range results {
    indices := r.ListIndices()
    // indices[0] = items index, indices[1] = sub index
    fmt.Printf("items[%d].sub[%d] = %s\n",
        indices[0], indices[1], r.Index(-1).Value.String())
}
```

### Mixed Fan-Out: Range × Wildcard

Fan-out steps don't all have to be the same kind. You can mix ranges, slices,
and wildcards:

```go
// First two items, all their sub-items in reverse
path, _ := pbpath.ParsePath(md, "items[0:2].sub[::-1].value")
results, _ := pbpath.PathValues(path, msg)
```

If `items[0]` has 3 subs and `items[1]` has 2 subs, this produces 5 results:

```
items[0].sub[2].value   ← reversed
items[0].sub[1].value
items[0].sub[0].value
items[1].sub[1].value   ← reversed
items[1].sub[0].value
```

## Step Constructors (Programmatic API)

Paths can also be built programmatically instead of parsing a string:

```go
p := pbpath.Path{
    pbpath.Root(md),
    pbpath.FieldAccess(repeatsFD),
    pbpath.ListWildcard(),
    pbpath.FieldAccess(nestedFD),
    pbpath.FieldAccess(stringfieldFD),
}
results, err := pbpath.PathValues(p, msg)
```

### Available Constructors

| Constructor | Produces | Path Syntax |
|---|---|---|
| `Root(md)` | `RootStep` | `(full.Name)` |
| `FieldAccess(fd)` | `FieldAccessStep` | `.field` |
| `ListIndex(i)` | `ListIndexStep` | `[i]` (negative OK) |
| `MapIndex(k)` | `MapIndexStep` | `[key]` |
| `AnyExpand(md)` | `AnyExpandStep` | `.(full.Name)` |
| `ListWildcard()` | `ListWildcardStep` | `[*]` |
| `ListRange(start, end)` | `ListRangeStep` | `[start:end]` |
| `ListRangeFrom(start)` | `ListRangeStep` | `[start:]` |
| `ListRangeStep3(start, end, step, startOmitted, endOmitted)` | `ListRangeStep` | `[start:end:step]` |

`ListRangeStep3` panics if `step` is 0. Use the `startOmitted`/`endOmitted`
flags to indicate that a bound should be defaulted at traversal time (matching
the behaviour of omitting them in the string syntax).

## Examples

### Simple Scalar

```protobuf
message BidRequestEvent {
  string id = 1;
  bool throttled = 10;
}
```

```
id              → string value of the id field
throttled       → bool value of the throttled field
```

### Nested Messages

```protobuf
message BidRequestEvent {
  DeviceEvent device = 3;
  message DeviceEvent {
    GeoEvent geo = 4;
    DeviceExtEvent ext = 7;
    message GeoEvent { string country = 4; }
    message DeviceExtEvent {
      DoohEvent dooh = 1;
      message DoohEvent { uint32 venuetypeid = 2; }
    }
  }
}
```

```
device.geo.country          → the country string
device.ext.dooh.venuetypeid → uint32 venue type id
```

### Repeated Message Elements

```protobuf
message BidRequestEvent {
  repeated ImpressionEvent imp = 4;
  message ImpressionEvent {
    string id = 1;
    BannerEvent banner = 4;
    message BannerEvent { uint32 w = 2; }
  }
}
```

```
imp[0].id        → id of the first impression
imp[0].banner.w  → banner width of the first impression
imp[-1].id       → id of the last impression
```

### Repeated Scalars

```protobuf
message BidRequestEvent {
  repeated string cur = 6;
}
```

```
cur[0]   → first currency string
cur[-1]  → last currency string
```

### Wildcard over Repeated Messages

```go
path, _ := pbpath.ParsePath(md, "imp[*].id")
results, _ := pbpath.PathValues(path, msg)
// One Values per impression, each ending with that impression's id.
for _, r := range results {
    fmt.Println(r.Index(-1).Value.String())
}
```

### Range: First N Impressions

```go
path, _ := pbpath.ParsePath(md, "imp[0:3].banner.w")
results, _ := pbpath.PathValues(path, msg)
// Up to 3 results (or fewer if imp has < 3 elements).
```

### Slice: Every Other Element in Reverse

```go
path, _ := pbpath.ParsePath(md, "imp[::-2]")
results, _ := pbpath.PathValues(path, msg)
// With 5 impressions → indices 4, 2, 0.
```

### Nested Fan-Out with ListIndices

```go
// All deals across all impressions.
path, _ := pbpath.ParsePath(md, "imp[*].pmp.deals[*].id")
results, _ := pbpath.PathValues(path, msg)

for _, r := range results {
    idx := r.ListIndices() // [imp_index, deal_index]
    fmt.Printf("imp[%d].deal[%d] = %s\n",
        idx[0], idx[1], r.Index(-1).Value.String())
}
```

### Deeply Nested Through Repeated Fields

```protobuf
message ImpressionEvent {
  PrivateMarketplaceEvent pmp = 3;
  message PrivateMarketplaceEvent {
    repeated DealEvent deals = 2;
    message DealEvent {
      string id = 1;
      DealExtEvent ext = 6;
      message DealExtEvent { bool must_bid = 3; }
    }
  }
}
```

```
imp[0].pmp.deals[0].id            → deal id
imp[0].pmp.deals[0].ext.must_bid  → must_bid flag on the deal
imp[*].pmp.deals[*].ext.must_bid  → must_bid for every deal in every impression
```

### Map Access

```protobuf
message Test {
  map<string, Nested> strkeymap = 4;
  map<int32, Test>    int32keymap = 6;
  message Nested { string stringfield = 2; }
}
```

```
strkeymap["mykey"]              → the Nested message for key "mykey"
strkeymap["mykey"].stringfield  → stringfield inside that Nested message
int32keymap[-6]                 → the Test message for key -6
```

### Self-Referential / Recursive Messages

```protobuf
message Test {
  Nested nested = 1;
  message Nested {
    string stringfield = 2;
    Test nested = 4;        // back-reference to Test
  }
}
```

```
nested.stringfield                  → top-level Nested's stringfield
nested.nested.nested.stringfield    → 3 levels deep through the cycle
```

### Complex: Multiple Step Types Combined

Using the `testmessage.proto` schema with octal and hex literals:

```
int32keymap[-6].uint64keymap[040000000000].repeats[0].nested.nested.strkeymap["k"].intfield
```

This path:
1. Indexes `int32keymap` with key `-6`
2. On the resulting `Test`, indexes `uint64keymap` with key `4294967296` (octal `040000000000`)
3. Indexes `repeats` list at position `0`
4. Accesses `nested` (a `Nested` message)
5. Accesses `nested` on that `Nested` (back to a `Test`)
6. Indexes `strkeymap` with string key `"k"`
7. Reads the `intfield` scalar

### Explicit Root

When you want to be explicit about the message type:

```
(bidrequest.BidRequestEvent).imp[0].pmp.deals[0].id
(pbpath.testdata.Test).nested.stringfield
```

The fully-qualified name inside `()` must exactly match the message
descriptor's `FullName()`.

## Strict Mode

By default, out-of-bounds indices and range bounds are silently handled:

- A negative index that resolves past the beginning of a list **skips** that
  branch (no result is emitted for it).
- Range/slice bounds are **clamped** to the list length.

### PathValues — global strict

Pass `Strict()` to make any out-of-bounds condition an immediate error:

```go
results, err := pbpath.PathValues(path, msg, pbpath.Strict())
// err is non-nil if any index or bound is out of range.
```

### Plan — per-path strict

With the Plan API, strict checking is **per-path** via `StrictPath()`. The
traversal itself is always lenient (clamp/skip); the clamped flag is checked at
leaf nodes only for strict paths.

```go
plan, _ := pbpath.NewPlan(md,
    pbpath.PlanPath("imp[0:100].id", pbpath.StrictPath()), // errors if clamped
    pbpath.PlanPath("imp[0:100].banner.w"),                    // silently clamped
)
results, err := plan.Eval(msg)
// err is non-nil only if the strict path's bounds were clamped.
```

This lets you mix lenient and strict paths in the same plan without separate
traversals.

## Expression Engine

The Plan API supports **computed columns** through a composable `Expr` tree.
Expressions reference protobuf field paths via `PathRef` and apply functions to
produce derived values — all evaluated inline during plan traversal.

### Quick Start

```go
plan, err := pbpath.NewPlan(md, nil,
    // Coalesce: first non-zero value from multiple paths
    pbpath.PlanPath("device_id",
        pbpath.WithExpr(pbpath.FuncCoalesce(
            pbpath.PathRef("user.id"),
            pbpath.PathRef("site.id"),
            pbpath.PathRef("device.ifa"),
        )),
        pbpath.Alias("device_id"),
    ),

    // Conditional: use banner dimensions if present, else video
    pbpath.PlanPath("width",
        pbpath.WithExpr(pbpath.FuncCond(
            pbpath.FuncHas(pbpath.PathRef("imp[0].banner.w")),
            pbpath.PathRef("imp[0].banner.w"),
            pbpath.PathRef("imp[0].video.w"),
        )),
        pbpath.Alias("width"),
    ),

    // Arithmetic: compute a derived value
    pbpath.PlanPath("total",
        pbpath.WithExpr(pbpath.FuncMul(
            pbpath.PathRef("items[0].price"),
            pbpath.PathRef("items[0].qty"),
        )),
        pbpath.Alias("total"),
    ),

    // Default: provide a fallback literal
    pbpath.PlanPath("country",
        pbpath.WithExpr(pbpath.FuncDefault(
            pbpath.PathRef("device.geo.country"),
            protoreflect.ValueOfString("UNKNOWN"),
        )),
        pbpath.Alias("country"),
    ),
)
```

### Available Functions

| Category | Functions | Output kind |
|---|---|---|
| Control flow | `FuncCoalesce`, `FuncDefault`, `FuncCond` | Same as input |
| Existence | `FuncHas` | Bool |
| Length | `FuncLen` | Int64 |
| Predicates | `FuncEq`, `FuncNe`, `FuncLt`, `FuncLe`, `FuncGt`, `FuncGe` | Bool |
| Arithmetic | `FuncAdd`, `FuncSub`, `FuncMul`, `FuncDiv`, `FuncMod` | Numeric (auto-promoted) |
| Math | `FuncAbs`, `FuncCeil`, `FuncFloor`, `FuncRound`, `FuncMin`, `FuncMax` | Preserved |
| String | `FuncUpper`, `FuncLower`, `FuncTrim`, `FuncTrimPrefix`, `FuncTrimSuffix`, `FuncConcat` | String |
| Cast | `FuncCastInt`, `FuncCastFloat`, `FuncCastString` | Changed |
| Timestamp | `FuncStrptime`, `FuncTryStrptime`, `FuncAge`, `FuncExtract{Year,Month,Day,Hour,Minute,Second}` | Int64 |
| ETL | `FuncHash`, `FuncEpochToDate`, `FuncDatePart`, `FuncBucket`, `FuncMask`, `FuncCoerce`, `FuncEnumName` | Varies |
| Aggregates | `FuncSum`, `FuncDistinct`, `FuncListConcat` | Varies |

Expressions compose freely — a `FuncCond` can contain `FuncHas` as its
predicate, `PathRef` as the then-branch, and `FuncDefault` as the else-branch.
See the [Architecture Guide](ARCHITECTURE.md#expression-system-expr) for
implementation details.

## EvalLeaves — High-Performance Evaluation

`Plan.EvalLeaves` is the recommended method for hot paths. It returns only
leaf values (not the full path/values chain) and reuses pre-allocated scratch
buffers, giving near-zero per-call allocations.

```go
// Compile once.
plan, _ := pbpath.NewPlan(md, nil,
    pbpath.PlanPath("device.geo.country", pbpath.Alias("country")),
    pbpath.PlanPath("imp[*].id",          pbpath.Alias("imp_id")),
)

// Evaluate per message — near-zero allocations.
for _, msg := range stream {
    leaves, _ := plan.EvalLeaves(msg)
    country := leaves[0]  // []protoreflect.Value with 1 element
    impIDs  := leaves[1]  // []protoreflect.Value with N elements
}
```

| Method | Allocations | Thread-safe | Returns |
|---|---|---|---|
| `Eval(msg)` | Full Values chains | ✅ | `[][]Values` |
| `EvalLeaves(msg)` | Near-zero (scratch reuse) | ❌ | `[][]protoreflect.Value` |
| `EvalLeavesConcurrent(msg)` | Fresh buffers per call | ✅ | `[][]protoreflect.Value` |

## Running Benchmarks

```sh
# Run all pbpath benchmarks
go test -bench=. -benchmem ./proto/pbpath/

# Run Plan evaluation benchmarks
go test -bench='BenchmarkPlan' -benchmem ./proto/pbpath/

# Run with longer duration for stable results
go test -bench='BenchmarkPlan' -benchmem -benchtime=5s -count=3 ./proto/pbpath/
```

## Error Cases

| Input | Error |
|---|---|
| `unknown` | field not found in message descriptor |
| `int32keymap["foo"]` | string key for int32 map |
| `nested.stringfield[0]` | indexing a non-repeated field |
| `strkeymap.key` | traversing map internal fields |
| `strkeymap["k"]["k2"]` | double-indexing (value is not a map) |
| `strkeymap[*]` | wildcard not supported on map fields |
| `strkeymap[0:3]` | range/slice not supported on map fields |
| `repeats[::0]` | step must not be zero |
| `(wrong.Name).id` | root name doesn't match descriptor |
| `nested.` | trailing dot with no field name |
| `nested 🎉` | illegal characters |

## Slice Quick-Reference

| Syntax | Selects | Python equivalent |
|---|---|---|
| `[*]` | all elements | `[:]` |
| `[:]` | all elements | `[:]` |
| `[::]` | all elements | `[::]` |
| `[0:3]` | elements 0, 1, 2 | `[0:3]` |
| `[2:]` | from index 2 to end | `[2:]` |
| `[:2]` | elements 0, 1 | `[:2]` |
| `[-2:]` | last 2 elements | `[-2:]` |
| `[-3:-1]` | 3rd-to-last through 2nd-to-last | `[-3:-1]` |
| `[::2]` | every other (0, 2, 4, …) | `[::2]` |
| `[1::2]` | odd-indexed (1, 3, 5, …) | `[1::2]` |
| `[::-1]` | all in reverse | `[::-1]` |
| `[3:0:-1]` | 3, 2, 1 | `[3:0:-1]` |
| `[0:10:3]` | 0, 3, 6, 9 | `[0:10:3]` |
| `[5:2]` | empty (start ≥ end, step=1) | `[5:2]` |
| `[::0]` | **error** — step must not be 0 | `[::0]` → `ValueError` |
