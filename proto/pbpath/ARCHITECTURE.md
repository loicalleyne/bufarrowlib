# pbpath — Architecture Guide

This document describes the internal architecture of the `pbpath` package, how
its objects relate to each other, and recommendations for building efficient
protobuf-to-analytics pipelines.

## Object model overview

```
                 ┌──────────────────────┐
                 │  ParsePath(md, str)  │
                 └──────────┬───────────┘
                            │ returns
                            ▼
                    ┌───────────────┐
                    │     Path      │ ← slice of Step values
                    │ []Step        │
                    └───────┬───────┘
                            │ used by
                ┌───────────┴─────────────┐
                │                         │
                ▼                         ▼
     ┌────────────────┐        ┌───────────────────┐
     │  PathValues()  │        │  NewPlan(md, ...) │
     │  (one-shot)    │        │  (compile once)   │
     └───────┬────────┘        └────────┬──────────┘
             │ returns                  │ produces
             ▼                          ▼
     ┌──────────────┐          ┌───────────────────┐
     │  []Values    │          │     Plan          │ ← immutable trie
     └──────────────┘          │  ┌─────────────┐  │
                               │  │  planNode   │──┼── children → planNode ...
                               │  │  (trie root)│  │
                               │  └─────────────┘  │
                               │  entries[]        │ ← one per path
                               │  scratch          │ ← reused by EvalLeaves
                               └────────┬──────────┘
                                        │ evaluate
                          ┌─────────────┼───────────────┐
                          ▼             ▼               ▼
                    ┌──────────┐  ┌───────────┐  ┌──────────────────┐
                    │  Eval()  │  │ EvalLeaves│  │EvalLeavesConcur. │
                    │ [][]Val  │  │ [][]prVal │  │  [][]prVal       │
                    └──────────┘  └───────────┘  └──────────────────┘
```

## Core types

### Path

A `Path` is a slice of `Step` values representing a chain of protobuf
reflection operations from a root message to a leaf value. Paths are the
fundamental unit of addressing in pbpath.

```go
type Path []Step
```

Paths are created by:
- **`ParsePath(md, string)`** — parse a human-readable string like `"imp[*].banner.w"` against a message descriptor.
- **Programmatic construction** — build a `Path` literal using step constructors (`Root`, `FieldAccess`, `ListIndex`, `ListWildcard`, etc.).

A `Path` is immutable after creation and safe to share across goroutines.

### Step

A `Step` is a single operation within a `Path`. It wraps `protopath.Step`
for standard kinds and adds fan-out step kinds.

| Kind | Constructor | String syntax | Behaviour |
|---|---|---|---|
| `RootStep` | `Root(md)` | `(pkg.Msg)` | Identifies the root message type |
| `FieldAccessStep` | `FieldAccess(fd)` | `.field` | Navigate into a message field |
| `ListIndexStep` | `ListIndex(i)` | `[i]` | Select one element (negative OK: -1 = last) |
| `MapIndexStep` | `MapIndex(k)` | `["key"]` | Index into a map by key |
| `AnyExpandStep` | `AnyExpand(md)` | `.(pkg.Msg)` | Expand a `google.protobuf.Any` |
| `ListWildcardStep` | `ListWildcard()` | `[*]` | Fan-out: select all elements |
| `ListRangeStep` | `ListRange(s,e)` | `[s:e]`, `[s:e:step]` | Fan-out: Python-style slice |

**Key design choice:** Fan-out steps (`ListWildcardStep`, `ListRangeStep`)
produce multiple result branches during traversal. This is what enables the
"UNNEST" / "CROSS JOIN" behaviour in denormalization without any explicit
loop in user code.

### Values

`Values` is the result of traversing a `Path` through a live message. When
the path contains fan-out steps, `PathValues` returns one `Values` per
matching branch.

```go
type Values struct {
    Path   Path                    // concrete path taken (wildcards resolved to indices)
    Values []protoreflect.Value    // value at each step along the path
}
```

- `Index(i)` returns the (step, value) pair at position `i` (negative indexing supported).
- `ListIndices()` extracts the concrete list indices visited along the path.
- `String()` returns a human-readable representation.

### Plan

`Plan` is an immutable, pre-compiled bundle of paths ready for repeated
evaluation. It is the performance-critical object for hot-path usage.

**Construction:** `NewPlan(md, opts, paths...)` compiles path strings, builds
an internal trie, and validates all types.

**Evaluation methods:**

| Method | Allocations | Thread-safe | Returns | Best for |
|---|---|---|---|---|
| `Eval(msg)` | Full Values chains | ✅ | `[][]Values` | Debugging, inspection, one-off use |
| `EvalLeaves(msg)` | Near-zero (reuses scratch) | ❌ | `[][]protoreflect.Value` | Hot path, single goroutine |
| `EvalLeavesConcurrent(msg)` | Fresh buffers per call | ✅ | `[][]protoreflect.Value` | Hot path, multiple goroutines |

**EvalLeaves is the preferred method for production** — it returns only the
leaf values (not the full path chain at each step) and reuses pre-allocated
scratch buffers, eliminating per-call allocations.

### PlanEntry

Metadata about one compiled path inside a Plan. Accessible via `Plan.Entries()`.

```go
type PlanEntry struct {
    Name       string             // alias or raw path string
    Path       Path               // the compiled Path (nil for Expr-only entries)
    OutputKind protoreflect.Kind  // non-zero when an Expr changes the output type
    HasExpr    bool               // true when WithExpr was used
}
```

## The trie — shared-prefix optimisation

Paths compiled into a Plan are stored in a trie (prefix tree) keyed by step
equality. Two paths that share a common prefix traverse the shared segment
only once during evaluation.

### Example

Given these paths:
```
imp[*].banner.w
imp[*].banner.h
imp[*].video.w
imp[*].pmp.deals[*].id
```

The trie looks like:
```
(root)
  └─ imp
       └─ [*]                          ← shared: traversed once for all 4 paths
            ├─ banner
            │    ├─ w    [leaf: path 0]
            │    └─ h    [leaf: path 1]
            ├─ video
            │    └─ w    [leaf: path 2]
            └─ pmp
                 └─ deals
                      └─ [*]           ← nested fan-out
                           └─ id [leaf: path 3]
```

The `imp[*]` wildcard is traversed **once**, producing N branches (one per
impression). Each branch then forks into the banner, video, and pmp subtrees.
Without the trie, evaluating 4 paths would require 4 separate traversals of
`imp[*]`.

### Step merging rules

| Step kind | Merged when |
|---|---|
| `FieldAccessStep` | Same field number |
| `ListIndexStep` | Same index (including negatives) |
| `MapIndexStep` | Same key value |
| `ListWildcardStep` | Always (all wildcards are equal) |
| `ListRangeStep` | Same start, end, step, and omitted flags |
| `AnyExpandStep` | Same message full name |

Different step kinds on the same field (e.g. `imp[*]` and `imp[0:3]`) create
separate trie branches — they produce independent fan-out groups.

## Expression system (Expr)

Expressions are composable trees that produce computed values from resolved
path leaves. They are attached to plan entries via `WithExpr` and evaluated
inline during `EvalLeaves` — no extra pass over the data.

### Architecture

```
                PlanPath("device_id",
                    WithExpr(FuncCoalesce(
                        PathRef("user.id"),       ← leaf
                        PathRef("site.id"),       ← leaf
                        PathRef("device.ifa"),    ← leaf
                    )),
                    Alias("device_id"),
                )
```

At `NewPlan` time:
1. Leaf `PathRef` nodes are collected from the Expr tree.
2. Each leaf path is parsed and inserted into the trie (deduplicating with
   existing paths or other Exprs' leaves).
3. An `entryIdx` is assigned to each leaf, pointing into the Plan's internal
   entries slice.
4. The Expr tree is stored on the plan entry.

At `EvalLeaves` time:
1. The trie is traversed, populating leaf values in `scratch.out[entryIdx]`.
2. Post-traversal, `evalExprs` walks each user entry that has an Expr and
   evaluates the expression tree against the resolved leaf values.
3. The Expr result replaces the raw leaf values in the output slot.

### Expression categories

| Category | Functions | Output kind |
|---|---|---|
| **Control flow** | `FuncCoalesce`, `FuncDefault`, `FuncCond` | Same as input (or promoted) |
| **Existence** | `FuncHas` → Bool, `FuncLen` → Int64 | Changed |
| **Predicates** | `FuncEq`, `FuncNe`, `FuncLt`, `FuncLe`, `FuncGt`, `FuncGe` | Bool |
| **Arithmetic** | `FuncAdd`, `FuncSub`, `FuncMul`, `FuncDiv`, `FuncMod` | Int or Float (auto-promoted) |
| **Math** | `FuncAbs`, `FuncCeil`, `FuncFloor`, `FuncRound`, `FuncMin`, `FuncMax` | Preserved |
| **String** | `FuncUpper`, `FuncLower`, `FuncTrim`, `FuncTrimPrefix`, `FuncTrimSuffix`, `FuncConcat` | String |
| **Cast** | `FuncCastInt`, `FuncCastFloat`, `FuncCastString` | Changed |
| **Timestamp** | `FuncStrptime`, `FuncTryStrptime`, `FuncAge`, `FuncExtract*` | Int64 |
| **ETL** | `FuncHash`, `FuncEpochToDate`, `FuncDatePart`, `FuncBucket`, `FuncMask`, `FuncCoerce`, `FuncEnumName` | Varies |
| **Aggregates** | `FuncSum`, `FuncDistinct`, `FuncListConcat` | Varies |

### Output kind override

When an Expr changes the protobuf kind of the result (e.g. `FuncLen` turns
a string into Int64, `FuncHas` turns anything into Bool), the `OutputKind`
field on `PlanEntry` is set. The bufarrowlib denormalizer uses this to
select the correct Arrow builder type for the column.

### Composability

Expressions compose freely. A `FuncCond` can contain `FuncHas` as its
predicate, `PathRef` as the then-branch, and `FuncDefault` as the else-branch:

```go
// If banner exists, use banner.w; else default to 0.
FuncCond(
    FuncHas(PathRef("imp[0].banner")),       // predicate
    PathRef("imp[0].banner.w"),               // then
    FuncDefault(PathRef("imp[0].video.w"),    // else
        protoreflect.ValueOfUint32(0)),
)
```

## Fan-out and cross-join model

When a path contains one or more wildcard/range steps, evaluation produces
**multiple branches**. The bufarrowlib denormalizer groups columns by their
**fan-out signature** — the sequence of wildcard/range positions and parent
field names — and cross-joins independent groups.

### Fan-out signature

Two paths have the same fan-out signature when they traverse the same
wildcard/range steps at the same positions:

```
imp[*].banner.w   → signature: "W3:imp"
imp[*].banner.h   → signature: "W3:imp"      ← same group
imp[*].video.w    → signature: "W3:imp"      ← same group

tags[*]           → signature: "W1:tags"     ← different group
```

Paths in the same group produce the same number of branches for any given
message and advance in lockstep during row iteration. Different groups are
cross-joined.

### Nested fan-out (cartesian product)

When a path traverses two wildcard steps (e.g. `imp[*].pmp.deals[*].id`),
the result is the cartesian product of both levels:

```
2 impressions × 3 deals each = 6 branches
```

Each branch's `Values.ListIndices()` returns `[imp_index, deal_index]`.

### Left-join semantics

When a fan-out group produces zero branches (e.g. an impression has no deals),
the denormalizer emits a single row with null values for that group's columns.
This ensures outer-group rows are never silently dropped — matching SQL
`LEFT JOIN UNNEST(...)` behaviour.

## Recommendations for use

### 1. Prefer Plan + EvalLeaves over PathValues

`PathValues` is the simplest API but allocates a full `Values` chain for every
branch at every step. For hot paths (thousands of messages per second), use the
Plan API with `EvalLeaves`:

```go
// Compile once at startup
plan, _ := pbpath.NewPlan(md, nil,
    pbpath.PlanPath("device.geo.country", pbpath.Alias("country")),
    pbpath.PlanPath("imp[*].id",          pbpath.Alias("imp_id")),
)

// Evaluate per message — near-zero allocations
for _, msg := range stream {
    leaves, _ := plan.EvalLeaves(msg)
    country := leaves[0]  // []protoreflect.Value with 1 element
    impIDs  := leaves[1]  // []protoreflect.Value with N elements
}
```

### 2. Use aliases for output column mapping

Aliases provide stable, human-readable names for mapping plan results to output
columns. Without aliases, the raw path string is used as the name.

```go
plan, _ := pbpath.NewPlan(md, nil,
    pbpath.PlanPath("device.geo.country", pbpath.Alias("country")),
)

for i, entry := range plan.Entries() {
    fmt.Println(entry.Name) // "country" — use as Arrow column name
}
```

### 3. Group related paths to maximize trie sharing

Paths that share common prefixes benefit from trie merging. Order your paths
to maximize prefix overlap:

```go
// Good: imp[*] traversed once, forked into banner/video/pmp
plan, _ := pbpath.NewPlan(md, nil,
    pbpath.PlanPath("imp[*].banner.w"),
    pbpath.PlanPath("imp[*].banner.h"),
    pbpath.PlanPath("imp[*].video.w"),
    pbpath.PlanPath("imp[*].pmp.deals[*].id"),
)
```

### 4. Use fixed indices for scalar broadcast, wildcards for fan-out

- `imp[0].banner.w` — selects a single value (broadcast as a scalar column).
- `imp[*].banner.w` — fans out, producing one row per impression.

Choose based on your analytics needs. Fixed indices are cheaper but lose
the per-element detail.

### 5. Use Exprs for computed columns

Rather than post-processing plan results in user code, use Exprs to compute
derived values inline. This avoids an extra pass over the data and keeps the
logic declarative:

```go
// Instead of this (manual post-processing):
leaves, _ := plan.EvalLeaves(msg)
userID := leaves[0]
siteID := leaves[1]
deviceIfa := leaves[2]
deviceID := coalesce(userID, siteID, deviceIfa) // custom function

// Do this (Expr-based):
plan, _ := pbpath.NewPlan(md, nil,
    pbpath.PlanPath("device_id",
        pbpath.WithExpr(pbpath.FuncCoalesce(
            pbpath.PathRef("user.id"),
            pbpath.PathRef("site.id"),
            pbpath.PathRef("device.ifa"),
        )),
        pbpath.Alias("device_id"),
    ),
)
leaves, _ := plan.EvalLeaves(msg)
deviceID := leaves[0] // already coalesced
```

### 6. Use EvalLeavesConcurrent for multi-goroutine access

`EvalLeaves` reuses internal scratch buffers and is **not** safe for concurrent
use. If multiple goroutines share the same Plan, use `EvalLeavesConcurrent`
(which allocates fresh buffers per call) or give each goroutine its own Plan
clone.

In the bufarrowlib integration, this is handled automatically: `Transcoder.Clone`
shares the immutable Plan but each clone calls `EvalLeaves` on its own
goroutine without contention.

### 7. Use StrictPath selectively

By default, out-of-bounds indices and range bounds are silently clamped or
skipped. Use `StrictPath()` on individual plan entries when you need to detect
data quality issues:

```go
plan, _ := pbpath.NewPlan(md, nil,
    // This path is critical — error if clamped
    pbpath.PlanPath("imp[0].id", pbpath.StrictPath()),

    // This path is best-effort — silently skip missing elements
    pbpath.PlanPath("imp[0:100].banner.w"),
)
```

### 8. Understand proto3 zero-value semantics

In proto3, unset scalar fields return their default value (0 for integers,
"" for strings, false for bools). pbpath's traversal follows this convention:

- `FuncHas(PathRef("field"))` returns true only for **non-zero** values.
- `FuncCoalesce` skips zero values, treating them as "null".
- `FuncDefault(PathRef("field"), literal)` returns the literal when the field is zero.

For message fields, an unset field produces an empty message (all fields at
default). If you need to distinguish "field not set" from "field set to
defaults", use `FuncHas` as a guard.

## Performance characteristics

| Operation | Cost | Notes |
|---|---|---|
| `ParsePath` | O(path length) | One-time string parsing; allocates Step slice |
| `NewPlan` | O(sum of path lengths) | One-time trie construction |
| `PathValues` | O(depth × branches) | Allocates full Values chains — use for debugging |
| `EvalLeaves` | O(depth × branches) | Near-zero allocs — reuses scratch buffers |
| `EvalLeavesConcurrent` | O(depth × branches) | Fresh allocs per call — slightly more expensive |
| Expr evaluation | O(tree depth) | Inline, no extra pass; switch-dispatched (no interface calls) |
| Trie merging | O(1) per shared step | Saves work proportional to shared prefix length × branches |

## Integration with bufarrowlib

The denormalizer in bufarrowlib uses pbpath's Plan API as its evaluation engine:

1. `WithDenormalizerPlan(paths...)` compiles a `pbpath.Plan` at `Transcoder` construction time.
2. Each `AppendDenorm(msg)` call invokes `plan.EvalLeaves(msg)` to extract leaf values.
3. Columns are grouped by fan-out signature; groups are cross-joined to produce flat rows.
4. Leaf values are appended to Arrow builders via pre-compiled append closures.
5. `NewDenormalizerRecordBatch()` flushes the builders into an Arrow record.

When using `AppendDenormRaw([]byte)`:
1. Raw bytes are decoded by hyperpb into a `hyperpb.Message` (implements `protoreflect.Message`).
2. The message is passed to `plan.EvalLeaves` — pbpath operates through the `protoreflect.Message` interface, so it works identically with `dynamicpb`, `hyperpb`, or generated types.
3. Memory is reclaimed via `hyperpb.Shared.Free()` after evaluation.
