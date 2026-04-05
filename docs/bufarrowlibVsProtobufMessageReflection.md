## bufarrowlib vs. `ProtobufMessageReflection`

### What `ProtobufMessageReflection` does

It's a **single-message utility** in `arrow/go/v17/arrow/util`:

- Takes a `proto.Message` (a decoded Go struct)
- Derives an `*arrow.Schema` via reflection
- Produces a single `arrow.Record` via `Record(mem)`
- Offers a few options: field exclusion, field name formatting, enum/oneOf type handlers

That's it. It's a **convenience wrapper** for one-shot, per-message conversion.

---

### Why you'd use bufarrowlib instead

**1. It operates on raw wire bytes, skipping deserialization entirely**

`ProtobufMessageReflection` requires an already-decoded `proto.Message`. bufarrowlib's `AppendDenormRaw` / `AppendRaw` take the raw `[]byte` off the wire and reads directly into Arrow builders — no intermediate Go struct, no `proto.Unmarshal`. This alone is **2.9–4.7× faster** on the hot path when combined with `HyperType`.

**2. Batch-oriented: N messages → one RecordBatch**

`ProtobufMessageReflection.Record()` returns one record per call. For columnar analytics you need batches. bufarrowlib accumulates messages across calls and flushes with `NewRecordBatch()` / `NewDenormalizerRecordBatch()`. At batch size ≥ 100 the per-message allocation cost drops from ~2,446 to ~26 allocs/msg.

**3. Denormalization and fan-out**

`ProtobufMessageReflection` mirrors the proto structure as-is. bufarrowlib can **flatten nested repeated fields** into a wider table in a single pass — e.g., `imp[*].bidfloor` across all impressions in a BidRequest fans out into individual rows. This is its primary use case for ad-tech / event ETL pipelines.

**4. Expression engine for computed columns**

`cond`, `coalesce`, `hash`, `bucket`, `cast_*`, `strptime`, enum name lookup, 30+ functions — evaluated inline during traversal, no second pass. `ProtobufMessageReflection` has no equivalent.

**5. Schema merging (two proto messages → one Arrow schema)**

`AppendRawMerged` safely concatenates wire bytes from two descriptors and produces a unified schema. Useful when a base message and a sidecar custom message need to be stored together.

**6. YAML-driven declarative config**

No Go code for the projection/denorm plan — point it at a `.proto` file and a YAML config. `ProtobufMessageReflection` is purely programmatic.

**7. Runtime `.proto` compilation — no codegen, no `protoc`**

`NewFromFile` compiles the descriptor at startup. `ProtobufMessageReflection` requires a generated `proto.Message` type.

**8. `HyperType` online PGO**

Wraps a compiled `hyperpb` parser that recompiles itself against live traffic samples, improving parse speed over time. Fully transparent to the caller.

**9. Goroutine-safe streaming design**

`HyperType` + `Plan` are shared immutable state; `Clone` gives each worker an independent builder at ~2× less cost than `New`. `ProtobufMessageReflection` has no concurrency story.

**10. Arrow → Protobuf round-trip**

`tc.Proto(rec, rows)` decodes an Arrow record back into `proto.Message` slices. `ProtobufMessageReflection` is one-way only.

---

### When you **would** use `ProtobufMessageReflection`

- You already have deserialized `proto.Message` objects and just need a schema/record for a one-off conversion
- You don't need batching, fan-out, expressions, or raw-byte performance
- You want to stay within the `apache/arrow` dependency tree without an additional library

---

**TL;DR**: `ProtobufMessageReflection` is a single-message schema/record helper. bufarrowlib is a **production ETL engine** — raw bytes in, batched Arrow out, with denormalization, fan-out, expressions, YAML config, and a compiled hot-path parser that beats hand-written Arrow builders by ~40%.
