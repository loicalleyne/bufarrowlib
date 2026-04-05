# bufarrowlib Python Guide

> **Who this is for:** Python engineers who want to decode protobuf messages into Apache Arrow / PyArrow with Go handling all the heavy lifting — parsing, schema inference, memory management, and concurrency.

**Install:**
```sh
# Build the shared library first (requires Go + CGO)
make libbufarrow

# Install the Python package (uv recommended)
make python-dev        # editable install via uv
# or manually:
uv pip install --editable python/
```

**Requires:** Python ≥ 3.10 · PyArrow ≥ 14 · libbufarrow.so (built via `make libbufarrow`)

If the library isn't on your `LD_LIBRARY_PATH`, point to it with the env var:
```sh
export BUFARROW_LIB=/path/to/libbufarrow.so
```

---

## The 30-second version

```python
from pybufarrow import Pool

with Pool.from_proto_file("events.proto", "UserEvent", workers=8) as pool:
    for raw_bytes in kafka_consumer:
        pool.submit(raw_bytes)
    batch = pool.flush()   # → pyarrow.RecordBatch, zero-copy
```

That's it. Go runs the goroutines; Python stays single-threaded.

---

## Core concepts

### Zero-copy via Arrow C Data Interface

`flush()` populates `ArrowArray` + `ArrowSchema` C structs in Go memory and hands the pointers to Python. PyArrow imports them via `RecordBatch._import_from_c(...)` — **no data is copied across the FFI boundary**.

### Two output modes

| Mode | API | Use when |
|---|---|---|
| **Full-fidelity** | `append` / `flush` | You want the entire proto message as a nested Arrow Struct schema |
| **Denormalization** | `append_denorm` / `flush_denorm` | You want a flat table, one row per element of a repeated field (fan-out) |

Both modes can be active on the same `Transcoder` simultaneously.

---

## The key types

### `HyperType`

A PGO-optimized protobuf parser. Compile it once, share it across all transcoders and pool workers. Thread-safe.

```python
from pybufarrow import HyperType

ht = HyperType("events.proto", "UserEvent")
# Optional: auto-recompile after N messages have been profiled
ht = HyperType("events.proto", "UserEvent", auto_recompile_threshold=50_000, sample_rate=0.01)
```

Without a `HyperType`, parsing falls back to standard `proto.Unmarshal` — still correct, but 3–5× slower.

### `Transcoder`

Single-goroutine worker. **Not goroutine-safe** — use `clone()` or `Pool` when you need parallelism.

```python
from pybufarrow import HyperType, Transcoder

ht = HyperType("events.proto", "UserEvent")
tc = Transcoder.from_proto_file("events.proto", "UserEvent", hyper_type=ht)

# Inspect the schema before you process any data
print(tc.schema)         # full-fidelity Arrow schema
print(tc.denorm_schema)  # flat denorm schema (if configured)
print(tc.field_names)    # top-level field names
```

Constructors:

| Constructor | When to use |
|---|---|
| `Transcoder.from_proto_file(proto, message, ...)` | You have a `.proto` file |
| `Transcoder.from_config(path)` | You have a YAML denorm config file |
| `Transcoder.from_config_string(yaml_str)` | Inline YAML string |

### `Pool`

A **Go-managed goroutine pool**. Python stays single-threaded; Go goroutines do the parsing concurrently. This is the highest-throughput option for streaming workloads.

```python
from pybufarrow import Pool

pool = Pool.from_proto_file("events.proto", "UserEvent", workers=8)
```

---

## Concurrency patterns

### Pattern 1 — `Pool` (Go handles everything)

**Best for:** Kafka consumers, streaming pipelines, anything where you're feeding a continuous stream of messages from a single Python thread.

```python
from pybufarrow import HyperType, Pool

ht = HyperType("events.proto", "UserEvent")

with Pool.from_proto_file(
    "events.proto", "UserEvent",
    workers=8,        # Go goroutines; 0 = GOMAXPROCS
    capacity=1024,    # job channel buffer; 0 = workers × 64
    hyper_type=ht,
) as pool:
    for msg in kafka_consumer:
        pool.submit(msg.value)           # blocks when channel full → natural backpressure

        if pool.pending() >= 10_000:
            batch = pool.flush()         # drain workers, merge batches, return RecordBatch
            downstream.write(batch)

    batch = pool.flush()                 # drain the remainder
    downstream.write(batch)
```

**How it works under the hood:**
1. `Pool` spins up `N` Go goroutines, each holding an independent `Transcoder` clone (same compiled schema and `HyperType`, separate Arrow builders).
2. `submit()` sends a job to a bounded Go channel. When the channel is full, it blocks Python — providing automatic backpressure without any manual throttling code.
3. `flush()` sends a drain signal, waits for all workers to finish (`wg.Wait()`), merges per-worker `RecordBatch`es column-by-column, restarts the workers for the next window, and returns the merged batch.

### Pattern 2 — `Transcoder.clone()` (Python-side threads)

**Best for:** Batch workloads where you already have partitioned data and want to process each partition in a thread.

```python
import concurrent.futures
from pybufarrow import HyperType, Transcoder

ht = HyperType("events.proto", "UserEvent")
base_tc = Transcoder.from_proto_file("events.proto", "UserEvent", hyper_type=ht)

def process_partition(partition):
    with base_tc.clone() as tc:           # independent builder, shared schema + HyperType
        for raw_bytes in partition:
            tc.append(raw_bytes)
        return tc.flush()                 # zero-copy RecordBatch

with concurrent.futures.ThreadPoolExecutor(max_workers=4) as executor:
    batches = list(executor.map(process_partition, partitioned_messages))
```

Each clone gets its own Go Arrow builder and scratch buffers but shares the immutable compiled schema and `HyperType` recorder. Cloning is ~50× cheaper than constructing a new `Transcoder`.

### Choosing between them

| | `Pool` | `clone()` + threads |
|---|---|---|
| Thread management | Go handles it | Python handles it |
| GIL concern | None — Go goroutines bypass the GIL | Python threads still acquire GIL for non-FFI code |
| Backpressure | Built-in (bounded channel) | Manual |
| Best for | Streaming / continuous ingestion | Batch / pre-partitioned data |

---

## Denormalization (flat fan-out)

When a proto message has a repeated field (e.g. `imp[]`, `items[]`), denormalization produces **one output row per element** — a cross-join across independent repeated fields.

```python
# 2 items × 3 tags in one message = 6 output rows
with Pool.from_proto_file(
    "order.proto", "Order",
    workers=4,
    denorm_columns=[
        "order_id",           # scalar — repeated on every output row
        "items[*].id",        # fan-out group A
        "items[*].price",     # fan-out group A (same index as above)
        "tags[*]",            # fan-out group B (cross-joined with A)
    ],
) as pool:
    for raw in order_stream:
        pool.submit(raw)
    flat_batch = pool.flush()   # flat pyarrow.RecordBatch
```

For YAML-driven configuration (useful for complex expressions and computed columns):

```python
tc = Transcoder.from_config("denorm.yaml")
```

```yaml
# denorm.yaml
proto:
  file: schema/order.proto
  message: Order

denormalizer:
  columns:
    - name: order_id
      path: order_id
    - name: item_id
      path: items[*].id
    - name: item_price
      path: items[*].price
    - name: item_type
      expr:
        func: cond
        args:
          - expr: {func: has, args: [{path: items[*].video_id}]}
          - literal: "video"
          - literal: "display"
```

### Path syntax quick reference

| Syntax | Meaning |
|---|---|
| `field` | scalar field |
| `parent.child` | nested message field |
| `repeated[*]` | fan-out wildcard — one row per element |
| `repeated[0]` | fixed index, returns scalar (no fan-out) |
| `repeated[1:3]` | slice, fan-out over elements 1 and 2 |
| `repeated[-2:]` | last two elements |

---

## Batch helpers

For one-shot conversions without manually managing a `Transcoder`:

```python
from pybufarrow.batch import transcode_batch, transcode_to_table, transcode_to_parquet

# Iterator of fixed-size RecordBatches (default batch_size=1024)
for batch in transcode_batch("events.proto", "UserEvent", raw_messages, batch_size=2048):
    process(batch)

# All messages → single pyarrow.Table
table = transcode_to_table("events.proto", "UserEvent", raw_messages)

# Direct to Parquet
transcode_to_parquet("events.proto", "UserEvent", raw_messages, "output.parquet")
```

---

## Parquet I/O

```python
tc = Transcoder.from_proto_file("events.proto", "UserEvent")

for raw in messages:
    tc.append(raw)

# Write buffered rows to Parquet
tc.write_parquet("output.parquet")

# Read back (columns=None reads all columns)
batch = tc.read_parquet("output.parquet", columns=[0, 2])
```

---

## Performance tips

- **Always pass a `HyperType`** — it enables the hyperpb TDP parser which is 3–5× faster than standard `proto.Unmarshal`.
- **Reuse your `Pool` or `Transcoder`** across a batch of messages; construction is expensive (~5 ms for `from_proto_file`). `Clone` is ~110 µs.
- **Let `flush()` decide batch boundaries**, not individual messages. Flush at fixed intervals (time or count) rather than per-message.
- **`workers=0` defers to `GOMAXPROCS`** — a reasonable default on most machines. Tune upward if your proto messages are large or deeply nested.
- **`capacity=0` defaults to `workers × 64`** — usually fine. Increase for very bursty producers to smooth out spikes.

Throughput reference (AMD EPYC 7B13, 8 workers, realistic ad-tech BidRequestEvent schema):

| Approach | Throughput |
|---|---|
| `Pool.submit` + hyperpb | ~296K msg/s |
| `Transcoder.append_denorm` (single-threaded) | ~180K msg/s |
| Without `HyperType` | ~3–5× slower |

---

## Quick-start checklist

1. `make libbufarrow` — build the Go shared library
2. `make python-dev` — install pybufarrow into a uv venv
3. Create a `HyperType` once at startup, pass it everywhere
4. Use `Pool` for streaming; use `clone()` + threads for batch
5. Always `flush()` at the end; always `.release()` or use `with` blocks for `RecordBatch`es
6. Inspect `pool.schema` / `tc.schema` before your first flush to validate the Arrow schema matches your expectations
