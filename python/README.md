# pybufarrow

Turn raw protobuf bytes into Apache Arrow RecordBatches — without deserializing, without codegen, without copying data.

**pybufarrow** wraps [bufarrowlib](https://github.com/loicalleyne/bufarrowlib), a Go library that transcodes serialized protobuf messages directly into Arrow columnar format using zero-copy memory sharing via the [Arrow C Data Interface](https://arrow.apache.org/docs/format/CDataInterface.html). Give it a `.proto` file and a stream of raw bytes; get back a `pyarrow.RecordBatch` you can hand straight to Pandas, Polars, DuckDB, or write to Parquet.

It's fast: the underlying engine processes **~300K messages/sec** on production-shaped data — 39% faster than hand-written Arrow builders — while using 57% fewer allocations per message.

## When to use pybufarrow

- **Kafka / Pub-Sub consumers** that receive protobuf-encoded messages and need them in columnar format for analytics, dashboards, or data lake ingestion.
- **ETL pipelines** where protobuf is the wire format from upstream services and the destination is Parquet, Delta Lake, or a columnar database.
- **Streaming into DuckDB** — denormalize protobuf messages into flat Arrow RecordBatches and append them directly to DuckDB. Aggregating pre-flattened data is orders of magnitude faster than querying nested structures.
- **Ad-tech / real-time bidding** — denormalize nested messages like OpenRTB BidRequests into flat, query-friendly tables with a single YAML config.
- **Data science notebooks** — skip the `proto → dict → DataFrame` dance. Go straight from wire bytes to Arrow-backed DataFrames with type fidelity.
- **Feature stores / ML pipelines** that ingest protobuf event streams and need low-latency materialization into Parquet or Arrow IPC.

## Installation

```bash
pip install pybufarrow
```

Or from source (using [uv](https://docs.astral.sh/uv/)):

```bash
# Build the shared library
cd bufarrowlib/cbinding && make build

# Copy into the Python package
cp cbinding/libbufarrow.so python/pybufarrow/

# Install
cd python && uv sync
```

## Quick Start

```python
from pybufarrow import HyperType, Transcoder

# HyperType compiles a high-performance parser from your .proto definition.
# Share one instance across all transcoders — it's thread-safe.
ht = HyperType("events.proto", "UserEvent")

with Transcoder.from_proto_file("events.proto", "UserEvent", hyper_type=ht) as tc:
    # Feed raw protobuf bytes — no deserialization needed
    for raw_bytes in kafka_consumer:
        tc.append(raw_bytes)

    # Flush to a zero-copy pyarrow RecordBatch
    batch = tc.flush()

# Use it anywhere Arrow is accepted
df = batch.to_pandas()
import duckdb
duckdb.sql("SELECT * FROM batch WHERE event_type = 'purchase'")
```

## Streaming Batches

Process millions of messages without holding everything in memory. `transcode_batch` yields fixed-size RecordBatches as an iterator:

```python
from pybufarrow import transcode_batch

# Yield 122880-row batches from a message stream
for batch in transcode_batch("events.proto", "UserEvent", message_stream, batch_size=122880):
    # Write each batch to a Parquet dataset, push to a queue, etc.
    writer.write_batch(batch)
```

Collect everything into a single Arrow Table, or write directly to Parquet:

```python
from pybufarrow import transcode_to_table, transcode_to_parquet

# Arrow Table — ready for Polars, DuckDB, or any Arrow-native tool
table = transcode_to_table("events.proto", "UserEvent", messages)

# Straight to Parquet — no intermediate DataFrame
transcode_to_parquet("events.proto", "UserEvent", messages, "events.parquet")
```

## Denormalization

Protobuf messages are often deeply nested — `repeated` fields, nested sub-messages, maps. Querying nested Arrow structs is painful. pybufarrow can **flatten (denormalize)** nested messages into wide, query-friendly rows with fan-out on repeated fields.

### With a YAML config

```yaml
# denorm.yaml
proto_file: order.proto
message_name: Order
denorm:
  columns:
    - name                      # top-level scalar
    - items[*].id               # fan-out: one row per item
    - items[*].price
    - seq
```

```python
with Transcoder.from_config("denorm.yaml") as tc:
    for raw in order_stream:
        tc.append_denorm(raw)

    flat = tc.flush_denorm()
    # An order with 3 items produces 3 rows, each with the parent's `name` and `seq`
```

### Programmatic column selection

```python
ht = HyperType("order.proto", "Order")

with Transcoder.from_proto_file(
    "order.proto", "Order",
    hyper_type=ht,
    denorm_columns=["name", "items[*].id", "items[*].price", "seq"],
) as tc:
    tc.append_denorm(raw_order)
    flat = tc.flush_denorm()
    print(flat.to_pandas())
    #   name  id     price  seq
    #   acme  sku-1  9.99   1
    #   acme  sku-2  4.50   1
```

Empty repeated fields produce one row with nulls (left-join semantics), so you never lose parent records.

### Why denormalize?

Nested protobuf structures are great for wire transport but terrible for analytics. Querying nested Arrow structs or repeated fields requires unnesting at query time — which is expensive and makes aggregations slow. The denormalizer does the fan-out once at ingest time, producing flat columns that DuckDB, Pandas, and Polars can aggregate at full speed:

```python
import duckdb

con = duckdb.connect("pipeline.duckdb")

# Create table from the first denormalized batch (schema inferred from Arrow)
con.execute("CREATE TABLE IF NOT EXISTS events AS SELECT * FROM flat_batch LIMIT 0")

# Append denormalized RecordBatches — zero-copy via Arrow
con.execute("INSERT INTO events SELECT * FROM flat_batch")

# Standard SQL on simple, flat columns — orders of magnitude faster than nested
con.sql("""
    SELECT service_name, count(*) as events, avg(latency_ms)
    FROM events
    GROUP BY service_name
    ORDER BY event_tm DESC
""")
```

## Merging Multiple Protobuf Messages

When your pipeline enriches a base message with sidecar data (e.g., a bid request plus server-side metadata), append both in a single call:

```python
with Transcoder.from_proto_file(
    "bidrequests.proto", "BidRequest",
    custom_proto="server_meta.proto",
    custom_message="ServerMeta",
) as tc:
    tc.append_merged(bid_request_bytes, server_meta_bytes)
    batch = tc.flush()
    # The resulting schema includes columns from both messages
```

## Parallel Processing with Clone

`clone()` creates an independent transcoder that shares the same compiled schema and HyperType. Use it to fan out across threads:

```python
import concurrent.futures
from pybufarrow import HyperType, Transcoder

ht = HyperType("events.proto", "UserEvent")
base_tc = Transcoder.from_proto_file("events.proto", "UserEvent", hyper_type=ht)

def process_partition(partition):
    with base_tc.clone() as tc:
        for msg in partition:
            tc.append(msg)
        return tc.flush()

with concurrent.futures.ThreadPoolExecutor(max_workers=4) as pool:
    batches = list(pool.map(process_partition, partitioned_messages))
```

## Parquet I/O

Write Arrow data to Parquet and read it back — useful for materializing transcoded streams to disk:

```python
with Transcoder.from_proto_file("events.proto", "UserEvent", hyper_type=ht) as tc:
    for msg in messages:
        tc.append(msg)

    # Write directly to Parquet (no intermediate pyarrow step)
    tc.write_parquet("events.parquet")

    # Read back as a RecordBatch, optionally selecting columns by index
    batch = tc.read_parquet("events.parquet", columns=[0, 1, 3])
```

## Architecture

```
Python user code
    ↓  Pythonic API
pybufarrow (ctypes)
    ↓  C ABI via Arrow C Data Interface
libbufarrow.so (CGo shared library)
    ↓  hyperpb TDP parser (2–3× faster than generated code)
bufarrowlib (Go)
    ↓  zero-copy Arrow record batches
pyarrow
```

No gRPC. No protoc codegen. No serialization round-trips. The Arrow C Data Interface means RecordBatches cross the Go→Python boundary without copying memory.

## API Reference

### Core Classes

| Class | Purpose |
|---|---|
| `Transcoder` | Ingests raw protobuf bytes, flushes Arrow RecordBatches |
| `HyperType` | Compiles a high-performance parser from a `.proto` file. Thread-safe, shareable across transcoders |
| `BufarrowError` | Raised on invalid protos, missing configs, use-after-close, etc. |

### Transcoder Constructors

| Constructor | Use when |
|---|---|
| `Transcoder.from_proto_file(proto, msg, ...)` | You have a `.proto` file |
| `Transcoder.from_config(path)` | You have a YAML config with denorm rules |
| `Transcoder.from_config_string(yaml)` | Same, but the YAML is a string |

### Transcoder Methods

| Method | Description |
|---|---|
| `append(data)` | Ingest raw protobuf bytes (requires HyperType) |
| `append_merged(base, custom)` | Ingest two protobuf messages as one row (requires `custom_proto`) |
| `append_denorm(data)` | Ingest with denormalization / fan-out (requires HyperType + denorm plan) |
| `flush()` → `RecordBatch` | Flush accumulated rows as a zero-copy Arrow RecordBatch |
| `flush_denorm()` → `RecordBatch` | Flush denormalized rows |
| `write_parquet(path)` | Write buffered data to Parquet |
| `read_parquet(path, columns=None)` | Read Parquet file back as a RecordBatch |
| `clone()` → `Transcoder` | Create an independent copy for parallel use |
| `schema` | Arrow schema (cached) |
| `field_names` | List of field name strings |

### Batch Helpers

| Function | Description |
|---|---|
| `transcode_batch(proto, msg, messages, batch_size=1024)` | Iterator of fixed-size RecordBatches |
| `transcode_merged_batch(proto, msg, messages, batch_size=1024)` | Same, for merged `(base, custom)` message pairs |
| `transcode_to_table(proto, msg, messages)` | Collect all messages into a single `pyarrow.Table` |
| `transcode_to_parquet(proto, msg, messages, path)` | Transcode and write directly to Parquet |

## Requirements

- Python >= 3.9
- pyarrow >= 14.0
- `libbufarrow.so` / `libbufarrow.dylib` shared library (built from Go source via `make build` in `cbinding/`)

## License

Apache-2.0
