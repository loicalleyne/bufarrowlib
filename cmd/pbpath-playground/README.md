# pbpath-playground

A local web UI for exploring protobuf messages and testing bufarrowlib denormalization configs interactively — before writing any pipeline code.

```sh
go run ./cmd/pbpath-playground --proto path/to/schema.proto
```

Opens at `http://localhost:4195` and auto-launches your browser.

---

## What it does

Two independent modes in one UI:

**Pipeline mode** — evaluate [`pbpath` pipeline expressions](../../proto/pbpath/README.md) (jq-style) against a live protobuf message. Type an expression and see the result instantly. Supports the full Pipeline API: field access, wildcards, slices, `select()`, arithmetic, `map()`, `sort_by()`, variables, conditionals, `reduce`, `try/catch`, and 70+ built-in functions.

**Denorm mode** — paste a YAML denorm config (same format as `NewTranscoderFromConfigFile`), and see the resulting Arrow `RecordBatch` rendered as a live table. The "Reset Skeleton" button generates a starter YAML from the selected message's top-level fields.

Both modes work with randomly generated messages (seeded for reproducibility) or with messages from a real binary corpus file.

---

## Flags

| Flag | Default | Description |
|---|---|---|
| `--proto` | required | Path to a `.proto` file. Repeatable — pass multiple times to load several files. |
| `--import-path` | — | Proto import directory. Repeatable. |
| `--corpus` | — | Path to a length-prefixed binary corpus file (see [Corpus format](#corpus-format)). |
| `--corpus-message` | — | Message name to use when deserializing corpus entries. Required when `--corpus` is set. |
| `--port` | `4195` | HTTP port to bind on. |
| `--host` | `localhost` | Bind address. |
| `--no-open` | false | Suppress auto-opening the browser. |
| `--seed` | 0 (random) | Protorand seed for reproducible random messages. 0 picks a new random seed each run. |

---

## Usage

### Basic: single proto file

```sh
go run ./cmd/pbpath-playground \
  --proto schema/bidrequest.proto
```

### Multiple proto files with shared imports

```sh
go run ./cmd/pbpath-playground \
  --proto schema/bidrequest.proto \
  --proto schema/extensions.proto \
  --import-path schema/
```

### With a real corpus

Load a length-prefixed binary file of serialized messages (e.g. exported from a Kafka consumer or benchmark tool) to run expressions against real production data instead of random data:

```sh
go run ./cmd/pbpath-playground \
  --proto schema/bidrequest.proto \
  --corpus testdata/corpus.bin \
  --corpus-message BidRequest
```

When a corpus is loaded, a `← / →` navigator appears in the UI to step through messages individually.

### Reproducible random messages

```sh
go run ./cmd/pbpath-playground \
  --proto schema/event.proto \
  --seed 42
```

The same seed always produces the same sequence of random messages. Useful for sharing a specific test case with a teammate.

---

## Pipeline mode

Type a [pbpath pipeline expression](../../proto/pbpath/README.md#pipeline-api-jq-style) in the editor. Results update live (debounced). Parse errors and execution errors are shown inline.

**Input format** — toggle between JSON and Proto Text format for the message display and input. The message view updates automatically when you switch.

**Output format** — toggle results between JSON and Proto Text.

```
# Basic field access
.id

# Nested access
.device.geo.country

# Fan-out over repeated fields
.imp | .[] | .id

# Select impressions with video
.imp | .[] | select(has(.video)) | .id

# Collect all deal IDs across all impressions
[.imp | .[] | .pmp.deals | .[] | .id]

# Cross-field arithmetic
.imp | .[0] | .bidfloor * 1000

# Variable binding
.id as $req_id | .imp | .[] | [$req_id, .id]

# Regex filter
.imp | .[] | select(.id | test("^imp-[0-9]+")) | .bidfloor

# Build an object
.imp | .[] | {imp_id: .id, floor: .bidfloor, has_video: has(.video)}
```

---

## Denorm mode

Switch to "Denorm" in the tab bar. Paste or edit a YAML `columns:` config:

```yaml
columns:
  - name: request_id
    path: id

  - name: imp_id
    path: imp[*].id         # fan-out: one row per impression

  - name: floor_price
    path: imp[*].bidfloor

  - name: imp_type
    expr:
      func: cond
      args:
        - expr: {func: has, args: [{path: imp[*].video.id}]}
        - literal: "video"
        - literal: "display"

  - name: floor_micros
    expr:
      func: mul
      args:
        - path: imp[*].bidfloor
        - literal: 1000.0
```

Click **Run** (or press `Ctrl+Enter`) to evaluate. The result is shown as a table — one row per fan-out branch, one column per declared output.

**Reset Skeleton** generates a starter config from the selected message's top-level fields: scalar fields become scalar columns, repeated scalars become `field[*]` fan-out columns, and repeated messages become a commented example.

The YAML config format is identical to the `denorm.yaml` format accepted by `NewTranscoderFromConfigFile`. Use Denorm mode to iterate on your config before deploying it in a pipeline.

---

## Corpus format

A corpus file is a sequence of length-prefixed protobuf messages:

```
[4-byte LE uint32 length][N bytes: proto.Marshal output] × M
```

To create one from a Go slice:

```go
import (
    "encoding/binary"
    "os"
    "google.golang.org/protobuf/proto"
)

func writeCorpus(path string, msgs []proto.Message) error {
    f, err := os.Create(path)
    if err != nil {
        return err
    }
    defer f.Close()
    var lenBuf [4]byte
    for _, m := range msgs {
        b, err := proto.Marshal(m)
        if err != nil {
            return err
        }
        binary.LittleEndian.PutUint32(lenBuf[:], uint32(len(b)))
        f.Write(lenBuf[:])
        f.Write(b)
    }
    return nil
}
```

---

## HTTP API

The playground exposes a simple JSON API (consumed by the browser UI):

| Endpoint | Method | Description |
|---|---|---|
| `GET /api/messages` | GET | List of loaded message names |
| `POST /api/generate` | POST | Generate a random message (`{message, seed}` → `{json, textpb}`) |
| `POST /api/execute` | POST | Evaluate a pipeline (`{message, input, format, pipeline, output_format}` → `{result, parse_error, exec_error}`) |
| `GET /api/corpus/{n}` | GET | Fetch corpus message at index `n` (`{json, textpb, index, total}`) |
| `POST /api/denorm` | POST | Evaluate a denorm YAML config (`{message, input, format, config}` → `{columns, rows, num_rows}`) |
| `GET /api/denorm/skeleton` | GET | Generate starter YAML for a message (`?message=Name` → `{config}`) |

The server binds to `localhost` only by default. Do not expose it to the network.

---

## Relation to other bufarrowlib tools

| Tool | Purpose |
|---|---|
| [`proto/pbpath`](../../proto/pbpath) | The path/expression engine the playground runs on top of |
| [`bufarrowlib`](../../) | The main transcoding library; Denorm mode uses `NewTranscoderFromConfig` directly |
| `llms.txt` | Machine-readable API reference for the full library |
| `docs/benchmark-results.md` | Performance guide with tuning advice |
