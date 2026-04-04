# AGENTS.md — bufarrowlib contributor guide for AI agents

This file tells AI coding agents (Claude Code, GitHub Copilot, Cursor, etc.) how to work effectively in this repository.

## Repository layout

```
bufarrowlib/
├── *.go                    Top-level package (Transcoder, HyperType, config, typemap, nodes)
├── proto/pbpath/           Standalone field-path + expression engine (import separately)
├── cmd/pbpath-playground/  Local web UI for testing paths and YAML denorm configs
├── cbinding/               C shared library (CGO, exports for Python / FFI)
├── python/pybufarrow/      Python bindings via Arrow C Data Interface
├── gen/                    Generated Go code from proto files — DO NOT EDIT MANUALLY
├── testdata/               YAML fixture configs and test proto files
├── docs/                   Performance guide, benchmark result files
├── assets/                 Static assets (logo, etc.)
└── scripts/                Helper shell scripts
```

## Build commands

| Command | Description |
|---|---|
| `go build ./...` | Compile top-level package and all subpackages |
| `make libbufarrow` | Build C shared library (`cbinding/libbufarrow.so`) — requires CGO |
| `make python-dev` | Build shared lib + editable Python install (requires `uv`) |
| `make venv-sync` | Create/update the uv-managed Python venv in `python/` |

## Test commands

```sh
make test-go           # go test -count=1 -timeout 180s ./...
make test-python       # pytest via uv (requires libbufarrow built first)
make test              # both of the above
```

Run a single Go test file or package:
```sh
go test -v -run TestAppendDenorm ./...
go test -v -run TestAppend ./proto/pbpath/...
```

## Benchmark commands

```sh
make bench-go                                          # all benchmarks, 3s each
make bench-go BENCH_FILTER=BenchmarkAppendDenormRaw    # targeted
make bench-go BENCH_FILTER=. BENCH_TIME=10s BENCH_COUNT=3
make bench-throughput                                  # concurrent max-throughput only
make bench-compare                                     # run + diff against previous results
```

## Proto / code generation

The `gen/` directory contains generated Go types from proto files in `testdata/`. Do **not** edit files under `gen/` — regenerate them with:

```sh
buf generate          # uses buf.gen.yaml + buf.work.yaml
```

Requires [buf CLI](https://buf.build/docs/installation). The generated code includes sample message types (`BidRequestEvent`, `TestMsg`, etc.) used in tests and benchmarks.

## Key dependencies

| Dependency | Role | Notes |
|---|---|---|
| `buf.build/go/hyperpb` | Fast protobuf parser | Compiled parser used by `AppendRaw*`. Do not replace with `proto.Unmarshal` — it is 3–5× slower. |
| `github.com/apache/arrow-go/v18` | Arrow schema, builders, RecordBatch, Parquet | Arrow v18 — do not drift the major version without testing all writer/reader paths. |
| `github.com/bufbuild/protocompile` | Runtime `.proto` compilation | Used by `NewFromFile` and `CompileProtoToFileDescriptor`. |
| `google.golang.org/protobuf` | `protoreflect`, `dynamicpb`, `proto.Marshal` | The reflection API is central to most of the library. |
| `gopkg.in/yaml.v3` | YAML config parsing | Used by `ParseDenormConfig`. |

## Code conventions

- **No global state.** `Transcoder` and `HyperType` are constructed explicitly; no `init()` side effects.
- **All errors returned**, never panicked, except programmer errors in Step constructors (e.g. `ListRangeStep3` with step=0).
- **Zero unnecessary allocations on the hot path.** Check with `-benchmem` before and after changes.
- **Shared immutable / independent mutable:** `HyperType` and `Plan` are shared across goroutines (immutable after construction). Arrow builders and scratch buffers are per-`Transcoder` and are **not** goroutine-safe.
- Generated files: `*_gen.go`, `*.pb.go` — never edit these manually.
- `node.go` and `denorm.go` contain the core traversal and fan-out logic. Read the comment headers before modifying.
- The `pbpath` subpackage is independently importable and has its own `README.md` and `ARCHITECTURE.md`. Changes to the `Plan` or expression API surface may break `bufarrow.go` which depends on it — run both test suites.

## When to run proto codegen

Run `buf generate` when:
- Adding new test schemas under `testdata/`
- Changing existing `.proto` files used in tests

Do not run `buf generate` for changes to the library API — the library uses `protoreflect` descriptors, not generated Go types.

## CI

See `.github/workflows/ci.yml`. The CI matrix runs:
- Go tests on `ubuntu-latest` with `CGO_ENABLED=1`
- Go tests triggered on push to `main`/`perf` and all PRs

Python tests are **not** in CI (require a pre-built shared library and platform-specific CGO); run them locally with `make test-python` before submitting Python-related changes.

## Common mistakes to avoid

- **Do not call `New` or `Clone` inside a message processing loop.** Construction is expensive (~300 µs). Pre-create workers, then feed messages through channels.
- **Do not omit `defer rec.Release()`** on every `RecordBatch` returned by `NewRecordBatch()` or `NewDenormalizerRecordBatch()`.
- **Do not share a `Transcoder` between goroutines.** Use `Clone` to get independent builder state per goroutine.
- **Do not drop `HyperType`** on raw-bytes paths. Without it, `AppendRaw` falls back to `dynamicpb` which is 3–5× slower.
- **Do not use `proto.Unmarshal` then `Append`** when raw bytes are available. Use `AppendRaw` / `AppendDenormRaw` instead.

## References

- Full API reference: [`llms-full.txt`](llms-full.txt) (LLM-optimized)
- Quick index: [`llms.txt`](llms.txt)
- pbpath architecture: [`proto/pbpath/ARCHITECTURE.md`](proto/pbpath/ARCHITECTURE.md)
- Performance guide: [`docs/benchmark-results.md`](docs/benchmark-results.md)
