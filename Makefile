# bufarrowlib top-level Makefile — build shared library and Python wheel.
.POSIX:

GO           ?= go
PYTHON       ?= python3
UV           ?= uv
BENCH_TIME   ?= 3s
BENCH_COUNT  ?= 1
BENCH_FILTER ?= .

# CPU_SLUG: detect CPU model from /proc/cpuinfo (Linux) or sysctl (macOS),
# then convert to lowercase snake_case suitable for use as a filename prefix.
CPU_MODEL := $(shell grep -m1 'model name' /proc/cpuinfo 2>/dev/null \
               | sed 's/.*: //' \
               | grep -v '^$$' \
             || sysctl -n machdep.cpu.brand_string 2>/dev/null)
CPU_SLUG  := $(shell echo '$(CPU_MODEL)' \
               | tr '[:upper:]' '[:lower:]' \
               | tr -cs 'a-z0-9' '_' \
               | sed 's/^_*//;s/_*$$//')
BENCH_OUT        ?= docs/$(CPU_SLUG)-benchmark-results.txt
BENCH_OUT_PYTHON ?= docs/$(CPU_SLUG)-benchmark-results-python.json

EXT      = so
ifeq ($(shell uname -s),Darwin)
  EXT    = dylib
endif

LIB      = cbinding/libbufarrow.$(EXT)

.PHONY: libbufarrow libbufarrow-all python python-dev venv-sync test-go test-python test \
        bench bench-go bench-python bench-throughput bench-throughput-go \
        bench-throughput-python bench-compare clean

# ── Shared library ──────────────────────────────────────────────────────

libbufarrow: $(LIB)

$(LIB):
	CGO_ENABLED=1 $(GO) build -buildmode=c-shared -tags cgo -o $(LIB) ./cbinding

libbufarrow-all:
	$(MAKE) -C cbinding build-all

# ── Python ──────────────────────────────────────────────────────────────

# venv-sync — create/update the uv-managed .venv inside python/ and install
# all dev dependencies declared in pyproject.toml [dependency-groups.dev].
venv-sync:
	cd python && $(UV) sync --all-groups

python: libbufarrow
	cp $(LIB) python/pybufarrow/
	cd python && $(UV) build

python-dev: libbufarrow venv-sync
	cp $(LIB) python/pybufarrow/
	cd python && $(UV) pip install -e .

# ── Tests ───────────────────────────────────────────────────────────────

test-go:
	$(GO) test -count=1 -timeout 180s ./...

test-python: venv-sync
	cp $(LIB) python/pybufarrow/
	cd python && $(UV) pip install -e . --quiet
	cd python && $(UV) run pytest tests/ -v

test: test-go test-python

# ── Benchmarks ──────────────────────────────────────────────────────────
#
# bench       — run both Go and Python benchmarks
# bench-go    — Go only; override filter with BENCH_FILTER=BenchmarkFoo
#               e.g. make bench-go BENCH_FILTER=BenchmarkAppendRaw BENCH_TIME=10s
# bench-python— Python only (uv-managed venv, pytest-benchmark)
# bench-compare — Go bench saved to BENCH_OUT for diffing with benchstat

bench: bench-go bench-python

bench-go:
	$(GO) test -run='^$$' \
	    -bench='$(BENCH_FILTER)' \
	    -benchtime=$(BENCH_TIME) \
	    -count=$(BENCH_COUNT) \
	    -timeout=60m \
	    ./...

bench-python:
	cd python && $(UV) run pytest tests/test_benchmark.py \
	    --benchmark-only \
	    --benchmark-columns=min,mean,stddev,rounds \
	    --benchmark-json=../$(BENCH_OUT_PYTHON) \
	    -v

# bench-throughput — run only the MaxThroughput concurrent benchmarks.
#   Go:    BenchmarkMaxThroughput_Concurrent{AppendRaw,AppendRawMerged,AppendDenormRaw}
#   Python: TestBenchmarkMaxThroughputConcurrent (all three × all worker counts)
#   Override worker-count filter with:
#     make bench-throughput BENCH_FILTER=BenchmarkMaxThroughput_ConcurrentAppendRaw
bench-throughput: bench-throughput-go bench-throughput-python

bench-throughput-go:
	$(GO) test -run='^$$' \
	    -bench='BenchmarkMaxThroughput_Concurrent' \
	    -benchtime=$(BENCH_TIME) \
	    -count=$(BENCH_COUNT) \
	    -timeout=60m \
	    ./...

bench-throughput-python:
	cd python && $(UV) run pytest tests/test_benchmark.py \
	    -k 'TestBenchmarkMaxThroughputConcurrent or TestBenchmarkMaxThroughputPool' \
	    --benchmark-only \
	    --benchmark-columns=min,mean,stddev,rounds \
	    --benchmark-json=../$(BENCH_OUT_PYTHON) \
	    -v

# bench-compare — rotate BENCH_OUT → BENCH_OUT.old, run, then diff with benchstat.
#   First run: no .old file yet; benchstat will note that and skip the diff.
#   Subsequent runs: benchstat old.txt new.txt shows delta automatically.
bench-compare:
	@if [ -f $(BENCH_OUT) ]; then \
	    cp $(BENCH_OUT) $(BENCH_OUT).old; \
	    echo "Rotated previous Go results to $(BENCH_OUT).old"; \
	fi
	@if [ -f $(BENCH_OUT_PYTHON) ]; then \
	    cp $(BENCH_OUT_PYTHON) $(BENCH_OUT_PYTHON).old; \
	    echo "Rotated previous Python results to $(BENCH_OUT_PYTHON).old"; \
	fi
	$(GO) test -run='^$$' \
	    -bench='$(BENCH_FILTER)' \
	    -benchtime=$(BENCH_TIME) \
	    -count=$(BENCH_COUNT) \
	    -timeout=60m \
	    ./... | tee $(BENCH_OUT)
	cd python && $(UV) run pytest tests/test_benchmark.py \
	    --benchmark-only \
	    --benchmark-columns=min,mean,stddev,rounds \
	    --benchmark-json=../$(BENCH_OUT_PYTHON) \
	    -v
	@if [ -f $(BENCH_OUT).old ]; then \
	    echo ""; \
	    echo "=== benchstat delta (Go) ==="; \
	    benchstat $(BENCH_OUT).old $(BENCH_OUT); \
	else \
	    echo "Go results saved to $(BENCH_OUT) — run again to compare."; \
	fi
	@if [ -f $(BENCH_OUT_PYTHON).old ]; then \
	    echo ""; \
	    echo "=== Python benchmark delta ==="; \
	    cd python && $(UV) run python ../scripts/bench_compare_py.py \
	        ../$(BENCH_OUT_PYTHON).old ../$(BENCH_OUT_PYTHON); \
	else \
	    echo "Python results saved to $(BENCH_OUT_PYTHON) — run again to compare."; \
	fi

# ── Clean ───────────────────────────────────────────────────────────────

clean:
	$(MAKE) -C cbinding clean
	rm -f python/pybufarrow/libbufarrow.so python/pybufarrow/libbufarrow.dylib
	rm -rf python/dist python/build python/*.egg-info
