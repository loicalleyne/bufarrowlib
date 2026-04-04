"""Benchmarks for pybufarrow — measures FFI overhead and throughput.

Requires pytest-benchmark: pip install pytest-benchmark
Run with: pytest tests/test_benchmark.py --benchmark-only
"""

from __future__ import annotations

import os
import concurrent.futures as _futures

import pytest

import pyarrow as pa

from pybufarrow import Pool, Transcoder

from .conftest import encode_order, encode_test_msg, encode_custom_extra, encode_bid_request

# Worker counts for concurrent benchmarks: 1, 2, 4, cpu_count, cpu_count×2.
_CPU_COUNT = os.cpu_count() or 4
_WORKER_COUNTS = sorted({1, 2, 4, _CPU_COUNT, _CPU_COUNT * 2})

# ── Fixtures ─────────────────────────────────────────────────────────


@pytest.fixture
def scalar_corpus() -> list[bytes]:
    """122880 serialized TestMsg messages."""
    return [
        encode_test_msg(f"user-{i}", i % 100, float(i) * 1.1, i % 2 == 0)
        for i in range(122880)
    ]


@pytest.fixture
def order_corpus() -> list[bytes]:
    """40960 serialized Order messages with 3 items each (= 122880 denorm rows)."""
    return [
        encode_order(
            f"order-{i}",
            [(f"item-{j}", float(j) + 0.99) for j in range(3)],
            seq=i,
        )
        for i in range(40960)
    ]


@pytest.fixture
def merged_corpus() -> list[tuple[bytes, bytes]]:
    """122880 (base, custom) byte pairs for append_merged."""
    return [
        (
            encode_test_msg(f"user-{i}", i % 100, float(i) * 1.1, i % 2 == 0),
            encode_custom_extra(1700000000 + i, f"src-{i % 10}"),
        )
        for i in range(122880)
    ]


# ── Benchmark: append + flush (scalar, no denorm) ───────────────────


class TestBenchmarkAppendFlush:
    """Benchmark the core append→flush cycle."""

    def test_append_flush_122880(
        self, benchmark, test_proto, hyper_type, scalar_corpus
    ):
        """122880 scalar messages: append all then flush."""

        def run():
            with Transcoder.from_proto_file(
                test_proto, "TestMsg", hyper_type=hyper_type
            ) as tc:
                for msg in scalar_corpus:
                    tc.append(msg)
                batch = tc.flush()
                assert batch.num_rows == 122880
                return batch

        benchmark(run)


# ── Benchmark: append_merged + flush ─────────────────────────────────


class TestBenchmarkAppendMerged:
    """Benchmark the merged-bytes append path."""

    def test_append_merged_122880(
        self, benchmark, test_proto, custom_proto, merged_corpus
    ):
        """122880 merged (base+custom) messages: append_merged then flush."""

        def run():
            with Transcoder.from_proto_file(
                test_proto,
                "TestMsg",
                custom_proto=custom_proto,
                custom_message="CustomExtra",
            ) as tc:
                for base, custom in merged_corpus:
                    tc.append_merged(base, custom)
                batch = tc.flush()
                assert batch.num_rows == 122880
                return batch

        benchmark(run)


# ── Benchmark: denorm append + flush ─────────────────────────────────


class TestBenchmarkDenorm:
    """Benchmark the denormalization path."""

    def test_append_denorm_122880(
        self, benchmark, order_proto, order_hyper_type, order_corpus
    ):
        """40960 Order messages × 3 items → denorm fan-out → 122880 rows."""
        denorm_columns = ["name", "items[*].id", "items[*].price", "seq"]

        def run():
            with Transcoder.from_proto_file(
                order_proto,
                "Order",
                hyper_type=order_hyper_type,
                denorm_columns=denorm_columns,
            ) as tc:
                for msg in order_corpus:
                    tc.append_denorm(msg)
                batch = tc.flush_denorm()
                # 40960 orders × 3 items = 122880 rows
                assert batch.num_rows == 122880
                return batch

        benchmark(run)


# ── Benchmark: schema access (cached property) ──────────────────────


class TestBenchmarkSchema:
    """Benchmark schema retrieval via FFI."""

    def test_schema_access(self, benchmark, test_proto):
        """Access schema property (should be cached after first call)."""
        tc = Transcoder.from_proto_file(test_proto, "TestMsg")

        def run():
            s = tc.schema
            assert isinstance(s, pa.Schema)
            return s

        benchmark(run)
        tc.close()


# ── Benchmark: flush empty (measures FFI round-trip overhead) ────────


class TestBenchmarkFFIOverhead:
    """Measure raw FFI round-trip cost."""

    def test_flush_empty(self, benchmark, test_proto, hyper_type):
        """Flush with zero rows — measures pure FFI + Arrow import overhead."""
        tc = Transcoder.from_proto_file(
            test_proto, "TestMsg", hyper_type=hyper_type
        )

        def run():
            batch = tc.flush()
            assert batch.num_rows == 0
            return batch

        benchmark(run)
        tc.close()

    def test_create_close_cycle(self, benchmark, test_proto):
        """Transcoder create + close — measures handle allocation overhead."""

        def run():
            tc = Transcoder.from_proto_file(test_proto, "TestMsg")
            tc.close()

        benchmark(run)


# ── Benchmark: Parquet write ─────────────────────────────────────────


class TestBenchmarkParquet:
    """Benchmark Parquet write throughput."""

    def test_write_parquet_100(
        self, benchmark, test_proto, hyper_type, scalar_corpus, tmp_path
    ):
        """Write 100 scalar rows to Parquet."""
        out = str(tmp_path / "bench.parquet")

        def run():
            with Transcoder.from_proto_file(
                test_proto, "TestMsg", hyper_type=hyper_type
            ) as tc:
                for msg in scalar_corpus:
                    tc.append(msg)
                tc.write_parquet(out)

        benchmark(run)


# ── Benchmark: Maximum-throughput concurrent (ThreadPoolExecutor) ────
#
# Mirrors the three Go BenchmarkMaxThroughput_Concurrent* benchmarks.
# Each test pre-creates one Transcoder per worker (mirroring the Go Clone
# pattern) and one shared ThreadPoolExecutor. The GIL is released during
# CGo FFI calls so Go parsing code executes in parallel across OS threads.
#
# Sub-benchmarks are parameterised over _WORKER_COUNTS so scaling curves
# are produced in the same way as the Go benchmarks.


class TestBenchmarkMaxThroughputConcurrent:
    """Maximum-throughput concurrent benchmarks using ThreadPoolExecutor."""

    # ── AppendRaw ──────────────────────────────────────────────────────

    @pytest.mark.parametrize("workers", _WORKER_COUNTS)
    def test_concurrent_append_raw(self, benchmark, test_proto, hyper_type, workers):
        """Concurrent AppendRaw: N workers each process a shard of 5120 TestMsg."""
        corpus = [
            encode_test_msg(f"user-{i % 1000}", i % 100, float(i) * 1.1, i % 2 == 0)
            for i in range(5120)
        ]
        shards = [corpus[i::workers] for i in range(workers)]
        # One Transcoder per worker — mirrors the Go Clone-per-worker pattern.
        pool = [
            Transcoder.from_proto_file(test_proto, "TestMsg", hyper_type=hyper_type)
            for _ in range(workers)
        ]

        def _process(tc, shard):
            for msg in shard:
                tc.append(msg)
            tc.flush()  # resets internal builders for the next iteration

        with _futures.ThreadPoolExecutor(max_workers=workers) as executor:

            def run():
                futs = [
                    executor.submit(_process, tc, shard)
                    for tc, shard in zip(pool, shards)
                ]
                for f in futs:
                    f.result()

            benchmark(run)

        for tc in pool:
            tc.close()

    # ── AppendMerged ───────────────────────────────────────────────────

    @pytest.mark.parametrize("workers", _WORKER_COUNTS)
    def test_concurrent_append_merged(
        self, benchmark, test_proto, custom_proto, workers
    ):
        """Concurrent AppendMerged: N workers each process a shard of 5120 base+custom pairs."""
        base_msgs = [
            encode_test_msg(f"user-{i % 1000}", i % 100, float(i) * 1.1, i % 2 == 0)
            for i in range(5120)
        ]
        custom_msgs = [
            encode_custom_extra(1700000000 + i, f"src-{i % 10}") for i in range(5120)
        ]
        corpus = list(zip(base_msgs, custom_msgs))
        shards = [corpus[i::workers] for i in range(workers)]
        pool = [
            Transcoder.from_proto_file(
                test_proto,
                "TestMsg",
                custom_proto=custom_proto,
                custom_message="CustomExtra",
            )
            for _ in range(workers)
        ]

        def _process(tc, shard):
            for base, custom in shard:
                tc.append_merged(base, custom)
            tc.flush()

        with _futures.ThreadPoolExecutor(max_workers=workers) as executor:

            def run():
                futs = [
                    executor.submit(_process, tc, shard)
                    for tc, shard in zip(pool, shards)
                ]
                for f in futs:
                    f.result()

            benchmark(run)

        for tc in pool:
            tc.close()

    # ── AppendDenorm ───────────────────────────────────────────────────

    @pytest.mark.parametrize("workers", _WORKER_COUNTS)
    def test_concurrent_append_denorm(
        self, benchmark, order_proto, order_hyper_type, workers
    ):
        """Concurrent AppendDenorm: N workers each process a shard of 5120 Order messages."""
        corpus = [
            encode_order(
                f"order-{i}",
                [(f"item-{j}", float(j) + 0.99) for j in range(3)],
                seq=i,
            )
            for i in range(5120)
        ]
        shards = [corpus[i::workers] for i in range(workers)]
        denorm_columns = ["name", "items[*].id", "items[*].price", "seq"]
        pool = [
            Transcoder.from_proto_file(
                order_proto,
                "Order",
                hyper_type=order_hyper_type,
                denorm_columns=denorm_columns,
            )
            for _ in range(workers)
        ]

        def _process(tc, shard):
            for msg in shard:
                tc.append_denorm(msg)
            tc.flush_denorm()

        with _futures.ThreadPoolExecutor(max_workers=workers) as executor:

            def run():
                futs = [
                    executor.submit(_process, tc, shard)
                    for tc, shard in zip(pool, shards)
                ]
                for f in futs:
                    f.result()

            benchmark(run)

        for tc in pool:
            tc.close()


# ── Benchmark: Maximum-throughput Pool (Go-managed goroutines) ───────────
#
# Mirrors the three Go BenchmarkMaxThroughput_Concurrent* benchmarks but uses
# the Pool handle instead of ThreadPoolExecutor + per-worker Transcoders.
# Python stays single-threaded; all parallelism is managed inside Go.
#
# Sub-benchmarks are parameterised over _WORKER_COUNTS so scaling curves
# can be compared directly against the ThreadPoolExecutor results above and
# the Go benchmark results.
#
# Corpus: 5120 messages per iteration — same as TestBenchmarkMaxThroughputConcurrent.


class TestBenchmarkMaxThroughputPool:
    """Maximum-throughput benchmarks using the Go-managed Pool handle."""

    # ── AppendRaw ──────────────────────────────────────────────────────

    @pytest.mark.parametrize("workers", _WORKER_COUNTS)
    def test_pool_append_raw(self, benchmark, test_proto, hyper_type, workers):
        """Pool AppendRaw: one Pool with N Go workers processes 5120 TestMsg."""
        corpus = [
            encode_test_msg(f"user-{i % 1000}", i % 100, float(i) * 1.1, i % 2 == 0)
            for i in range(5120)
        ]

        pool = Pool.from_proto_file(
            test_proto, "TestMsg", workers=workers, hyper_type=hyper_type
        )

        def run():
            for msg in corpus:
                pool.submit(msg)
            batch = pool.flush()
            assert batch.num_rows == len(corpus)

        benchmark(run)
        pool.close()

    # ── AppendMerged ───────────────────────────────────────────────────

    @pytest.mark.parametrize("workers", _WORKER_COUNTS)
    def test_pool_append_merged(self, benchmark, test_proto, custom_proto, workers):
        """Pool AppendMerged: one Pool processes 5120 base+custom pairs."""
        base_msgs = [
            encode_test_msg(f"user-{i % 1000}", i % 100, float(i) * 1.1, i % 2 == 0)
            for i in range(5120)
        ]
        custom_msgs = [
            encode_custom_extra(1700000000 + i, f"src-{i % 10}") for i in range(5120)
        ]
        corpus = list(zip(base_msgs, custom_msgs))

        pool = Pool.from_proto_file(
            test_proto,
            "TestMsg",
            workers=workers,
            custom_proto=custom_proto,
            custom_message="CustomExtra",
        )

        def run():
            for base, custom in corpus:
                pool.submit_merged(base, custom)
            batch = pool.flush()
            assert batch.num_rows == len(corpus)

        benchmark(run)
        pool.close()

    # ── AppendDenorm ───────────────────────────────────────────────────

    @pytest.mark.parametrize("workers", _WORKER_COUNTS)
    def test_pool_append_denorm(
        self, benchmark, order_proto, order_hyper_type, workers
    ):
        """Pool AppendDenorm: one Pool processes 5120 Order messages (denorm fan-out)."""
        corpus = [
            encode_order(
                f"order-{i}",
                [(f"item-{j}", float(j) + 0.99) for j in range(3)],
                seq=i,
            )
            for i in range(5120)
        ]
        denorm_columns = ["name", "items[*].id", "items[*].price", "seq"]

        pool = Pool.from_proto_file(
            order_proto,
            "Order",
            workers=workers,
            hyper_type=order_hyper_type,
            denorm_columns=denorm_columns,
        )

        def run():
            for msg in corpus:
                pool.submit(msg)
            batch = pool.flush()
            # 5120 msgs × 3 items = 15360 denorm rows
            assert batch.num_rows == len(corpus) * 3

        benchmark(run)
        pool.close()


# ── Benchmark: Maximum-throughput concurrent — BidRequestEvent schema ────
#
# These benchmarks are the Python counterpart of:
#   Go: BenchmarkMaxThroughput_ConcurrentAppendRaw      (BidRequest, 122880-msg corpus)
#   Go: BenchmarkMaxThroughput_ConcurrentAppendRaw_TestMsg (TestMsg, 5120-msg corpus)
#
# Both Python and Go use:
#   - Schema:  BidRequestEvent — 14 top-level fields, 6 levels of nesting,
#              repeated imp[], repeated deals[], DecimalValue sub-messages,
#              Timestamp, ~50+ encoded fields per message
#   - Corpus:  5120 messages per iteration matching benchRealisticBidRequestCorpus
#              distribution (75% 2 imps, 61% 1 deal, 55% video-primary)
#   - Workers: uniqueSortedWorkerCounts fan-out (1…cpu×2)
#   - HyperType PGO warm-up performed once at fixture setup
#
# This is the apples-to-apples comparison with the Go BidRequest benchmarks.


@pytest.fixture(scope="module")
def _bid_request_corpus_5120() -> list[bytes]:
    return [encode_bid_request(i) for i in range(5120)]


class TestBenchmarkMaxThroughputConcurrent_BidRequest:
    """Maximum-throughput concurrent benchmarks — BidRequestEvent schema.

    Uses the same BidRequest corpus and worker fan-out as the Go
    BenchmarkMaxThroughput_ConcurrentAppendRaw benchmark.
    """

    @pytest.mark.parametrize("workers", _WORKER_COUNTS)
    def test_concurrent_append_raw_bidrequest(
        self,
        benchmark,
        bid_request_proto,
        bid_request_hyper_type,
        bid_request_import_paths,
        workers,
        _bid_request_corpus_5120,
    ):
        """Concurrent AppendRaw: N workers each process a shard of 5120 BidRequestEvent."""
        corpus = _bid_request_corpus_5120
        shards = [corpus[i::workers] for i in range(workers)]
        pool = [
            Transcoder.from_proto_file(
                bid_request_proto,
                "BidRequestEvent",
                import_paths=bid_request_import_paths,
                hyper_type=bid_request_hyper_type,
            )
            for _ in range(workers)
        ]

        def _process(tc, shard):
            for msg in shard:
                tc.append(msg)
            tc.flush()

        with _futures.ThreadPoolExecutor(max_workers=workers) as executor:

            def run():
                futs = [
                    executor.submit(_process, tc, shard)
                    for tc, shard in zip(pool, shards)
                ]
                for f in futs:
                    f.result()

            benchmark(run)

        for tc in pool:
            tc.close()


class TestBenchmarkMaxThroughputPool_BidRequest:
    """Maximum-throughput Pool benchmarks — BidRequestEvent schema.

    Uses the same BidRequest corpus and worker fan-out as
    TestBenchmarkMaxThroughputConcurrent_BidRequest but uses the Go-managed
    Pool handle so Python stays single-threaded.
    """

    @pytest.mark.parametrize("workers", _WORKER_COUNTS)
    def test_pool_append_raw_bidrequest(
        self,
        benchmark,
        bid_request_proto,
        bid_request_hyper_type,
        bid_request_import_paths,
        workers,
        _bid_request_corpus_5120,
    ):
        """Pool AppendRaw: one Pool with N Go workers processes 5120 BidRequestEvent."""
        corpus = _bid_request_corpus_5120

        pool = Pool.from_proto_file(
            bid_request_proto,
            "BidRequestEvent",
            import_paths=bid_request_import_paths,
            workers=workers,
            hyper_type=bid_request_hyper_type,
        )

        def run():
            for msg in corpus:
                pool.submit(msg)
            batch = pool.flush()
            assert batch.num_rows == len(corpus)

        benchmark(run)
        pool.close()
