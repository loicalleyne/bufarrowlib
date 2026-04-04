"""Correctness tests for pybufarrow.Pool (Go-managed concurrent ingestion)."""

from __future__ import annotations

import pytest
import pyarrow as pa

from pybufarrow import Pool, BufarrowError

from .conftest import (
    encode_test_msg,
    encode_order,
    encode_custom_extra,
)


# ── helpers ──────────────────────────────────────────────────────────────────


def make_pool(
    test_proto: str,
    *,
    workers: int = 2,
    hyper_type=None,
    denorm_columns: list[str] | None = None,
    custom_proto: str | None = None,
    custom_message: str | None = None,
) -> Pool:
    return Pool.from_proto_file(
        test_proto,
        "TestMsg",
        workers=workers,
        hyper_type=hyper_type,
        denorm_columns=denorm_columns,
        custom_proto=custom_proto,
        custom_message=custom_message,
    )


def make_order_pool(
    order_proto: str,
    *,
    workers: int = 2,
    hyper_type=None,
) -> Pool:
    return Pool.from_proto_file(
        order_proto,
        "Order",
        workers=workers,
        hyper_type=hyper_type,
        denorm_columns=["name", "items[*].id", "items[*].price", "seq"],
    )


# ── lifecycle ─────────────────────────────────────────────────────────────────


class TestPoolLifecycle:
    def test_context_manager(self, test_proto, hyper_type):
        """Pool can be used as a context manager; close is called on exit."""
        with Pool.from_proto_file(test_proto, "TestMsg", hyper_type=hyper_type) as pool:
            assert not pool._closed
        assert pool._closed

    def test_close_idempotent(self, test_proto, hyper_type):
        """Calling close() twice does not raise."""
        pool = Pool.from_proto_file(test_proto, "TestMsg", hyper_type=hyper_type)
        pool.close()
        pool.close()  # must not raise

    def test_submit_after_close_raises(self, test_proto, hyper_type):
        """submit() on a closed pool raises BufarrowError."""
        pool = Pool.from_proto_file(test_proto, "TestMsg", hyper_type=hyper_type)
        pool.close()
        with pytest.raises(BufarrowError):
            pool.submit(encode_test_msg("x", 1, 0.0, False))

    def test_flush_after_close_raises(self, test_proto, hyper_type):
        """flush() on a closed pool raises BufarrowError."""
        pool = Pool.from_proto_file(test_proto, "TestMsg", hyper_type=hyper_type)
        pool.close()
        with pytest.raises(BufarrowError):
            pool.flush()

    def test_repr(self, test_proto, hyper_type):
        """Pool repr includes closed=False."""
        pool = Pool.from_proto_file(test_proto, "TestMsg", hyper_type=hyper_type)
        r = repr(pool)
        assert "Pool" in r
        pool.close()


# ── basic submit + flush ──────────────────────────────────────────────────────


class TestPoolSubmitFlush:
    def test_flush_empty(self, test_proto, hyper_type):
        """Flush with no messages returns a 0-row batch with valid schema."""
        with Pool.from_proto_file(
            test_proto, "TestMsg", workers=2, hyper_type=hyper_type
        ) as pool:
            batch = pool.flush()
            assert isinstance(batch, pa.RecordBatch)
            assert batch.num_rows == 0
            assert batch.schema is not None

    def test_known_row_count(self, test_proto, hyper_type):
        """N submitted messages produce N rows after flush."""
        n = 100
        with Pool.from_proto_file(
            test_proto, "TestMsg", workers=4, hyper_type=hyper_type
        ) as pool:
            for i in range(n):
                pool.submit(encode_test_msg(f"user-{i}", i % 50, float(i), i % 2 == 0))
            batch = pool.flush()
            assert batch.num_rows == n

    def test_multiple_flush_windows(self, test_proto, hyper_type):
        """Each flush window produces the correct row count."""
        n_per_window = 50
        windows = 4
        with Pool.from_proto_file(
            test_proto, "TestMsg", workers=2, hyper_type=hyper_type
        ) as pool:
            for w in range(windows):
                for i in range(n_per_window):
                    pool.submit(encode_test_msg(f"w{w}-u{i}", i, float(i), True))
                batch = pool.flush()
                assert batch.num_rows == n_per_window, f"window {w}"

    def test_schema_preserved(self, test_proto, hyper_type):
        """Flushed batch has the expected column names."""
        with Pool.from_proto_file(
            test_proto, "TestMsg", workers=2, hyper_type=hyper_type
        ) as pool:
            pool.submit(encode_test_msg("alice", 30, 99.9, True))
            batch = pool.flush()
            names = batch.schema.names
            # TestMsg fields: name, age, score, active
            assert "name" in names
            assert "age" in names

    def test_single_worker(self, test_proto, hyper_type):
        """Single worker pool produces correct row count."""
        n = 30
        with Pool.from_proto_file(
            test_proto, "TestMsg", workers=1, hyper_type=hyper_type
        ) as pool:
            for i in range(n):
                pool.submit(encode_test_msg(f"u{i}", i, float(i), i % 2 == 0))
            batch = pool.flush()
            assert batch.num_rows == n

    def test_many_workers(self, test_proto, hyper_type):
        """Pool with more workers than messages still produces correct row count."""
        n = 10
        with Pool.from_proto_file(
            test_proto, "TestMsg", workers=16, hyper_type=hyper_type
        ) as pool:
            for i in range(n):
                pool.submit(encode_test_msg(f"u{i}", i + 1, 0.0, False))
            batch = pool.flush()
            assert batch.num_rows == n

    def test_pending_count(self, test_proto, hyper_type):
        """pending() returns a non-negative integer."""
        with Pool.from_proto_file(
            test_proto, "TestMsg", workers=2, capacity=1024, hyper_type=hyper_type
        ) as pool:
            p = pool.pending()
            assert isinstance(p, int)
            assert p >= 0
            for i in range(20):
                pool.submit(encode_test_msg(f"u{i}", i, 0.0, True))
            pool.flush()
            assert pool.pending() == 0


# ── submit_merged ─────────────────────────────────────────────────────────────


class TestPoolSubmitMerged:
    def test_merged_row_count(self, test_proto, custom_proto):
        """submit_merged produces correct row count."""
        n = 50
        with Pool.from_proto_file(
            test_proto,
            "TestMsg",
            workers=2,
            custom_proto=custom_proto,
            custom_message="CustomExtra",
        ) as pool:
            for i in range(n):
                base = encode_test_msg(f"u{i}", i, float(i), True)
                custom = encode_custom_extra(1700000000 + i, f"src-{i % 5}")
                pool.submit_merged(base, custom)
            batch = pool.flush()
            assert batch.num_rows == n


# ── denorm pool path ──────────────────────────────────────────────────────────


class TestPoolDenorm:
    def test_denorm_row_count(self, order_proto, order_hyper_type):
        """Denorm pool: N orders × K items = N×K rows after flush."""
        n_orders = 10
        items_per_order = 3
        with Pool.from_proto_file(
            order_proto,
            "Order",
            workers=2,
            hyper_type=order_hyper_type,
            denorm_columns=["name", "items[*].id", "items[*].price", "seq"],
        ) as pool:
            for i in range(n_orders):
                pool.submit(
                    encode_order(
                        f"order-{i}",
                        [(f"item-{j}", float(j) + 0.99) for j in range(items_per_order)],
                        seq=i,
                    )
                )
            batch = pool.flush()
            assert batch.num_rows == n_orders * items_per_order

    def test_denorm_multiple_windows(self, order_proto, order_hyper_type):
        """Denorm pool correctly resets between flush windows."""
        n_orders = 5
        items_per_order = 3
        windows = 3
        with Pool.from_proto_file(
            order_proto,
            "Order",
            workers=2,
            hyper_type=order_hyper_type,
            denorm_columns=["name", "items[*].id", "items[*].price", "seq"],
        ) as pool:
            for w in range(windows):
                for i in range(n_orders):
                    pool.submit(
                        encode_order(
                            f"w{w}-order-{i}",
                            [(f"item-{j}", float(j)) for j in range(items_per_order)],
                            seq=i,
                        )
                    )
                batch = pool.flush()
                assert batch.num_rows == n_orders * items_per_order, f"window {w}"

    def test_denorm_schema(self, order_proto, order_hyper_type):
        """Denorm batch has expected column names."""
        with Pool.from_proto_file(
            order_proto,
            "Order",
            workers=2,
            hyper_type=order_hyper_type,
            denorm_columns=["name", "items[*].id", "items[*].price", "seq"],
        ) as pool:
            pool.submit(
                encode_order("ord-0", [("item-0", 1.0)], seq=0)
            )
            batch = pool.flush()
            # Column names should include the last segments of the paths
            names = set(batch.schema.names)
            assert "name" in names
