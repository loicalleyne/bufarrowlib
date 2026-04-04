"""Tests for HyperType PGO coordinator lifecycle and multi-threaded usage."""

from __future__ import annotations

import threading

import pyarrow as pa
import pytest

from pybufarrow import BufarrowError, HyperType, Transcoder

from .conftest import encode_test_msg


class TestHyperTypeLifecycle:
    """Test HyperType creation, close, and context manager."""

    def test_create(self, test_proto):
        ht = HyperType(test_proto, "TestMsg")
        assert ht is not None
        ht.close()

    def test_context_manager(self, test_proto):
        with HyperType(test_proto, "TestMsg") as ht:
            assert ht is not None

    def test_double_close_safe(self, test_proto):
        ht = HyperType(test_proto, "TestMsg")
        ht.close()
        ht.close()  # should not raise

    def test_invalid_proto_raises(self, fixtures_dir):
        with pytest.raises(BufarrowError):
            HyperType(str(fixtures_dir / "nonexistent.proto"), "Msg")

    def test_invalid_message_raises(self, test_proto):
        with pytest.raises(BufarrowError):
            HyperType(test_proto, "NoSuchMessage")

    def test_with_auto_recompile(self, test_proto):
        with HyperType(
            test_proto, "TestMsg",
            auto_recompile_threshold=100,
            sample_rate=0.1,
        ) as ht:
            tc = Transcoder.from_proto_file(
                test_proto, "TestMsg", hyper_type=ht
            )
            tc.append(encode_test_msg("test", 1, 1.0, True))
            batch = tc.flush()
            assert batch.num_rows == 1
            tc.close()


class TestHyperTypeShared:
    """Test sharing a HyperType across multiple Transcoders."""

    def test_shared_hyper_type(self, test_proto):
        """Two transcoders sharing the same HyperType."""
        with HyperType(test_proto, "TestMsg") as ht:
            with Transcoder.from_proto_file(
                test_proto, "TestMsg", hyper_type=ht
            ) as tc1:
                with Transcoder.from_proto_file(
                    test_proto, "TestMsg", hyper_type=ht
                ) as tc2:
                    tc1.append(encode_test_msg("A", 1, 1.0, True))
                    tc2.append(encode_test_msg("B", 2, 2.0, False))
                    b1 = tc1.flush()
                    b2 = tc2.flush()
                    assert b1.num_rows == 1
                    assert b2.num_rows == 1
                    assert b1.column("name")[0].as_py() == "A"
                    assert b2.column("name")[0].as_py() == "B"

    def test_threaded_append(self, test_proto):
        """Two threads each with own Transcoder sharing one HyperType."""
        results = {}
        errors = []

        def worker(name, ht, proto):
            try:
                with Transcoder.from_proto_file(
                    proto, "TestMsg", hyper_type=ht
                ) as tc:
                    for i in range(50):
                        tc.append(encode_test_msg(f"{name}-{i}", i, float(i), True))
                    batch = tc.flush()
                    results[name] = batch.num_rows
            except Exception as e:
                errors.append(e)

        with HyperType(test_proto, "TestMsg") as ht:
            t1 = threading.Thread(target=worker, args=("t1", ht, test_proto))
            t2 = threading.Thread(target=worker, args=("t2", ht, test_proto))
            t1.start()
            t2.start()
            t1.join()
            t2.join()

        assert not errors, f"Thread errors: {errors}"
        assert results["t1"] == 50
        assert results["t2"] == 50
