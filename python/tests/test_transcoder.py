"""Tests for pybufarrow Transcoder lifecycle, append, flush, schema."""

from __future__ import annotations

import pyarrow as pa
import pytest

from pybufarrow import BufarrowError, Transcoder

from .conftest import encode_test_msg


class TestTranscoderLifecycle:
    """Test creation, clone, close, context manager."""

    def test_from_proto_file(self, test_proto):
        tc = Transcoder.from_proto_file(test_proto, "TestMsg")
        assert tc is not None
        tc.close()

    def test_context_manager(self, test_proto):
        with Transcoder.from_proto_file(test_proto, "TestMsg") as tc:
            assert tc is not None
        # Should not raise on double close
        tc.close()

    def test_double_close_is_safe(self, test_proto):
        tc = Transcoder.from_proto_file(test_proto, "TestMsg")
        tc.close()
        tc.close()  # should not raise

    def test_use_after_close_raises(self, test_proto):
        tc = Transcoder.from_proto_file(test_proto, "TestMsg")
        tc.close()
        with pytest.raises(BufarrowError, match="closed"):
            tc.flush()

    def test_clone(self, test_proto):
        with Transcoder.from_proto_file(test_proto, "TestMsg") as tc:
            clone = tc.clone()
            assert clone is not None
            clone.close()

    def test_invalid_proto_raises(self, fixtures_dir):
        with pytest.raises(BufarrowError):
            Transcoder.from_proto_file(
                str(fixtures_dir / "nonexistent.proto"), "TestMsg"
            )

    def test_invalid_message_raises(self, test_proto):
        with pytest.raises(BufarrowError):
            Transcoder.from_proto_file(test_proto, "NoSuchMessage")


class TestSchema:
    """Test schema and field_names access."""

    def test_schema_type(self, test_proto):
        with Transcoder.from_proto_file(test_proto, "TestMsg") as tc:
            assert isinstance(tc.schema, pa.Schema)

    def test_schema_fields(self, test_proto):
        with Transcoder.from_proto_file(test_proto, "TestMsg") as tc:
            names = tc.schema.names
            assert "name" in names
            assert "age" in names
            assert "score" in names
            assert "active" in names

    def test_field_names(self, test_proto):
        with Transcoder.from_proto_file(test_proto, "TestMsg") as tc:
            names = tc.field_names
            assert isinstance(names, list)
            assert "name" in names
            assert "age" in names


class TestAppendFlush:
    """Test append + flush cycle (requires HyperType)."""

    def test_append_flush_single(self, test_proto, hyper_type):
        with Transcoder.from_proto_file(test_proto, "TestMsg", hyper_type=hyper_type) as tc:
            msg = encode_test_msg("Alice", 30, 95.5, True)
            tc.append(msg)
            batch = tc.flush()
            assert isinstance(batch, pa.RecordBatch)
            assert batch.num_rows == 1
            assert batch.column("name")[0].as_py() == "Alice"
            assert batch.column("age")[0].as_py() == 30

    def test_append_flush_multiple(self, test_proto, hyper_type):
        with Transcoder.from_proto_file(test_proto, "TestMsg", hyper_type=hyper_type) as tc:
            for i in range(10):
                tc.append(encode_test_msg(f"User{i}", i, float(i), i % 2 == 0))
            batch = tc.flush()
            assert batch.num_rows == 10
            assert batch.column("name")[0].as_py() == "User0"
            assert batch.column("name")[9].as_py() == "User9"

    def test_flush_resets_builder(self, test_proto, hyper_type):
        with Transcoder.from_proto_file(test_proto, "TestMsg", hyper_type=hyper_type) as tc:
            tc.append(encode_test_msg("A", 1, 1.0, False))
            batch1 = tc.flush()
            assert batch1.num_rows == 1

            tc.append(encode_test_msg("B", 2, 2.0, True))
            tc.append(encode_test_msg("C", 3, 3.0, False))
            batch2 = tc.flush()
            assert batch2.num_rows == 2

    def test_append_empty_message(self, test_proto, hyper_type):
        with Transcoder.from_proto_file(test_proto, "TestMsg", hyper_type=hyper_type) as tc:
            tc.append(b"")  # empty protobuf = all defaults
            batch = tc.flush()
            assert batch.num_rows == 1


class TestMergedAppend:
    """Test append_merged with custom fields."""

    def test_append_merged_requires_custom(self, test_proto):
        with Transcoder.from_proto_file(test_proto, "TestMsg") as tc:
            with pytest.raises(BufarrowError):
                tc.append_merged(b"", b"")

    def test_append_merged(self, test_proto, custom_proto):
        with Transcoder.from_proto_file(
            test_proto,
            "TestMsg",
            custom_proto=custom_proto,
            custom_message="CustomExtra",
        ) as tc:
            from .conftest import encode_custom_extra

            base = encode_test_msg("Alice", 30, 95.5, True)
            custom = encode_custom_extra(1234567890, "sensor-1")
            tc.append_merged(base, custom)
            batch = tc.flush()
            assert batch.num_rows == 1
            # The merged schema should have both base and custom fields
            names = batch.schema.names
            assert "name" in names
            assert "event_ts" in names
            assert "source_id" in names
