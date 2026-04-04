"""Tests for Parquet write/read round-trip."""

from __future__ import annotations

import tempfile
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet
import pytest

from pybufarrow import Transcoder

from .conftest import encode_test_msg


class TestParquet:
    """Test Parquet write and read operations (require HyperType)."""

    def test_write_parquet(self, test_proto, hyper_type, tmp_path):
        pq_path = str(tmp_path / "out.parquet")
        with Transcoder.from_proto_file(test_proto, "TestMsg", hyper_type=hyper_type) as tc:
            tc.append(encode_test_msg("Alice", 30, 95.5, True))
            tc.append(encode_test_msg("Bob", 25, 88.0, False))
            tc.write_parquet(pq_path)

        # Verify with pyarrow directly
        table = pa.parquet.read_table(pq_path)
        assert table.num_rows == 2
        assert "name" in table.schema.names

    def test_read_parquet(self, test_proto, hyper_type, tmp_path):
        pq_path = str(tmp_path / "roundtrip.parquet")

        # Write
        with Transcoder.from_proto_file(test_proto, "TestMsg", hyper_type=hyper_type) as tc:
            for i in range(5):
                tc.append(encode_test_msg(f"User{i}", i + 20, float(i), i % 2 == 0))
            tc.write_parquet(pq_path)

        # Read back
        with Transcoder.from_proto_file(test_proto, "TestMsg", hyper_type=hyper_type) as tc:
            batch = tc.read_parquet(pq_path)
            assert isinstance(batch, pa.RecordBatch)
            assert batch.num_rows == 5
            assert batch.column("name")[0].as_py() == "User0"

    def test_read_parquet_selected_columns(self, test_proto, hyper_type, tmp_path):
        pq_path = str(tmp_path / "cols.parquet")

        with Transcoder.from_proto_file(test_proto, "TestMsg", hyper_type=hyper_type) as tc:
            tc.append(encode_test_msg("Alice", 30, 95.5, True))
            tc.write_parquet(pq_path)

        with Transcoder.from_proto_file(test_proto, "TestMsg", hyper_type=hyper_type) as tc:
            batch = tc.read_parquet(pq_path, columns=[0, 1])
            assert batch.num_columns == 2
