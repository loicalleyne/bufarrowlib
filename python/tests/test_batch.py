"""Tests for batch helper functions."""

from __future__ import annotations

import pyarrow as pa
import pytest

from pybufarrow import HyperType, transcode_batch, transcode_to_table

from .conftest import encode_test_msg


class TestBatchHelpers:
    """Test convenience batch transcoding functions (require HyperType)."""

    def test_transcode_batch(self, test_proto, hyper_type):
        messages = [encode_test_msg(f"U{i}", i, float(i), True) for i in range(10)]
        batches = list(transcode_batch(
            test_proto, "TestMsg", messages, batch_size=4,
            hyper_type=hyper_type,
        ))
        # 10 messages with batch_size=4 → 3 batches (4, 4, 2)
        assert len(batches) == 3
        assert batches[0].num_rows == 4
        assert batches[1].num_rows == 4
        assert batches[2].num_rows == 2

    def test_transcode_to_table(self, test_proto, hyper_type):
        messages = [encode_test_msg(f"U{i}", i, float(i), True) for i in range(5)]
        table = transcode_to_table(
            test_proto, "TestMsg", messages,
            hyper_type=hyper_type,
        )
        assert isinstance(table, pa.Table)
        assert table.num_rows == 5
        assert "name" in table.schema.names
