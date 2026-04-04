"""Tests for error handling — invalid inputs should raise clean Python exceptions."""

from __future__ import annotations

import pytest

from pybufarrow import BufarrowError, Transcoder

from .conftest import encode_test_msg


class TestErrors:
    """Test that error conditions raise BufarrowError."""

    def test_invalid_proto_file(self, fixtures_dir):
        with pytest.raises(BufarrowError):
            Transcoder.from_proto_file(
                str(fixtures_dir / "does_not_exist.proto"), "Msg"
            )

    def test_invalid_message_name(self, test_proto):
        with pytest.raises(BufarrowError):
            Transcoder.from_proto_file(test_proto, "NonExistentMessage")

    def test_invalid_config_file(self):
        with pytest.raises(BufarrowError):
            Transcoder.from_config("/tmp/no_such_config.yaml")

    def test_append_raw_without_hypertype(self, test_proto):
        """AppendRaw requires HyperType. Without it, should raise."""
        with Transcoder.from_proto_file(test_proto, "TestMsg") as tc:
            with pytest.raises(BufarrowError):
                tc.append(encode_test_msg("test", 1, 1.0, True))

    def test_append_denorm_without_plan(self, test_proto):
        """AppendDenormRaw requires a denorm plan. Without it, should raise."""
        with Transcoder.from_proto_file(test_proto, "TestMsg") as tc:
            with pytest.raises(BufarrowError):
                tc.append_denorm(encode_test_msg("test", 1, 1.0, True))

    def test_append_merged_without_custom(self, test_proto):
        """AppendRawMerged requires custom message configuration."""
        with Transcoder.from_proto_file(test_proto, "TestMsg") as tc:
            with pytest.raises(BufarrowError):
                tc.append_merged(b"\x0a\x04test", b"\x08\x01")

    def test_read_parquet_nonexistent(self, test_proto):
        with Transcoder.from_proto_file(test_proto, "TestMsg") as tc:
            with pytest.raises(BufarrowError):
                tc.read_parquet("/tmp/no_such.parquet")

    def test_version(self):
        from pybufarrow import version

        v = version()
        assert isinstance(v, str)
        assert len(v) > 0
