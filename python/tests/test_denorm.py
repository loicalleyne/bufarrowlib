"""Tests for denormalization — YAML config and programmatic denorm_columns."""

from __future__ import annotations

import pyarrow as pa
import pytest

from pybufarrow import BufarrowError, Transcoder

from .conftest import encode_order


class TestDenormFromConfig:
    """Test denormalization via YAML config file."""

    def test_config_creates_transcoder(self, denorm_config):
        with Transcoder.from_config(denorm_config) as tc:
            assert tc is not None

    def test_config_denorm_schema(self, denorm_config):
        with Transcoder.from_config(denorm_config) as tc:
            ds = tc.denorm_schema
            assert isinstance(ds, pa.Schema)
            assert "order_name" in ds.names
            assert "item_id" in ds.names
            assert "item_price" in ds.names
            assert "order_seq" in ds.names

    def test_config_denorm_fanout(self, denorm_config):
        """Denorm with repeated field fan-out: one row per item."""
        with Transcoder.from_config(denorm_config) as tc:
            msg = encode_order("order-1", [("A", 1.50), ("B", 2.75)], seq=42)
            tc.append_denorm(msg)
            batch = tc.flush_denorm()
            assert batch.num_rows == 2
            assert batch.column("order_name")[0].as_py() == "order-1"
            assert batch.column("order_name")[1].as_py() == "order-1"
            assert batch.column("item_id")[0].as_py() == "A"
            assert batch.column("item_id")[1].as_py() == "B"
            assert batch.column("item_price")[0].as_py() == pytest.approx(1.50)
            assert batch.column("item_price")[1].as_py() == pytest.approx(2.75)
            assert batch.column("order_seq")[0].as_py() == 42
            assert batch.column("order_seq")[1].as_py() == 42

    def test_config_denorm_multiple_messages(self, denorm_config):
        """Append multiple messages, flush once."""
        with Transcoder.from_config(denorm_config) as tc:
            tc.append_denorm(encode_order("o1", [("X", 10.0)], seq=1))
            tc.append_denorm(encode_order("o2", [("Y", 20.0), ("Z", 30.0)], seq=2))
            batch = tc.flush_denorm()
            # o1 has 1 item, o2 has 2 items → 3 rows total
            assert batch.num_rows == 3

    def test_config_denorm_empty_items(self, denorm_config):
        """A message with no repeated items produces 1 row with null fan-out fields (left-join)."""
        with Transcoder.from_config(denorm_config) as tc:
            tc.append_denorm(encode_order("empty", [], seq=99))
            batch = tc.flush_denorm()
            # Left-join semantics: empty repeated fields produce 1 null row
            assert batch.num_rows == 1
            assert batch.column("order_name")[0].as_py() == "empty"
            assert batch.column("item_id")[0].as_py() is None
            assert batch.column("item_price")[0].as_py() is None
            assert batch.column("order_seq")[0].as_py() == 99


class TestDenormFromConfigString:
    """Test denormalization via YAML config string."""

    def test_config_string(self, order_proto):
        yaml_str = f"""\
proto:
  file: {order_proto}
  message: Order
denormalizer:
  columns:
    - name: order_name
      path: name
    - name: item_id
      path: items[*].id
"""
        with Transcoder.from_config_string(yaml_str) as tc:
            msg = encode_order("hello", [("i1", 5.0)])
            tc.append_denorm(msg)
            batch = tc.flush_denorm()
            assert batch.num_rows == 1
            assert batch.column("order_name")[0].as_py() == "hello"
            assert batch.column("item_id")[0].as_py() == "i1"


class TestDenormColumns:
    """Test programmatic denorm_columns parameter."""

    def test_denorm_columns_basic(self, order_proto, order_hyper_type):
        with Transcoder.from_proto_file(
            order_proto,
            "Order",
            denorm_columns=["name", "items[*].id", "items[*].price"],
            hyper_type=order_hyper_type,
        ) as tc:
            ds = tc.denorm_schema
            assert "name" in ds.names
            assert "id" in ds.names
            assert "price" in ds.names

    def test_denorm_columns_fanout(self, order_proto, order_hyper_type):
        with Transcoder.from_proto_file(
            order_proto,
            "Order",
            denorm_columns=["name", "items[*].id", "items[*].price", "seq"],
            hyper_type=order_hyper_type,
        ) as tc:
            msg = encode_order("test", [("A", 1.0), ("B", 2.0)], seq=7)
            tc.append_denorm(msg)
            batch = tc.flush_denorm()
            assert batch.num_rows == 2
            assert batch.column("name")[0].as_py() == "test"
            assert batch.column("id")[0].as_py() == "A"
            assert batch.column("id")[1].as_py() == "B"

    def test_denorm_without_plan_raises(self, order_proto, order_hyper_type):
        """append_denorm without denorm plan should raise."""
        with Transcoder.from_proto_file(
            order_proto, "Order", hyper_type=order_hyper_type
        ) as tc:
            with pytest.raises(BufarrowError):
                tc.append_denorm(encode_order("x", [("a", 1.0)]))
