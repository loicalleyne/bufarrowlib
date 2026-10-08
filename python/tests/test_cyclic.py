"""Tests for cyclic message types: json_termination, cyclic_types, CyclicTypeError."""

from __future__ import annotations

import json

import pyarrow as pa
import pytest

import pybufarrow
from pybufarrow import BufarrowError, CyclicTypeError, HyperType, Pool, Transcoder

from .conftest import _encode_varint


@pytest.fixture
def cyclic_proto(fixtures_dir) -> str:
    return str(fixtures_dir / "cyclic.proto")


def _len_field(num: int, payload: bytes) -> bytes:
    return _encode_varint((num << 3) | 2) + _encode_varint(len(payload)) + payload


def encode_node(name: str, *children: bytes) -> bytes:
    """Encode Node{name, children} by hand; children are encoded Nodes."""
    out = _len_field(1, name.encode())
    for c in children:
        out += _len_field(2, c)
    return out


class TestCyclicTypeError:
    def test_raised_with_type_and_path(self, cyclic_proto):
        with pytest.raises(CyclicTypeError) as exc:
            Transcoder.from_proto_file(cyclic_proto, "Node")
        assert exc.value.type == "Node"
        assert exc.value.path == "children"
        assert "cyclic message type" in str(exc.value)

    def test_is_bufarrow_error(self, cyclic_proto):
        with pytest.raises(BufarrowError):
            Transcoder.from_proto_file(cyclic_proto, "Node")

    def test_pool_raises_too(self, cyclic_proto):
        with pytest.raises(CyclicTypeError):
            Pool.from_proto_file(cyclic_proto, "Node")

    def test_other_errors_are_not_cyclic(self, cyclic_proto):
        with pytest.raises(BufarrowError) as exc:
            Transcoder.from_proto_file(cyclic_proto, "Missing")
        assert not isinstance(exc.value, CyclicTypeError)

    def test_retry_loop_converges(self, cyclic_proto):
        names: list[str] = []
        for _ in range(10):
            try:
                tc = Transcoder.from_proto_file(cyclic_proto, "Tree", json_termination=names)
            except CyclicTypeError as e:
                names.append(e.type)
                continue
            tc.close()
            break
        else:
            pytest.fail(f"retry loop did not converge: {names}")
        assert sorted(names) == ["Node", "Ping"]


class TestCyclicTypes:
    def test_lists_every_cycle(self, cyclic_proto):
        assert pybufarrow.cyclic_types(cyclic_proto, "Tree") == ["Node", "Ping"]

    def test_none(self, cyclic_proto):
        assert pybufarrow.cyclic_types(cyclic_proto, "Flat") == []

    def test_error(self, cyclic_proto):
        with pytest.raises(BufarrowError):
            pybufarrow.cyclic_types(cyclic_proto, "Missing")

    def test_result_makes_constructor_succeed(self, cyclic_proto):
        names = pybufarrow.cyclic_types(cyclic_proto, "Tree")
        with Transcoder.from_proto_file(cyclic_proto, "Tree", json_termination=names) as tc:
            with Transcoder.from_proto_file(cyclic_proto, "Tree", json_termination_auto=True) as auto:
                assert tc.schema == auto.schema


class TestJSONTermination:
    def test_schema(self, cyclic_proto):
        with Transcoder.from_proto_file(cyclic_proto, "Node", json_termination=["Node"]) as tc:
            assert tc.schema.field("name").type == pa.string()
            assert tc.schema.field("children").type.value_type == pa.string()

    def test_round_trip_values(self, cyclic_proto):
        ht = HyperType(cyclic_proto, "Node")
        try:
            with Transcoder.from_proto_file(
                cyclic_proto, "Node", json_termination=["Node"], hyper_type=ht
            ) as tc:
                tc.append(encode_node("root", encode_node("c1", encode_node("gc")), encode_node("c2")))
                batch = tc.flush()
        finally:
            ht.close()
        row = batch.to_pylist()[0]
        assert row["name"] == "root"
        # Compare decoded JSON, never the raw text: protojson is not byte-stable.
        children = [json.loads(c) for c in row["children"]]
        assert children == [
            {"name": "c1", "children": [{"name": "gc"}]},
            {"name": "c2"},
        ]

    def test_auto(self, cyclic_proto):
        with Transcoder.from_proto_file(cyclic_proto, "Tree", json_termination_auto=True) as tc:
            assert "root" in tc.field_names

    def test_pool_accepts_option(self, cyclic_proto):
        with Pool.from_proto_file(cyclic_proto, "Node", json_termination=["Node"]) as pool:
            assert pool is not None


class TestOptsValidation:
    def test_unknown_key_raises(self, test_proto):
        with pytest.raises(BufarrowError, match="json_terminaton"):
            Transcoder.from_proto_file(test_proto, "TestMsg", opts={"json_terminaton": ["X"]})

    def test_bool_options(self, test_proto):
        with Transcoder.from_proto_file(
            test_proto, "TestMsg", well_known_types=True, prune_empty_messages=True
        ) as tc:
            assert tc.field_names
