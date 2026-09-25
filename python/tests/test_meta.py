"""Unit tests for metadata value conversion helpers."""

from __future__ import annotations

import ctypes

import pytest

from pybufarrow._meta import _to_meta_value


def test_to_meta_value_none() -> None:
    mv, keepalive = _to_meta_value(None)
    assert mv.kind == 3
    assert keepalive is None


def test_to_meta_value_bool() -> None:
    mv_true, _ = _to_meta_value(True)
    mv_false, _ = _to_meta_value(False)
    assert mv_true.kind == 0 and mv_true.i64 == 1
    assert mv_false.kind == 0 and mv_false.i64 == 0


def test_to_meta_value_int_and_float() -> None:
    mv_int, _ = _to_meta_value(42)
    mv_float, _ = _to_meta_value(3.25)
    assert mv_int.kind == 0 and mv_int.i64 == 42
    assert mv_float.kind == 1 and mv_float.f64 == 3.25


def test_to_meta_value_string_and_bytes() -> None:
    mv_str, keep_str = _to_meta_value("abc")
    mv_bytes, keep_bytes = _to_meta_value(b"xyz")

    assert mv_str.kind == 2
    assert mv_str.len == 3
    assert keep_str is not None

    assert mv_bytes.kind == 2
    assert mv_bytes.len == 3
    assert keep_bytes is not None

    assert ctypes.string_at(mv_str.ptr, mv_str.len) == b"abc"
    assert ctypes.string_at(mv_bytes.ptr, mv_bytes.len) == b"xyz"


def test_to_meta_value_unsupported_type_raises() -> None:
    with pytest.raises(TypeError):
        _to_meta_value({"bad": "type"})
