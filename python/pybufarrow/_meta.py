"""Metadata value conversion helpers for C API calls."""

from __future__ import annotations

import ctypes
from typing import Any

from ._ffi import BufarrowMetaValue


def _to_meta_value(value: Any) -> tuple[BufarrowMetaValue, ctypes.Array[ctypes.c_char] | None]:
    """Convert a Python metadata value to a BufarrowMetaValue.

    Timestamp metadata must be supplied as an int containing epoch units
    matching the declared column type (milliseconds or microseconds).
    """
    if value is None:
        return BufarrowMetaValue(kind=3, i64=0, f64=0.0, ptr=None, len=0), None
    if isinstance(value, bool):
        return BufarrowMetaValue(kind=0, i64=1 if value else 0, f64=0.0, ptr=None, len=0), None
    if isinstance(value, int):
        return BufarrowMetaValue(kind=0, i64=value, f64=0.0, ptr=None, len=0), None
    if isinstance(value, float):
        return BufarrowMetaValue(kind=1, i64=0, f64=value, ptr=None, len=0), None
    if isinstance(value, str):
        raw = value.encode("utf-8")
        buf = ctypes.create_string_buffer(raw)
        return (
            BufarrowMetaValue(
                kind=2,
                i64=0,
                f64=0.0,
                ptr=ctypes.cast(buf, ctypes.c_char_p),
                len=len(raw),
            ),
            buf,
        )
    if isinstance(value, bytes):
        buf = ctypes.create_string_buffer(value)
        return (
            BufarrowMetaValue(
                kind=2,
                i64=0,
                f64=0.0,
                ptr=ctypes.cast(buf, ctypes.c_char_p),
                len=len(value),
            ),
            buf,
        )
    raise TypeError(f"unsupported metadata value type: {type(value)!r}")
