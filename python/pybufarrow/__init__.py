"""pybufarrow — Python bindings for bufarrowlib (protobuf → Arrow/Parquet)."""

from ._ffi import BufarrowError
from .batch import (
    transcode_batch,
    transcode_merged_batch,
    transcode_to_parquet,
    transcode_to_table,
)
from .hypertype import HyperType
from .pool import Pool
from .transcoder import Transcoder

__all__ = [
    "BufarrowError",
    "HyperType",
    "Pool",
    "Transcoder",
    "transcode_batch",
    "transcode_merged_batch",
    "transcode_to_parquet",
    "transcode_to_table",
]


def version() -> str:
    """Return the libbufarrow C library version."""
    from ._ffi import _get_lib, _read_c_string

    lib = _get_lib()
    ptr = lib.BufarrowVersion()
    result = _read_c_string(ptr) or "unknown"
    if ptr:
        lib.BufarrowFreeString(ptr)
    return result
