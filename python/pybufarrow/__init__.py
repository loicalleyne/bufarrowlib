"""pybufarrow — Python bindings for bufarrowlib (protobuf → Arrow/Parquet)."""

from ._ffi import BufarrowError, CyclicTypeError
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
    "CyclicTypeError",
    "HyperType",
    "Pool",
    "Transcoder",
    "cyclic_types",
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


def cyclic_types(
    proto_path: str,
    message_name: str,
    import_paths: list[str] | None = None,
) -> list[str]:
    """Return every cyclic message type reachable from ``message_name``.

    The names are fully qualified and sorted, and the list is empty if the
    schema has no cycles. Pass it to ``json_termination`` so the constructor
    succeeds::

        names = pybufarrow.cyclic_types("tree.proto", "Tree", ["protos"])
        tc = Transcoder.from_proto_file(
            "tree.proto", "Tree", ["protos"], json_termination=names
        )

    For a two-type cycle such as ``A -> B -> A``, only ``A`` is reported,
    because the cycle closes when ``A`` is re-entered.

    Raises
    ------
    BufarrowError
        If the file does not compile or the message is not found.
    """
    import ctypes
    import json

    from ._ffi import _check_global, _encode, _get_lib, _make_import_paths, _read_c_string

    lib = _get_lib()
    paths_arr, n_paths = _make_import_paths(import_paths)
    out = ctypes.c_void_p()
    status = lib.BufarrowCyclicTypes(
        _encode(proto_path),
        _encode(message_name),
        paths_arr,
        n_paths,
        ctypes.byref(out),
    )
    _check_global(status)
    raw = _read_c_string(out.value)
    if out.value:
        lib.BufarrowFreeString(out.value)
    return json.loads(raw) if raw else []
