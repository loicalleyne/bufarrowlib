"""Low-level ctypes FFI layer for libbufarrow shared library."""

from __future__ import annotations

import ctypes
import ctypes.util
import os
import sys
from pathlib import Path


class BufarrowError(Exception):
    """Error raised by the bufarrow C library."""


# ── Arrow C Data Interface ABI structs ──────────────────────────────────


class ArrowSchema(ctypes.Structure):
    pass


ArrowSchema._fields_ = [
    ("format", ctypes.c_char_p),
    ("name", ctypes.c_char_p),
    ("metadata", ctypes.c_char_p),
    ("flags", ctypes.c_int64),
    ("n_children", ctypes.c_int64),
    ("children", ctypes.POINTER(ctypes.POINTER(ArrowSchema))),
    ("dictionary", ctypes.POINTER(ArrowSchema)),
    ("release", ctypes.CFUNCTYPE(None, ctypes.POINTER(ArrowSchema))),
    ("private_data", ctypes.c_void_p),
]


class ArrowArray(ctypes.Structure):
    pass


ArrowArray._fields_ = [
    ("length", ctypes.c_int64),
    ("null_count", ctypes.c_int64),
    ("offset", ctypes.c_int64),
    ("n_buffers", ctypes.c_int64),
    ("n_children", ctypes.c_int64),
    ("buffers", ctypes.POINTER(ctypes.c_void_p)),
    ("children", ctypes.POINTER(ctypes.POINTER(ArrowArray))),
    ("dictionary", ctypes.POINTER(ArrowArray)),
    ("release", ctypes.CFUNCTYPE(None, ctypes.POINTER(ArrowArray))),
    ("private_data", ctypes.c_void_p),
]


# ── Library loader ──────────────────────────────────────────────────────


def _find_library() -> str:
    """Locate libbufarrow shared library."""
    # 1. Check next to this file (wheel / editable install)
    pkg_dir = Path(__file__).parent
    candidates: list[Path] = []
    if sys.platform == "linux":
        candidates.append(pkg_dir / "libbufarrow.so")
    elif sys.platform == "darwin":
        candidates.append(pkg_dir / "libbufarrow.dylib")

    for p in candidates:
        if p.exists():
            return str(p)

    # 2. Environment variable override
    env = os.environ.get("BUFARROW_LIB")
    if env and os.path.isfile(env):
        return env

    # 3. System search via ctypes
    found = ctypes.util.find_library("bufarrow")
    if found:
        return found

    raise OSError(
        "Cannot find libbufarrow shared library. Set BUFARROW_LIB or "
        "place libbufarrow.so / libbufarrow.dylib next to the pybufarrow package."
    )


_lib: ctypes.CDLL | None = None


def _get_lib() -> ctypes.CDLL:
    global _lib
    if _lib is None:
        _lib = ctypes.CDLL(_find_library())
        _declare_signatures(_lib)
    return _lib


# ── C function signatures ───────────────────────────────────────────────

_HANDLE = ctypes.c_void_p
_HANDLE_PTR = ctypes.POINTER(ctypes.c_void_p)


def _declare_signatures(lib: ctypes.CDLL) -> None:
    # Lifecycle
    lib.BufarrowNewFromFile.argtypes = [
        ctypes.c_char_p,  # proto_path
        ctypes.c_char_p,  # msg_name
        ctypes.POINTER(ctypes.c_char_p),  # import_paths
        ctypes.c_int,  # n_paths
        ctypes.c_char_p,  # opts_json
        _HANDLE_PTR,  # out_handle
    ]
    lib.BufarrowNewFromFile.restype = ctypes.c_int

    lib.BufarrowNewFromConfig.argtypes = [ctypes.c_char_p, _HANDLE_PTR]
    lib.BufarrowNewFromConfig.restype = ctypes.c_int

    lib.BufarrowNewFromConfigString.argtypes = [
        ctypes.c_char_p,  # config_yaml
        ctypes.c_int,     # config_len
        _HANDLE_PTR,      # out_handle
    ]
    lib.BufarrowNewFromConfigString.restype = ctypes.c_int

    lib.BufarrowClone.argtypes = [_HANDLE, _HANDLE_PTR]
    lib.BufarrowClone.restype = ctypes.c_int

    lib.BufarrowFree.argtypes = [_HANDLE]
    lib.BufarrowFree.restype = None

    # Ingestion
    lib.BufarrowAppendRaw.argtypes = [_HANDLE, ctypes.c_void_p, ctypes.c_int]
    lib.BufarrowAppendRaw.restype = ctypes.c_int

    lib.BufarrowAppendDenormRaw.argtypes = [_HANDLE, ctypes.c_void_p, ctypes.c_int]
    lib.BufarrowAppendDenormRaw.restype = ctypes.c_int

    lib.BufarrowAppendRawMerged.argtypes = [
        _HANDLE,
        ctypes.c_void_p, ctypes.c_int,  # base
        ctypes.c_void_p, ctypes.c_int,  # custom
    ]
    lib.BufarrowAppendRawMerged.restype = ctypes.c_int

    lib.BufarrowAppendDenormRawMerged.argtypes = [
        _HANDLE,
        ctypes.c_void_p, ctypes.c_int,
        ctypes.c_void_p, ctypes.c_int,
    ]
    lib.BufarrowAppendDenormRawMerged.restype = ctypes.c_int

    # Flush
    lib.BufarrowFlush.argtypes = [
        _HANDLE,
        ctypes.POINTER(ArrowArray),
        ctypes.POINTER(ArrowSchema),
    ]
    lib.BufarrowFlush.restype = ctypes.c_int

    lib.BufarrowFlushDenorm.argtypes = [
        _HANDLE,
        ctypes.POINTER(ArrowArray),
        ctypes.POINTER(ArrowSchema),
    ]
    lib.BufarrowFlushDenorm.restype = ctypes.c_int

    # Schema
    lib.BufarrowGetSchema.argtypes = [_HANDLE, ctypes.POINTER(ArrowSchema)]
    lib.BufarrowGetSchema.restype = ctypes.c_int

    lib.BufarrowGetDenormSchema.argtypes = [_HANDLE, ctypes.POINTER(ArrowSchema)]
    lib.BufarrowGetDenormSchema.restype = ctypes.c_int

    # Parquet
    lib.BufarrowWriteParquet.argtypes = [_HANDLE, ctypes.c_char_p]
    lib.BufarrowWriteParquet.restype = ctypes.c_int

    lib.BufarrowWriteParquetDenorm.argtypes = [_HANDLE, ctypes.c_char_p]
    lib.BufarrowWriteParquetDenorm.restype = ctypes.c_int

    lib.BufarrowReadParquet.argtypes = [
        _HANDLE,
        ctypes.c_char_p,  # file_path
        ctypes.c_char_p,  # columns_json
        ctypes.POINTER(ArrowArray),
        ctypes.POINTER(ArrowSchema),
    ]
    lib.BufarrowReadParquet.restype = ctypes.c_int

    # Info — use c_void_p (not c_char_p) to retain the raw C pointer
    # so we can free it with BufarrowFreeString after reading the string.
    lib.BufarrowFieldNames.argtypes = [_HANDLE]
    lib.BufarrowFieldNames.restype = ctypes.c_void_p

    lib.BufarrowLastError.argtypes = [_HANDLE]
    lib.BufarrowLastError.restype = ctypes.c_void_p

    lib.BufarrowFreeString.argtypes = [ctypes.c_void_p]
    lib.BufarrowFreeString.restype = None

    lib.BufarrowVersion.argtypes = []
    lib.BufarrowVersion.restype = ctypes.c_void_p

    lib.BufarrowGetGlobalError.argtypes = []
    lib.BufarrowGetGlobalError.restype = ctypes.c_void_p

    # HyperType
    lib.BufarrowNewHyperType.argtypes = [
        ctypes.c_char_p,
        ctypes.c_char_p,
        ctypes.POINTER(ctypes.c_char_p),
        ctypes.c_int,
        ctypes.c_int64,
        ctypes.c_double,
        _HANDLE_PTR,
    ]
    lib.BufarrowNewHyperType.restype = ctypes.c_int

    lib.BufarrowFreeHyperType.argtypes = [_HANDLE]
    lib.BufarrowFreeHyperType.restype = None

    lib.BufarrowNewFromFileWithHyperType.argtypes = [
        ctypes.c_char_p,
        ctypes.c_char_p,
        ctypes.POINTER(ctypes.c_char_p),
        ctypes.c_int,
        ctypes.c_char_p,
        _HANDLE,  # hyper_handle
        _HANDLE_PTR,  # out_handle
    ]
    lib.BufarrowNewFromFileWithHyperType.restype = ctypes.c_int

    # Pool — Go-managed concurrent ingestion
    lib.BufarrowPoolNew.argtypes = [
        ctypes.c_char_p,  # proto_path
        ctypes.c_char_p,  # msg_name
        ctypes.POINTER(ctypes.c_char_p),  # import_paths
        ctypes.c_int,  # n_paths
        ctypes.c_char_p,  # opts_json
        ctypes.c_int,  # workers
        ctypes.c_int,  # capacity
        _HANDLE_PTR,  # out_handle
    ]
    lib.BufarrowPoolNew.restype = ctypes.c_int

    lib.BufarrowPoolNewWithHyperType.argtypes = [
        ctypes.c_char_p,  # proto_path
        ctypes.c_char_p,  # msg_name
        ctypes.POINTER(ctypes.c_char_p),  # import_paths
        ctypes.c_int,  # n_paths
        ctypes.c_char_p,  # opts_json
        _HANDLE,  # hyper_handle
        ctypes.c_int,  # workers
        ctypes.c_int,  # capacity
        _HANDLE_PTR,  # out_handle
    ]
    lib.BufarrowPoolNewWithHyperType.restype = ctypes.c_int

    lib.BufarrowPoolSubmit.argtypes = [_HANDLE, ctypes.c_void_p, ctypes.c_int]
    lib.BufarrowPoolSubmit.restype = ctypes.c_int

    lib.BufarrowPoolSubmitMerged.argtypes = [
        _HANDLE,
        ctypes.c_void_p, ctypes.c_int,  # base
        ctypes.c_void_p, ctypes.c_int,  # custom
    ]
    lib.BufarrowPoolSubmitMerged.restype = ctypes.c_int

    lib.BufarrowPoolFlush.argtypes = [
        _HANDLE,
        ctypes.POINTER(ArrowArray),
        ctypes.POINTER(ArrowSchema),
    ]
    lib.BufarrowPoolFlush.restype = ctypes.c_int

    lib.BufarrowPoolPending.argtypes = [_HANDLE]
    lib.BufarrowPoolPending.restype = ctypes.c_int

    lib.BufarrowPoolLastError.argtypes = [_HANDLE]
    lib.BufarrowPoolLastError.restype = ctypes.c_void_p

    lib.BufarrowPoolFree.argtypes = [_HANDLE]
    lib.BufarrowPoolFree.restype = None


# ── Helpers ─────────────────────────────────────────────────────────────


def _read_c_string(ptr: int) -> str:
    """Read a C string from a raw void pointer, returning '' if NULL."""
    if not ptr:
        return ""
    return ctypes.string_at(ptr).decode("utf-8", errors="replace")


def _check(status: int, handle: ctypes.c_void_p) -> None:
    """Raise BufarrowError if status != 0, pulling message from BufarrowLastError."""
    if status != 0:
        lib = _get_lib()
        ptr = lib.BufarrowLastError(handle)
        msg = _read_c_string(ptr) or "unknown error"
        if ptr:
            lib.BufarrowFreeString(ptr)
        raise BufarrowError(msg)


def _check_global(status: int) -> None:
    """Raise BufarrowError for pre-handle failures using the global error store."""
    if status != 0:
        lib = _get_lib()
        ptr = lib.BufarrowGetGlobalError()
        msg = _read_c_string(ptr) or "unknown error"
        if ptr:
            lib.BufarrowFreeString(ptr)
        raise BufarrowError(msg)


def _encode(s: str | None) -> bytes | None:
    """Encode a string for C, returning None for None."""
    if s is None:
        return None
    return s.encode("utf-8")


def _make_import_paths(paths: list[str] | None):
    """Create a ctypes array of C strings from a list of Python strings."""
    if not paths:
        return None, 0
    arr_type = ctypes.c_char_p * len(paths)
    arr = arr_type(*(p.encode("utf-8") for p in paths))
    return arr, len(paths)
