"""Pythonic Transcoder wrapper around libbufarrow."""

from __future__ import annotations

import ctypes
import json
from functools import cached_property
from typing import TYPE_CHECKING

import pyarrow as pa

from ._ffi import (
    ArrowArray,
    ArrowSchema,
    BufarrowError,
    _check,
    _check_global,
    _encode,
    _get_lib,
    _make_import_paths,
)

if TYPE_CHECKING:
    from .hypertype import HyperType


class Transcoder:
    """Protobuf-to-Arrow transcoder backed by the bufarrowlib CGo shared library.

    Use :meth:`from_proto_file` or :meth:`from_config` to create instances.
    The transcoder manages an opaque C handle; use as a context manager
    or call :meth:`close` explicitly to release resources.
    """

    def __init__(self, handle: ctypes.c_void_p) -> None:
        self._handle = handle
        self._closed = False

    # ── Constructors ────────────────────────────────────────────────────

    @classmethod
    def from_proto_file(
        cls,
        proto_path: str,
        message_name: str,
        import_paths: list[str] | None = None,
        *,
        custom_proto: str | None = None,
        custom_message: str | None = None,
        custom_import_paths: list[str] | None = None,
        denorm_columns: list[str] | None = None,
        hyper_type: HyperType | None = None,
        opts: dict | None = None,
    ) -> Transcoder:
        """Create a Transcoder from a .proto file.

        Parameters
        ----------
        proto_path : str
            Path to the .proto file.
        message_name : str
            Top-level message name.
        import_paths : list[str], optional
            Directories to search for proto imports.
        custom_proto : str, optional
            Path to a .proto file with custom (extra) fields.
        custom_message : str, optional
            Message name inside custom_proto.
        custom_import_paths : list[str], optional
            Import paths for the custom proto.
        denorm_columns : list[str], optional
            List of pbpath expressions for denormalization columns
            (e.g. ``["name", "items[*].id", "items[*].price"]``).
            Each path's last segment is used as the output column alias.
        hyper_type : HyperType, optional
            Shared HyperType coordinator for PGO-enabled ingestion.
        opts : dict, optional
            Additional JSON-serializable options.
        """
        lib = _get_lib()
        handle = ctypes.c_void_p()
        paths_arr, n_paths = _make_import_paths(import_paths)

        opts_payload = opts.copy() if opts else {}
        if custom_proto:
            opts_payload["custom_proto"] = custom_proto
        if custom_message:
            opts_payload["custom_message"] = custom_message
        if custom_import_paths:
            opts_payload["custom_import_paths"] = custom_import_paths
        if denorm_columns:
            opts_payload["denorm_columns"] = denorm_columns

        opts_json = _encode(json.dumps(opts_payload)) if opts_payload else None

        if hyper_type is not None:
            status = lib.BufarrowNewFromFileWithHyperType(
                _encode(proto_path),
                _encode(message_name),
                paths_arr,
                n_paths,
                opts_json,
                hyper_type._handle,
                ctypes.byref(handle),
            )
        else:
            status = lib.BufarrowNewFromFile(
                _encode(proto_path),
                _encode(message_name),
                paths_arr,
                n_paths,
                opts_json,
                ctypes.byref(handle),
            )

        if status != 0:
            _check_global(status)
        return cls(handle)

    @classmethod
    def from_config(cls, config_path: str) -> Transcoder:
        """Create a Transcoder from a YAML config file."""
        lib = _get_lib()
        handle = ctypes.c_void_p()
        status = lib.BufarrowNewFromConfig(
            _encode(config_path), ctypes.byref(handle)
        )
        if status != 0:
            _check_global(status)
        return cls(handle)

    @classmethod
    def from_config_string(cls, yaml_string: str) -> Transcoder:
        """Create a Transcoder from a YAML config string."""
        lib = _get_lib()
        handle = ctypes.c_void_p()
        encoded = yaml_string.encode("utf-8")
        status = lib.BufarrowNewFromConfigString(
            encoded, len(encoded), ctypes.byref(handle)
        )
        if status != 0:
            _check_global(status)
        return cls(handle)

    # ── Ingestion ───────────────────────────────────────────────────────

    def append(self, data: bytes) -> None:
        """Append serialized protobuf bytes (requires HyperType)."""
        self._ensure_open()
        lib = _get_lib()
        status = lib.BufarrowAppendRaw(self._handle, data, len(data))
        _check(status, self._handle)

    def append_merged(self, base: bytes, custom: bytes) -> None:
        """Append merged base + custom serialized protobuf bytes.

        Use when adding extra fields (e.g. timestamps, metadata) to a
        protobuf stream. ``base`` is the original message bytes and
        ``custom`` is the extra-fields message bytes.
        """
        self._ensure_open()
        lib = _get_lib()
        status = lib.BufarrowAppendRawMerged(
            self._handle, base, len(base), custom, len(custom)
        )
        _check(status, self._handle)

    def append_denorm(self, data: bytes) -> None:
        """Append serialized protobuf bytes to the denormalizer."""
        self._ensure_open()
        lib = _get_lib()
        status = lib.BufarrowAppendDenormRaw(self._handle, data, len(data))
        _check(status, self._handle)

    def append_denorm_merged(self, base: bytes, custom: bytes) -> None:
        """Append merged base + custom bytes to the denormalizer."""
        self._ensure_open()
        lib = _get_lib()
        status = lib.BufarrowAppendDenormRawMerged(
            self._handle, base, len(base), custom, len(custom)
        )
        _check(status, self._handle)

    # ── Flush ───────────────────────────────────────────────────────────

    def flush(self) -> pa.RecordBatch:
        """Flush the record builder and return a pyarrow RecordBatch."""
        self._ensure_open()
        lib = _get_lib()
        c_array = ArrowArray()
        c_schema = ArrowSchema()
        status = lib.BufarrowFlush(
            self._handle, ctypes.byref(c_array), ctypes.byref(c_schema)
        )
        _check(status, self._handle)
        return pa.RecordBatch._import_from_c(
            ctypes.addressof(c_array), ctypes.addressof(c_schema)
        )

    def flush_denorm(self) -> pa.RecordBatch:
        """Flush the denormalizer and return a pyarrow RecordBatch."""
        self._ensure_open()
        lib = _get_lib()
        c_array = ArrowArray()
        c_schema = ArrowSchema()
        status = lib.BufarrowFlushDenorm(
            self._handle, ctypes.byref(c_array), ctypes.byref(c_schema)
        )
        _check(status, self._handle)
        return pa.RecordBatch._import_from_c(
            ctypes.addressof(c_array), ctypes.addressof(c_schema)
        )

    # ── Schema ──────────────────────────────────────────────────────────

    @cached_property
    def schema(self) -> pa.Schema:
        """Arrow schema for the full message."""
        self._ensure_open()
        lib = _get_lib()
        c_schema = ArrowSchema()
        status = lib.BufarrowGetSchema(self._handle, ctypes.byref(c_schema))
        _check(status, self._handle)
        return pa.Schema._import_from_c(ctypes.addressof(c_schema))

    @cached_property
    def denorm_schema(self) -> pa.Schema:
        """Arrow schema for the denormalized view."""
        self._ensure_open()
        lib = _get_lib()
        c_schema = ArrowSchema()
        status = lib.BufarrowGetDenormSchema(
            self._handle, ctypes.byref(c_schema)
        )
        _check(status, self._handle)
        return pa.Schema._import_from_c(ctypes.addressof(c_schema))

    @cached_property
    def field_names(self) -> list[str]:
        """Top-level Arrow field names."""
        self._ensure_open()
        lib = _get_lib()
        ptr = lib.BufarrowFieldNames(self._handle)
        if not ptr:
            return []
        from ._ffi import _read_c_string
        result = json.loads(_read_c_string(ptr))
        lib.BufarrowFreeString(ptr)
        return result

    # ── Parquet ─────────────────────────────────────────────────────────

    def write_parquet(self, path: str) -> None:
        """Write buffered records to a Parquet file."""
        self._ensure_open()
        lib = _get_lib()
        status = lib.BufarrowWriteParquet(self._handle, _encode(path))
        _check(status, self._handle)

    def write_parquet_denorm(self, path: str) -> None:
        """Write buffered denorm records to a Parquet file."""
        self._ensure_open()
        lib = _get_lib()
        status = lib.BufarrowWriteParquetDenorm(self._handle, _encode(path))
        _check(status, self._handle)

    def read_parquet(
        self, path: str, columns: list[int] | None = None
    ) -> pa.RecordBatch:
        """Read a Parquet file and return a pyarrow RecordBatch.

        Parameters
        ----------
        path : str
            Path to the Parquet file.
        columns : list[int], optional
            Column indices to read. None reads all columns.
        """
        self._ensure_open()
        lib = _get_lib()
        c_array = ArrowArray()
        c_schema = ArrowSchema()
        cols_json = _encode(json.dumps(columns)) if columns else None
        status = lib.BufarrowReadParquet(
            self._handle,
            _encode(path),
            cols_json,
            ctypes.byref(c_array),
            ctypes.byref(c_schema),
        )
        _check(status, self._handle)
        return pa.RecordBatch._import_from_c(
            ctypes.addressof(c_array), ctypes.addressof(c_schema)
        )

    # ── Clone ───────────────────────────────────────────────────────────

    def clone(self) -> Transcoder:
        """Create an independent copy for parallel use."""
        self._ensure_open()
        lib = _get_lib()
        handle = ctypes.c_void_p()
        status = lib.BufarrowClone(self._handle, ctypes.byref(handle))
        _check(status, self._handle)
        return Transcoder(handle)

    # ── Lifecycle ───────────────────────────────────────────────────────

    def close(self) -> None:
        """Release the underlying C handle."""
        if not self._closed and self._handle:
            _get_lib().BufarrowFree(self._handle)
            self._closed = True

    def _ensure_open(self) -> None:
        if self._closed:
            raise BufarrowError("Transcoder has been closed")

    def __enter__(self) -> Transcoder:
        return self

    def __exit__(self, *exc) -> None:
        self.close()

    def __del__(self) -> None:
        try:
            self.close()
        except Exception:
            pass
