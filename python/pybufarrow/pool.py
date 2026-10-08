"""Go-managed parallel ingestion pool for pybufarrow."""

from __future__ import annotations

import ctypes
import json
from typing import TYPE_CHECKING, Any

import pyarrow as pa

from ._ffi import (
    ArrowArray,
    ArrowSchema,
    BufarrowMetaValue,
    BufarrowError,
    _check_global,
    _encode,
    _get_lib,
    _make_import_paths,
    _read_c_string,
    _schema_opts,
)
from ._meta import _to_meta_value

if TYPE_CHECKING:
    from .hypertype import HyperType


class Pool:
    """Go-managed parallel ingestion pool.

    A Pool fans out protobuf message parsing across N Go goroutines, keeping
    Python single-threaded. This avoids OS-thread management overhead compared
    to a ``ThreadPoolExecutor`` of :class:`Transcoder` handles.

    **Usage pattern**::

        from pybufarrow import Pool

        pool = Pool.from_proto_file(
            "BidRequest.proto", "BidRequestEvent",
            workers=8,
        )
        for msg in kafka_consumer:
            pool.submit(msg.value)          # fast, usually non-blocking
            if pool.pending() >= 10_000:
                batch = pool.flush()        # returns pa.RecordBatch
                sink.write(batch)
        batch = pool.flush()                # drain remainder
        sink.write(batch)
        pool.close()

    Use as a context manager to ensure cleanup::

        with Pool.from_proto_file(...) as pool:
            for msg in messages:
                pool.submit(msg)
            batch = pool.flush()

    **Backpressure**: :meth:`submit` blocks only when the internal job channel
    is full (``capacity`` slots). This propagates flow control to the caller's
    consumer loop without any explicit rate-limiting logic.

    **Flush semantics**: all messages whose :meth:`submit` call returned before
    :meth:`flush` is called are included in the returned batch (messages that
    were enqueued but not yet processed by a worker are drained before the
    batch is assembled).
    """

    def __init__(self, handle: ctypes.c_void_p) -> None:
        self._handle = handle
        self._closed = False
        self._meta_col_order: list[str] = []

    # ── Constructors ────────────────────────────────────────────────────

    @classmethod
    def from_proto_file(
        cls,
        proto_path: str,
        message_name: str,
        import_paths: list[str] | None = None,
        *,
        workers: int = 0,
        capacity: int = 0,
        hyper_type: HyperType | None = None,
        custom_proto: str | None = None,
        custom_message: str | None = None,
        custom_import_paths: list[str] | None = None,
        denorm_columns: list[str] | None = None,
        denorm_metadata_columns: list[tuple[str, str]] | None = None,
        json_termination: list[str] | None = None,
        json_termination_auto: bool = False,
        well_known_types: bool = False,
        prune_empty_messages: bool = False,
        opts: dict | None = None,
    ) -> Pool:
        """Create a Pool from a .proto file.

        Parameters
        ----------
        proto_path : str
            Path to the ``.proto`` file.
        message_name : str
            Top-level message name.
        import_paths : list[str], optional
            Directories to search for proto imports.
        workers : int, optional
            Number of Go worker goroutines (0 → ``GOMAXPROCS``).
        capacity : int, optional
            Job channel capacity (0 → ``workers × 64``).
        hyper_type : HyperType, optional
            Shared HyperType coordinator for PGO-enabled ingestion.
            Required for :meth:`submit` (AppendRaw path).
        custom_proto : str, optional
            Path to a .proto file with custom (extra) fields.
        custom_message : str, optional
            Message name inside custom_proto.
        custom_import_paths : list[str], optional
            Import paths for the custom proto.
        denorm_columns : list[str], optional
            List of pbpath expressions for denormalization columns.
            When set, :meth:`flush` returns a denormalized record batch.
        json_termination : list[str], optional
            Fully qualified names of self-referential message types to allow.
            Each type expands normally, and only the field where it would
            contain itself again becomes a protojson string column
            (``list<string>`` when repeated). Without it, a cyclic schema
            raises :class:`CyclicTypeError`. Use :func:`cyclic_types` to find
            the names. The cells are JSON text, so strings inside them stay
            double-quoted; compare them after ``json.loads``, never as strings.
        json_termination_auto : bool, optional
            Terminate every cyclic type without naming them. Caution: a
            recursive field added to the .proto later then changes the
            Arrow and Parquet schema with no error. Prefer
            ``json_termination=cyclic_types(...)`` to keep that change visible.
        well_known_types : bool, optional
            Map well-known types (Timestamp, the wrappers, ...) to flat Arrow
            scalars, as the denormalizer does. Changes the schema.
        prune_empty_messages : bool, optional
            Drop fields of empty message types, which Parquet cannot store.
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
        if denorm_metadata_columns:
            opts_payload["denorm_metadata_columns"] = [
                {"name": name, "type": typ}
                for name, typ in denorm_metadata_columns
            ]

        opts_payload.update(
            _schema_opts(
                json_termination,
                json_termination_auto,
                well_known_types,
                prune_empty_messages,
            )
        )

        opts_json = _encode(json.dumps(opts_payload)) if opts_payload else None

        if hyper_type is not None:
            status = lib.BufarrowPoolNewWithHyperType(
                _encode(proto_path),
                _encode(message_name),
                paths_arr,
                n_paths,
                opts_json,
                hyper_type._handle,
                workers,
                capacity,
                ctypes.byref(handle),
            )
        else:
            status = lib.BufarrowPoolNew(
                _encode(proto_path),
                _encode(message_name),
                paths_arr,
                n_paths,
                opts_json,
                workers,
                capacity,
                ctypes.byref(handle),
            )

        if status != 0:
            _check_global(status)
        pool = cls(handle)
        if denorm_metadata_columns:
            pool._meta_col_order = [name for name, _ in denorm_metadata_columns]
        return pool

    # ── Ingestion ───────────────────────────────────────────────────────

    def submit(self, data: bytes, **meta_values: Any) -> None:
        """Enqueue one serialized proto message.

        Copies the bytes into Go-managed memory before returning; the
        ``data`` buffer may be freed or reused immediately after this call.

        Blocks only when the internal job channel is full, providing
        automatic backpressure to the caller's consumer loop.

        Parameters
        ----------
        data : bytes
            Serialized protobuf message bytes.
        """
        self._ensure_open()
        lib = _get_lib()
        meta_array, n_meta, keepalive = self._build_meta_values(meta_values)
        status = lib.BufarrowPoolSubmit(
            self._handle,
            data,
            len(data),
            meta_array,
            n_meta,
        )
        _ = keepalive
        if status != 0:
            ptr = lib.BufarrowPoolLastError(self._handle)
            msg = _read_c_string(ptr) or "pool submit error"
            if ptr:
                lib.BufarrowFreeString(ptr)
            raise BufarrowError(msg)

    def submit_merged(self, base: bytes, custom: bytes, **meta_values: Any) -> None:
        """Enqueue one base+custom byte pair (merged ingestion path).

        Both buffers are copied before return; the caller may free them
        immediately.

        Parameters
        ----------
        base : bytes
            Serialized base protobuf message.
        custom : bytes
            Serialized custom-fields protobuf message.
        """
        self._ensure_open()
        lib = _get_lib()
        meta_array, n_meta, keepalive = self._build_meta_values(meta_values)
        status = lib.BufarrowPoolSubmitMerged(
            self._handle,
            base,
            len(base),
            custom,
            len(custom),
            meta_array,
            n_meta,
        )
        _ = keepalive
        if status != 0:
            ptr = lib.BufarrowPoolLastError(self._handle)
            msg = _read_c_string(ptr) or "pool submit merged error"
            if ptr:
                lib.BufarrowFreeString(ptr)
            raise BufarrowError(msg)

    # ── Flush ───────────────────────────────────────────────────────────

    def flush(self) -> pa.RecordBatch:
        """Wait for all in-flight messages, merge per-worker batches, return.

        All messages submitted before this call are guaranteed to appear in
        the returned batch. The pool is immediately ready for the next
        submit/flush window after this call returns.

        Returns
        -------
        pa.RecordBatch
            Arrow RecordBatch containing all messages from the current window.
        """
        self._ensure_open()
        lib = _get_lib()
        c_array = ArrowArray()
        c_schema = ArrowSchema()
        status = lib.BufarrowPoolFlush(
            self._handle, ctypes.byref(c_array), ctypes.byref(c_schema)
        )
        if status != 0:
            ptr = lib.BufarrowPoolLastError(self._handle)
            msg = _read_c_string(ptr) or "pool flush error"
            if ptr:
                lib.BufarrowFreeString(ptr)
            raise BufarrowError(msg)
        return pa.RecordBatch._import_from_c(
            ctypes.addressof(c_array), ctypes.addressof(c_schema)
        )

    # ── Info ────────────────────────────────────────────────────────────

    def pending(self) -> int:
        """Approximate number of messages queued but not yet flushed."""
        self._ensure_open()
        lib = _get_lib()
        return int(lib.BufarrowPoolPending(self._handle))

    # ── Lifecycle ───────────────────────────────────────────────────────

    def close(self) -> None:
        """Release the pool and all worker resources."""
        if not self._closed:
            self._closed = True
            lib = _get_lib()
            lib.BufarrowPoolFree(self._handle)

    def _ensure_open(self) -> None:
        if self._closed:
            raise BufarrowError("Pool is closed")

    def _build_meta_values(
        self, meta_values: dict[str, Any]
    ) -> tuple[ctypes.Array[BufarrowMetaValue] | None, int, list[ctypes.Array[ctypes.c_char]]]:
        if not self._meta_col_order:
            if meta_values:
                raise BufarrowError("metadata columns are not configured for this pool")
            return None, 0, []

        unknown = [k for k in meta_values if k not in self._meta_col_order]
        if unknown:
            raise BufarrowError(f"unknown metadata column(s): {', '.join(sorted(unknown))}")

        n = len(self._meta_col_order)
        arr_type = BufarrowMetaValue * n
        arr = arr_type()
        keepalive: list[ctypes.Array[ctypes.c_char]] = []
        for i, key in enumerate(self._meta_col_order):
            mv, raw = _to_meta_value(meta_values.get(key))
            arr[i] = mv
            if raw is not None:
                keepalive.append(raw)
        return arr, n, keepalive

    def __enter__(self) -> Pool:
        return self

    def __exit__(self, *_) -> None:
        self.close()

    def __repr__(self) -> str:
        return f"Pool(closed={self._closed})"
