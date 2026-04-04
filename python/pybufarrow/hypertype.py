"""HyperType PGO coordinator wrapper."""

from __future__ import annotations

import ctypes

from ._ffi import (
    BufarrowError,
    _encode,
    _get_lib,
    _make_import_paths,
)


class HyperType:
    """Shared HyperType coordinator for PGO-enabled protobuf ingestion.

    A single HyperType can be shared across multiple :class:`Transcoder`
    instances (including clones) running in separate threads. All
    transcoders sharing the same HyperType contribute profiling data,
    and a recompile atomically upgrades the parser for all of them.

    Parameters
    ----------
    proto_path : str
        Path to the .proto file.
    message_name : str
        Top-level message name.
    import_paths : list[str], optional
        Directories to search for proto imports.
    auto_recompile_threshold : int, optional
        Recompile after this many messages (0 = manual only).
    sample_rate : float, optional
        Profiling sampling fraction (default 0.01 = 1%).
    """

    def __init__(
        self,
        proto_path: str,
        message_name: str,
        import_paths: list[str] | None = None,
        auto_recompile_threshold: int = 0,
        sample_rate: float = 0.01,
    ) -> None:
        lib = _get_lib()
        self._handle = ctypes.c_void_p()
        paths_arr, n_paths = _make_import_paths(import_paths)

        status = lib.BufarrowNewHyperType(
            _encode(proto_path),
            _encode(message_name),
            paths_arr,
            n_paths,
            auto_recompile_threshold,
            sample_rate,
            ctypes.byref(self._handle),
        )
        if status != 0:
            raise BufarrowError(
                f"Failed to create HyperType from {proto_path}:{message_name}"
            )
        self._closed = False

    def close(self) -> None:
        """Release the underlying C handle."""
        if not self._closed and self._handle:
            _get_lib().BufarrowFreeHyperType(self._handle)
            self._closed = True

    def __enter__(self) -> HyperType:
        return self

    def __exit__(self, *exc) -> None:
        self.close()

    def __del__(self) -> None:
        try:
            self.close()
        except Exception:
            pass
