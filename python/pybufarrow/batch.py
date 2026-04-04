"""Convenience batch helpers for streaming protobuf-to-Arrow transcoding."""

from __future__ import annotations

from collections.abc import Iterable, Iterator

import pyarrow as pa

from .transcoder import Transcoder


def transcode_batch(
    proto_path: str,
    message_name: str,
    messages: Iterable[bytes],
    batch_size: int = 1024,
    import_paths: list[str] | None = None,
    **kwargs,
) -> Iterator[pa.RecordBatch]:
    """Yield RecordBatches by appending raw protobuf messages in chunks.

    Parameters
    ----------
    proto_path : str
        Path to the .proto file.
    message_name : str
        Top-level message name.
    messages : Iterable[bytes]
        Stream of serialized protobuf messages.
    batch_size : int
        Number of messages per RecordBatch.
    import_paths : list[str], optional
        Proto import directories.
    **kwargs
        Forwarded to :meth:`Transcoder.from_proto_file`.
    """
    with Transcoder.from_proto_file(
        proto_path, message_name, import_paths=import_paths, **kwargs
    ) as tc:
        count = 0
        for msg in messages:
            tc.append(msg)
            count += 1
            if count >= batch_size:
                yield tc.flush()
                count = 0
        if count > 0:
            yield tc.flush()


def transcode_merged_batch(
    proto_path: str,
    message_name: str,
    messages: Iterable[tuple[bytes, bytes]],
    batch_size: int = 1024,
    import_paths: list[str] | None = None,
    **kwargs,
) -> Iterator[pa.RecordBatch]:
    """Yield RecordBatches from merged base+custom protobuf byte pairs.

    Each element in *messages* is ``(base_bytes, custom_bytes)``.

    Parameters
    ----------
    proto_path : str
        Path to the .proto file (base message).
    message_name : str
        Top-level message name.
    messages : Iterable[tuple[bytes, bytes]]
        Stream of (base, custom) serialized protobuf byte pairs.
    batch_size : int
        Number of messages per RecordBatch.
    import_paths : list[str], optional
        Proto import directories.
    **kwargs
        Must include ``custom_proto`` and ``custom_message`` for the
        custom field definition. Forwarded to :meth:`Transcoder.from_proto_file`.
    """
    with Transcoder.from_proto_file(
        proto_path, message_name, import_paths=import_paths, **kwargs
    ) as tc:
        count = 0
        for base, custom in messages:
            tc.append_merged(base, custom)
            count += 1
            if count >= batch_size:
                yield tc.flush()
                count = 0
        if count > 0:
            yield tc.flush()


def transcode_to_table(
    proto_path: str,
    message_name: str,
    messages: Iterable[bytes],
    import_paths: list[str] | None = None,
    **kwargs,
) -> pa.Table:
    """Transcode all messages into a single pyarrow Table.

    Parameters
    ----------
    proto_path : str
        Path to the .proto file.
    message_name : str
        Top-level message name.
    messages : Iterable[bytes]
        Stream of serialized protobuf messages.
    import_paths : list[str], optional
        Proto import directories.
    **kwargs
        Forwarded to :meth:`Transcoder.from_proto_file`.
    """
    batches = list(
        transcode_batch(
            proto_path, message_name, messages,
            import_paths=import_paths, **kwargs,
        )
    )
    if not batches:
        with Transcoder.from_proto_file(
            proto_path, message_name, import_paths=import_paths, **kwargs
        ) as tc:
            return pa.table([], schema=tc.schema)
    return pa.Table.from_batches(batches)


def transcode_to_parquet(
    proto_path: str,
    message_name: str,
    messages: Iterable[bytes],
    output_path: str,
    import_paths: list[str] | None = None,
    **kwargs,
) -> None:
    """Transcode protobuf messages directly to a Parquet file.

    Parameters
    ----------
    proto_path : str
        Path to the .proto file.
    message_name : str
        Top-level message name.
    messages : Iterable[bytes]
        Stream of serialized protobuf messages.
    output_path : str
        Destination Parquet file path.
    import_paths : list[str], optional
        Proto import directories.
    **kwargs
        Forwarded to :meth:`Transcoder.from_proto_file`.
    """
    table = transcode_to_table(
        proto_path, message_name, messages,
        import_paths=import_paths, **kwargs,
    )
    pa.parquet.write_table(table, output_path)
