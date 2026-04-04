"""Test fixtures and helpers for pybufarrow tests."""

from __future__ import annotations

import os
import struct
from pathlib import Path

import pytest

FIXTURES_DIR = Path(__file__).parent / "fixtures"

# Proto samples directory (Go source tree) — contains BidRequest.proto and
# custom/CustomTypes.proto that the Python BidRequest benchmarks need.
_PROTO_SAMPLES_DIR = str(Path(__file__).parent.parent.parent / "proto" / "samples")


@pytest.fixture
def fixtures_dir() -> Path:
    return FIXTURES_DIR


@pytest.fixture
def test_proto() -> str:
    return str(FIXTURES_DIR / "test_msg.proto")


@pytest.fixture
def custom_proto() -> str:
    return str(FIXTURES_DIR / "custom_extra.proto")


@pytest.fixture
def hyper_type(test_proto):
    """Shared HyperType for tests that need AppendRaw."""
    from pybufarrow import HyperType

    ht = HyperType(test_proto, "TestMsg")
    yield ht
    ht.close()


def _encode_varint(value: int) -> bytes:
    """Encode an unsigned integer as a protobuf varint."""
    result = bytearray()
    while value > 0x7F:
        result.append((value & 0x7F) | 0x80)
        value >>= 7
    result.append(value & 0x7F)
    return bytes(result)


def _encode_zigzag(value: int) -> int:
    """ZigZag encode a signed integer."""
    return (value << 1) ^ (value >> 31)


def encode_test_msg(name: str, age: int, score: float, active: bool) -> bytes:
    """Encode a TestMsg protobuf message manually.

    Wire format for proto3 fields:
      field 1 (string name):  tag=0x0A, len-delimited
      field 2 (int32 age):    tag=0x10, varint
      field 3 (double score): tag=0x19, fixed64
      field 4 (bool active):  tag=0x20, varint
    """
    parts = bytearray()

    # field 1: string name (wire type 2 = length-delimited)
    if name:
        name_bytes = name.encode("utf-8")
        parts.append(0x0A)  # (1 << 3) | 2
        parts.extend(_encode_varint(len(name_bytes)))
        parts.extend(name_bytes)

    # field 2: int32 age (wire type 0 = varint)
    if age != 0:
        parts.append(0x10)  # (2 << 3) | 0
        parts.extend(_encode_varint(age))

    # field 3: double score (wire type 1 = fixed64)
    if score != 0.0:
        parts.append(0x19)  # (3 << 3) | 1
        parts.extend(struct.pack("<d", score))

    # field 4: bool active (wire type 0 = varint)
    if active:
        parts.append(0x20)  # (4 << 3) | 0
        parts.append(0x01)

    return bytes(parts)


def encode_custom_extra(event_ts: int, source_id: str) -> bytes:
    """Encode a CustomExtra protobuf message manually.

    Wire format:
      field 1 (int64 event_ts):  tag=0x08, varint
      field 2 (string source_id): tag=0x12, len-delimited
    """
    parts = bytearray()

    if event_ts != 0:
        parts.append(0x08)  # (1 << 3) | 0
        parts.extend(_encode_varint(event_ts))

    if source_id:
        src_bytes = source_id.encode("utf-8")
        parts.append(0x12)  # (2 << 3) | 2
        parts.extend(_encode_varint(len(src_bytes)))
        parts.extend(src_bytes)

    return bytes(parts)


# ── Order / Item encoders for denorm tests ──────────────────────────────


@pytest.fixture
def order_proto() -> str:
    return str(FIXTURES_DIR / "order.proto")


@pytest.fixture
def denorm_config(tmp_path) -> str:
    """Generate a denorm config with absolute proto path."""
    proto_path = str(FIXTURES_DIR / "order.proto")
    yaml_content = f"""\
proto:
  file: {proto_path}
  message: Order
denormalizer:
  columns:
    - name: order_name
      path: name
    - name: item_id
      path: items[*].id
    - name: item_price
      path: items[*].price
    - name: order_seq
      path: seq
"""
    config_path = tmp_path / "denorm_config.yaml"
    config_path.write_text(yaml_content)
    return str(config_path)


@pytest.fixture
def order_hyper_type(order_proto):
    """HyperType for Order message."""
    from pybufarrow import HyperType

    ht = HyperType(order_proto, "Order")
    yield ht
    ht.close()


def _encode_length_delimited(field_number: int, data: bytes) -> bytes:
    """Encode a length-delimited protobuf field."""
    tag = (field_number << 3) | 2
    return bytes([*_encode_varint(tag), *_encode_varint(len(data)), *data])


def _encode_varint_field(field_number: int, value: int) -> bytes:
    """Encode a varint protobuf field."""
    tag = (field_number << 3) | 0
    return bytes([*_encode_varint(tag), *_encode_varint(value)])


def _encode_double_field(field_number: int, value: float) -> bytes:
    """Encode a fixed64 (double) protobuf field."""
    tag = (field_number << 3) | 1
    return bytes([*_encode_varint(tag), *struct.pack("<d", value)])


def encode_item(item_id: str, price: float) -> bytes:
    """Encode an Item message: field 1=string id, field 2=double price."""
    parts = bytearray()
    if item_id:
        parts.extend(_encode_length_delimited(1, item_id.encode("utf-8")))
    if price != 0.0:
        parts.extend(_encode_double_field(2, price))
    return bytes(parts)


def encode_order(name: str, items: list[tuple[str, float]], seq: int = 0) -> bytes:
    """Encode an Order message.

    Fields: 1=string name, 2=repeated Item items, 3=int64 seq.
    """
    parts = bytearray()
    if name:
        parts.extend(_encode_length_delimited(1, name.encode("utf-8")))
    for item_id, price in items:
        item_bytes = encode_item(item_id, price)
        parts.extend(_encode_length_delimited(2, item_bytes))
    if seq != 0:
        parts.extend(_encode_varint_field(3, seq))
    return bytes(parts)


# ── BidRequestEvent encoder (matches benchRealisticBidRequestCorpus) ────────
#
# Wire-format encoder for BidRequestEvent and all its nested messages.
# The generation distribution mirrors Go's benchRealisticBidRequestCorpus so
# that Python and Go MaxThroughput benchmarks process the same logical data shape:
#
#   imp count  : 75% → 2, 18% → 1, 6% → 3, 1% → 4
#   banner/video: 55% video-primary (banner 0×0), 45% banner-primary (video 0×0)
#   deals/imp  : 2% → 0, 61% → 1, 21% → 2, 6% → 3, 10% → 4
#   All top-level fields always present (user, device, site, tp, dooh, timestamp)
#
# This schema has 14 top-level fields, deeply nested messages (up to 6 levels),
# repeated fields, DecimalValue sub-messages, and a Timestamp sub-message.
# It is representative of production ad-tech workloads.


def _enc_varint_br(v: int) -> bytes:
    """Encode unsigned integer as protobuf varint."""
    out = bytearray()
    while v > 0x7F:
        out.append((v & 0x7F) | 0x80)
        v >>= 7
    out.append(v & 0x7F)
    return bytes(out)


def _fld_len(field_no: int, data: bytes) -> bytes:
    """Wire type 2: length-delimited field."""
    tag = _enc_varint_br((field_no << 3) | 2)
    return tag + _enc_varint_br(len(data)) + data


def _fld_str(field_no: int, s: str) -> bytes:
    """Wire type 2: string field (omitted when empty)."""
    if not s:
        return b""
    return _fld_len(field_no, s.encode())


def _fld_u32(field_no: int, v: int) -> bytes:
    """Wire type 0: uint32 / uint64 varint field (omitted when 0)."""
    if v == 0:
        return b""
    return _enc_varint_br((field_no << 3) | 0) + _enc_varint_br(v)


def _fld_sint32(field_no: int, v: int) -> bytes:
    """Wire type 0: sint32 zigzag-encoded field."""
    zz = (v << 1) ^ (v >> 31)
    return _enc_varint_br((field_no << 3) | 0) + _enc_varint_br(zz)


def _fld_float(field_no: int, v: float) -> bytes:
    """Wire type 5: float (sfixed32 format)."""
    return _enc_varint_br((field_no << 3) | 5) + struct.pack("<f", v)


def _enc_decimal(units: int, nanos: int = 0) -> bytes:
    """Encode CustomTypes.DecimalValue{units int64 (field 1), nanos sfixed32 (field 2)}."""
    out = bytearray()
    if units != 0:
        out += _fld_u32(1, units)
    if nanos != 0:
        # sfixed32: wire type 5, tag = (2<<3)|5 = 0x15, 4 bytes LE signed
        out += bytes([0x15]) + struct.pack("<i", nanos)
    return bytes(out)


def _enc_timestamp(seconds: int) -> bytes:
    """Encode google.protobuf.Timestamp{seconds int64 (field 1)}."""
    return _fld_u32(1, seconds)


def encode_bid_request(i: int) -> bytes:
    """Encode BidRequestEvent matching benchRealisticBidRequestCorpus distribution.

    Parameters
    ----------
    i:
        Message index (deterministic, use range(n) for a corpus).
    """
    _tp_ids = [11, 15, 22, 35, 42, 55, 67, 80, 99, 110, 125, 150, 175, 200,
               250, 300, 350, 400, 450, 530]
    _pub_ids = ["pub-001", "pub-002", "pub-003", "pub-004", "pub-005",
                "pub-010", "pub-020", "pub-050", "pub-100"]
    _site_names = ["ExampleNews", "SportsBlog", "TechReview", "WeatherApp", "GamingHub"]
    _countries = ["US", "CA", "GB", "DE", "FR", "AU", "JP"]
    _regions = ["CA", "NY", "TX", "IL", "FL", "ON", "BC"]
    _banner_sz = [(1080, 1920), (1920, 1080), (1024, 555), (1024, 768),
                  (1400, 400), (1600, 900), (840, 400)]
    _video_sz = [(1080, 1920), (1920, 1080), (720, 1280), (2160, 3840), (1024, 555)]

    # ── imp count distribution ───────────────────────────────────────
    m = i % 100
    if m < 75:
        imp_count = 2
    elif m < 93:
        imp_count = 1
    elif m < 99:
        imp_count = 3
    else:
        imp_count = 4

    msg = bytearray()

    # field  1: id (string)
    msg += _fld_str(1, f"bid-{i:06d}")

    # field  2: user
    demographic = _fld_len(1, _enc_decimal(i % 100 + 1, 500_000_000))
    user_ext = _fld_len(1, demographic)
    user = _fld_str(1, f"user-{i % 1000}") + _fld_len(2, user_ext)
    msg += _fld_len(2, user)

    # field  3: device  (geo sub-message)
    geo = (
        _fld_len(1, _enc_decimal(37 + i % 5, 0))   # lat
        + _fld_len(2, _enc_decimal(-122 + i % 5, 0))  # lon
        + _fld_u32(3, 1)                              # type
        + _fld_str(4, _countries[i % len(_countries)])
        + _fld_str(5, _regions[i % len(_regions)])
        + _fld_str(6, "CityName")
        + _fld_sint32(8, -5 + i % 10)                 # utcoffset (sint32)
    )
    device = (
        _fld_str(1, f"ifa-{i % 500:04x}-{i % 100:02x}")
        + _fld_str(2, "Mozilla/5.0 (compatible)")
        + _fld_str(3, f"192.168.{i % 256}.{i % 128}")
        + _fld_len(4, geo)
        + _fld_u32(5, 1920)  # w
        + _fld_u32(6, 1080)  # h
    )
    msg += _fld_len(3, device)

    # field  4: imp[] (repeated)
    for j in range(imp_count):
        # deal count distribution
        dm = (i + j) % 100
        if dm < 2:
            deal_count = 0
        elif dm < 63:
            deal_count = 1
        elif dm < 84:
            deal_count = 2
        elif dm < 90:
            deal_count = 3
        else:
            deal_count = 4

        # pmp.deals
        pmp_parts = bytearray(_fld_u32(1, 0))  # private_auction = 0
        for k in range(deal_count):
            deal_ext = (
                _fld_u32(1, k % 3 + 1)  # adspottype
                + _fld_u32(2, 1)        # guaranteed
                + _fld_u32(3, 1)        # must_bid (bool)
            )
            deal = (
                _fld_str(1, f"deal-{i:06d}-{j}-{k}")
                + _fld_len(2, _enc_decimal(k + 1, 0))   # bidfloor
                + _fld_u32(3, k % 3 + 1)                # at
                + _fld_len(6, deal_ext)
            )
            pmp_parts += _fld_len(2, deal)

        # banner / video (mutually exclusive real dimensions)
        if (i + j) % 100 < 55:
            # video-primary
            vs = _video_sz[(i + j) % len(_video_sz)]
            banner = _fld_u32(2, 0) + _fld_u32(3, 0)
            video = (
                _fld_str(1, "video/mp4")
                + _fld_u32(2, vs[0])
                + _fld_u32(3, vs[1])
                + _fld_u32(4, 5)    # minduration
                + _fld_u32(5, 30)   # maxduration
            )
        else:
            # banner-primary
            bs = _banner_sz[(i + j) % len(_banner_sz)]
            banner = (
                _fld_str(1, "image/jpeg")
                + _fld_u32(2, bs[0])
                + _fld_u32(3, bs[1])
            )
            video = _fld_u32(2, 0) + _fld_u32(3, 0)

        imp = (
            _fld_str(1, f"imp-{i:06d}-{j}")
            + _fld_len(3, bytes(pmp_parts))
            + _fld_len(4, banner)
            + _fld_len(5, video)
            + _fld_len(6, _enc_decimal(i % 10 + 1, 500_000_000))  # bidfloor
        )
        msg += _fld_len(4, imp)

    # field  5: site
    site_pub = _fld_str(1, _pub_ids[i % len(_pub_ids)])
    site = (
        _fld_str(1, f"site-{i % 50:03d}")
        + _fld_str(2, _site_names[i % len(_site_names)])
        + _fld_len(3, site_pub)
    )
    msg += _fld_len(5, site)

    # field  6: cur (repeated string, packed as individual fields)
    msg += _fld_str(6, "USD")

    # field  9: at
    msg += _fld_u32(9, 2)

    # field 10: timestamp
    msg += _fld_len(10, _enc_timestamp(1735700000 + i))

    # field 11: technicalprovider
    tp = _fld_u32(1, _tp_ids[i % len(_tp_ids)]) + _fld_str(2, "tp-name")
    msg += _fld_len(11, tp)

    # field 12: dooh
    dooh_pub = _fld_str(1, _pub_ids[i % len(_pub_ids)]) + _fld_str(2, "DoohPub")
    dooh = (
        _fld_str(1, f"dooh-{i % 200:04d}")
        + _fld_str(2, "DoohVenue")
        + _fld_str(3, "outdoor")
        + _fld_u32(4, 1)
        + _fld_len(5, dooh_pub)
        + _fld_float(6, float(i % 1000) / 10.0)  # audience
    )
    msg += _fld_len(12, dooh)

    return bytes(msg)


@pytest.fixture
def bid_request_proto() -> str:
    """BidRequest proto filename (resolved via bid_request_import_paths)."""
    return "BidRequest.proto"


@pytest.fixture
def bid_request_import_paths() -> list[str]:
    """Import paths required to resolve BidRequest.proto imports."""
    return [_PROTO_SAMPLES_DIR]


@pytest.fixture
def bid_request_hyper_type(bid_request_import_paths):
    """HyperType for BidRequestEvent, configured for AppendRaw benchmarks."""
    from pybufarrow import HyperType

    ht = HyperType("BidRequest.proto", "BidRequestEvent",
                   import_paths=bid_request_import_paths)
    yield ht
    ht.close()
