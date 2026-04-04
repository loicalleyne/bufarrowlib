"""Memory leak tests — verify RSS doesn't grow under repeated append/flush cycles."""

from __future__ import annotations

import gc
import os

import pytest

from pybufarrow import HyperType, Transcoder

from .conftest import encode_test_msg


def _rss_kb() -> int:
    """Return current process RSS in kilobytes (Linux only)."""
    try:
        with open(f"/proc/{os.getpid()}/status") as f:
            for line in f:
                if line.startswith("VmRSS:"):
                    return int(line.split()[1])
    except (FileNotFoundError, OSError):
        pytest.skip("RSS measurement requires /proc (Linux)")
    return 0


class TestMemory:
    """Test for memory leaks under repeated append/flush cycles."""

    def test_append_flush_no_leak(self, test_proto):
        """Repeated append+flush cycles should not grow RSS significantly."""
        with HyperType(test_proto, "TestMsg") as ht:
            with Transcoder.from_proto_file(
                test_proto, "TestMsg", hyper_type=ht
            ) as tc:
                # Warm up — let allocators stabilize
                for _ in range(100):
                    tc.append(encode_test_msg("warmup", 1, 1.0, True))
                tc.flush()
                gc.collect()

                rss_before = _rss_kb()

                # Main loop — 200 cycles of 100 messages each
                for cycle in range(200):
                    for _ in range(100):
                        tc.append(encode_test_msg("test", cycle, float(cycle), True))
                    batch = tc.flush()
                    del batch

                gc.collect()
                rss_after = _rss_kb()

                # Allow up to 50 MB growth (generous threshold to avoid flaky tests)
                growth_kb = rss_after - rss_before
                assert growth_kb < 50_000, (
                    f"RSS grew by {growth_kb} KB ({growth_kb / 1024:.1f} MB) "
                    f"over 200 flush cycles — possible memory leak"
                )

    def test_transcoder_lifecycle_no_leak(self, test_proto):
        """Repeated create/close cycles should not leak handles."""
        with HyperType(test_proto, "TestMsg") as ht:
            # Warm up
            for _ in range(10):
                tc = Transcoder.from_proto_file(test_proto, "TestMsg", hyper_type=ht)
                tc.close()
            gc.collect()

            rss_before = _rss_kb()

            for _ in range(500):
                tc = Transcoder.from_proto_file(test_proto, "TestMsg", hyper_type=ht)
                tc.append(encode_test_msg("t", 1, 1.0, True))
                tc.flush()
                tc.close()

            gc.collect()
            rss_after = _rss_kb()

            growth_kb = rss_after - rss_before
            assert growth_kb < 50_000, (
                f"RSS grew by {growth_kb} KB ({growth_kb / 1024:.1f} MB) "
                f"over 500 transcoder create/close cycles — possible handle leak"
            )
