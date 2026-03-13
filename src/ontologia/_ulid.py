"""Minimal ULID generator using only stdlib.

ULID = 48-bit timestamp (ms) + 80-bit randomness, Crockford Base32 encoded.
Total: 26 characters, lexicographically sortable by creation time.
"""

from __future__ import annotations

import secrets
import time

# Crockford's Base32 alphabet (excludes I, L, O, U to avoid confusion)
_CROCKFORD = "0123456789ABCDEFGHJKMNPQRSTVWXYZ"


def _encode_base32(value: int, length: int) -> str:
    """Encode an integer as Crockford Base32 with fixed width."""
    chars: list[str] = []
    for _ in range(length):
        chars.append(_CROCKFORD[value & 0x1F])
        value >>= 5
    chars.reverse()
    return "".join(chars)


def generate_ulid(timestamp_ms: int | None = None) -> str:
    """Generate a ULID string (26 chars, Crockford Base32).

    Args:
        timestamp_ms: Optional explicit timestamp in milliseconds since epoch.
            If None, uses current time. Useful for deterministic testing.

    Returns:
        26-character ULID string.
    """
    if timestamp_ms is None:
        timestamp_ms = int(time.time() * 1000)

    # 48-bit timestamp → 10 Base32 chars
    ts_part = _encode_base32(timestamp_ms & 0xFFFFFFFFFFFF, 10)

    # 80-bit randomness → 16 Base32 chars
    rand_bytes = secrets.token_bytes(10)
    rand_int = int.from_bytes(rand_bytes, "big")
    rand_part = _encode_base32(rand_int, 16)

    return ts_part + rand_part
