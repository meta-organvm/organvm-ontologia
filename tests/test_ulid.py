"""Tests for the ULID generator."""

from ontologia._ulid import generate_ulid


class TestGenerateUlid:
    def test_length_is_26(self):
        assert len(generate_ulid()) == 26

    def test_crockford_alphabet(self):
        valid = set("0123456789ABCDEFGHJKMNPQRSTVWXYZ")
        ulid = generate_ulid()
        assert all(c in valid for c in ulid), f"Invalid chars in {ulid}"

    def test_deterministic_with_timestamp(self):
        ts = 1710000000000
        u1 = generate_ulid(timestamp_ms=ts)
        u2 = generate_ulid(timestamp_ms=ts)
        # Same timestamp prefix (first 10 chars), different random suffix
        assert u1[:10] == u2[:10]
        # Random parts should differ (astronomically unlikely to match)
        assert u1[10:] != u2[10:]

    def test_lexicographic_ordering(self):
        ts1 = 1710000000000
        ts2 = 1710000001000  # 1 second later
        u1 = generate_ulid(timestamp_ms=ts1)
        u2 = generate_ulid(timestamp_ms=ts2)
        assert u1 < u2, "Later timestamp should produce lexicographically greater ULID"

    def test_uniqueness(self):
        ulids = {generate_ulid() for _ in range(100)}
        assert len(ulids) == 100
