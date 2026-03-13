"""Temporal naming system — mutable display names with full alias history.

An entity's name can change over time (renames, rebrandings, merges), but
every name it ever held is preserved with temporal validity windows. This
enables resolving historical references ("what was this called in March?").
"""

from __future__ import annotations

import json
import re
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _slugify(name: str) -> str:
    """Convert a display name to a URL-safe slug.

    Preserves double-hyphens (``--``) which are semantic separators
    in the ORGANVM naming convention.
    """
    slug = name.lower().strip()
    slug = re.sub(r"[^a-z0-9\-]+", "-", slug)
    # Collapse runs of 3+ hyphens to 2, and single runs to 1,
    # but preserve exactly 2 (the semantic separator).
    slug = re.sub(r"-{3,}", "--", slug)
    # Collapse single hyphens that aren't part of a double
    # by replacing lone hyphens (not adjacent to another hyphen)
    parts = re.split(r"(--)", slug)
    result: list[str] = []
    for part in parts:
        if part == "--":
            result.append(part)
        else:
            result.append(re.sub(r"-+", "-", part))
    return "".join(result).strip("-")


@dataclass
class NameRecord:
    """A single name binding for an entity with temporal validity.

    A name record is valid from valid_from until valid_to. If valid_to is
    None, the name is currently active. An entity can have multiple active
    names (aliases), but exactly one should be marked is_primary=True.
    """

    entity_id: str
    display_name: str
    slug: str
    valid_from: str
    valid_to: str | None = None
    is_primary: bool = True
    source: str = "system"

    def is_active(self, at: str | None = None) -> bool:
        """Check if this name is active at a given time (default: now)."""
        check_time = at or _now_iso()
        if self.valid_from > check_time:
            return False
        return not (self.valid_to is not None and self.valid_to <= check_time)

    def to_dict(self) -> dict[str, Any]:
        return {
            "entity_id": self.entity_id,
            "display_name": self.display_name,
            "slug": self.slug,
            "valid_from": self.valid_from,
            "valid_to": self.valid_to,
            "is_primary": self.is_primary,
            "source": self.source,
        }

    def to_jsonl(self) -> str:
        return json.dumps(self.to_dict(), separators=(",", ":"))

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> NameRecord:
        return cls(
            entity_id=data["entity_id"],
            display_name=data["display_name"],
            slug=data.get("slug", _slugify(data["display_name"])),
            valid_from=data.get("valid_from", ""),
            valid_to=data.get("valid_to"),
            is_primary=data.get("is_primary", True),
            source=data.get("source", "system"),
        )


@dataclass
class NameIndex:
    """In-memory index of all name records for fast lookup.

    Maintains forward (entity_id → names) and reverse (slug → entity_ids)
    indexes. The JSONL file is the source of truth; this is a query cache.
    """

    _by_entity: dict[str, list[NameRecord]] = field(default_factory=dict)
    _by_slug: dict[str, list[str]] = field(default_factory=dict)
    _by_display: dict[str, list[str]] = field(default_factory=dict)

    def add(self, record: NameRecord) -> None:
        """Index a name record."""
        self._by_entity.setdefault(record.entity_id, []).append(record)

        if record.slug not in self._by_slug:
            self._by_slug[record.slug] = []
        if record.entity_id not in self._by_slug[record.slug]:
            self._by_slug[record.slug].append(record.entity_id)

        display_lower = record.display_name.lower()
        if display_lower not in self._by_display:
            self._by_display[display_lower] = []
        if record.entity_id not in self._by_display[display_lower]:
            self._by_display[display_lower].append(record.entity_id)

    def current_name(self, entity_id: str, at: str | None = None) -> NameRecord | None:
        """Get the current primary name for an entity."""
        records = self._by_entity.get(entity_id, [])
        for record in reversed(records):
            if record.is_primary and record.is_active(at):
                return record
        # Fall back to any active name
        for record in reversed(records):
            if record.is_active(at):
                return record
        return None

    def all_names(self, entity_id: str) -> list[NameRecord]:
        """Get all name records (historical + current) for an entity."""
        return list(self._by_entity.get(entity_id, []))

    def active_names(self, entity_id: str, at: str | None = None) -> list[NameRecord]:
        """Get all currently active names for an entity."""
        return [r for r in self._by_entity.get(entity_id, []) if r.is_active(at)]

    def resolve_slug(self, slug: str) -> list[str]:
        """Find entity IDs that have (or had) this slug."""
        return list(self._by_slug.get(slug, []))

    def resolve_display_name(self, name: str) -> list[str]:
        """Find entity IDs that have (or had) this display name."""
        return list(self._by_display.get(name.lower(), []))

    def entity_count(self) -> int:
        """Number of distinct entities with name records."""
        return len(self._by_entity)


def add_name(
    index: NameIndex,
    entity_id: str,
    display_name: str,
    is_primary: bool = True,
    source: str = "system",
    valid_from: str | None = None,
) -> NameRecord:
    """Create and index a new name record for an entity.

    If is_primary=True, retires any existing primary name for the entity.

    Returns:
        The new NameRecord.
    """
    now = valid_from or _now_iso()

    if is_primary:
        # Retire existing primary names
        for existing in index.active_names(entity_id):
            if existing.is_primary:
                existing.valid_to = now
                existing.is_primary = False

    record = NameRecord(
        entity_id=entity_id,
        display_name=display_name,
        slug=_slugify(display_name),
        valid_from=now,
        is_primary=is_primary,
        source=source,
    )
    index.add(record)
    return record


def retire_name(
    index: NameIndex,
    entity_id: str,
    display_name: str,
    retired_at: str | None = None,
) -> bool:
    """Retire a specific active name for an entity.

    Returns True if a name was retired, False if not found.
    """
    now = retired_at or _now_iso()
    for record in index.active_names(entity_id):
        if record.display_name == display_name:
            record.valid_to = now
            return True
    return False
