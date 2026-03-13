"""Entity lineage — track predecessor/successor relationships.

When entities merge, split, or are superseded, lineage records preserve
the full genealogy. This enables questions like "what did this entity
come from?" and "what replaced this entity?"
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timezone
from enum import Enum
from typing import Any


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


class LineageType(str, Enum):
    """Types of lineage relationships."""

    DERIVED_FROM = "derived_from"    # entity was created based on another
    SUPERSEDES = "supersedes"        # entity replaces another
    MERGED_INTO = "merged_into"      # entity was merged into another
    SPLIT_FROM = "split_from"        # entity was split from another


@dataclass
class LineageRecord:
    """A directed lineage relationship between two entities."""

    entity_id: str         # the subject entity
    related_id: str        # the predecessor/successor
    lineage_type: LineageType
    recorded_at: str = field(default_factory=_now_iso)
    metadata: dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        return {
            "entity_id": self.entity_id,
            "related_id": self.related_id,
            "lineage_type": self.lineage_type.value,
            "recorded_at": self.recorded_at,
            "metadata": self.metadata,
        }

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> LineageRecord:
        return cls(
            entity_id=data["entity_id"],
            related_id=data["related_id"],
            lineage_type=LineageType(data["lineage_type"]),
            recorded_at=data.get("recorded_at", ""),
            metadata=data.get("metadata", {}),
        )


@dataclass
class LineageIndex:
    """In-memory index for lineage lookups."""

    _records: list[LineageRecord] = field(default_factory=list)
    _by_entity: dict[str, list[LineageRecord]] = field(default_factory=dict)
    _by_related: dict[str, list[LineageRecord]] = field(default_factory=dict)

    def add(self, record: LineageRecord) -> None:
        self._records.append(record)
        self._by_entity.setdefault(record.entity_id, []).append(record)
        self._by_related.setdefault(record.related_id, []).append(record)

    def predecessors(self, entity_id: str) -> list[LineageRecord]:
        """Find what this entity came from (derived_from, split_from)."""
        return [
            r for r in self._by_entity.get(entity_id, [])
            if r.lineage_type in (LineageType.DERIVED_FROM, LineageType.SPLIT_FROM)
        ]

    def successors(self, entity_id: str) -> list[LineageRecord]:
        """Find what replaced or continued this entity.

        Checks two directions:
        - _by_entity: records where entity_id MERGED_INTO related_id (related is successor)
        - _by_related: records where entity_id SUPERSEDES this entity (entity_id is successor)
        """
        results: list[LineageRecord] = []
        # Entity was merged into something → related_id is the successor
        for r in self._by_entity.get(entity_id, []):
            if r.lineage_type == LineageType.MERGED_INTO:
                results.append(r)
        # Something supersedes this entity → entity_id is the successor
        for r in self._by_related.get(entity_id, []):
            if r.lineage_type == LineageType.SUPERSEDES:
                results.append(r)
        return results

    def full_lineage(self, entity_id: str) -> list[LineageRecord]:
        """All lineage records involving this entity (as subject or related)."""
        records = list(self._by_entity.get(entity_id, []))
        records.extend(self._by_related.get(entity_id, []))
        return records

    def trace_ancestry(self, entity_id: str, max_depth: int = 10) -> list[str]:
        """Walk back through predecessors to find the full ancestry chain."""
        chain: list[str] = []
        seen: set[str] = {entity_id}
        current = entity_id

        for _ in range(max_depth):
            preds = self.predecessors(current)
            if not preds:
                break
            # Follow the first predecessor (merge/split may have multiple)
            ancestor = preds[0].related_id
            if ancestor in seen:
                break
            seen.add(ancestor)
            chain.append(ancestor)
            current = ancestor

        return chain

    def all_records(self) -> list[LineageRecord]:
        return list(self._records)

    def to_list(self) -> list[dict[str, Any]]:
        return [r.to_dict() for r in self._records]

    @classmethod
    def from_list(cls, records: list[dict[str, Any]]) -> LineageIndex:
        index = cls()
        for rdict in records:
            index.add(LineageRecord.from_dict(rdict))
        return index
