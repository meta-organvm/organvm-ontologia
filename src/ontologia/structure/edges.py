"""Temporal hierarchy and relation edges.

Every edge has a validity window (valid_from, valid_to). When an entity
moves in the hierarchy, the old edge is retired and a new one created.
This preserves the complete structural history for temporal queries.
"""

from __future__ import annotations

import json
from dataclasses import dataclass, field
from datetime import datetime, timezone
from enum import Enum
from typing import Any


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


class RelationType(str, Enum):
    """Known relation types between entities."""

    DEPENDS_ON = "depends_on"
    PRODUCES_FOR = "produces_for"
    CONSUMES_FROM = "consumes_from"
    SUBSCRIBES_TO = "subscribes_to"
    DERIVED_FROM = "derived_from"
    SUPERSEDES = "supersedes"
    MERGED_INTO = "merged_into"
    SPLIT_FROM = "split_from"


@dataclass
class HierarchyEdge:
    """A parent→child edge in the structural hierarchy.

    Examples: organ→repo, repo→module, module→document.
    """

    parent_id: str
    child_id: str
    valid_from: str
    valid_to: str | None = None
    metadata: dict[str, Any] = field(default_factory=dict)

    def is_active(self, at: str | None = None) -> bool:
        check = at or _now_iso()
        if self.valid_from > check:
            return False
        if self.valid_to is not None and self.valid_to <= check:
            return False
        return True

    def to_dict(self) -> dict[str, Any]:
        d: dict[str, Any] = {
            "parent_id": self.parent_id,
            "child_id": self.child_id,
            "valid_from": self.valid_from,
        }
        if self.valid_to is not None:
            d["valid_to"] = self.valid_to
        if self.metadata:
            d["metadata"] = self.metadata
        return d

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> HierarchyEdge:
        return cls(
            parent_id=data["parent_id"],
            child_id=data["child_id"],
            valid_from=data.get("valid_from", ""),
            valid_to=data.get("valid_to"),
            metadata=data.get("metadata", {}),
        )


@dataclass
class RelationEdge:
    """A typed, directed relation between two entities.

    Unlike hierarchy edges, relation edges can form arbitrary graphs
    (cycles allowed for semantic relations like depends_on).
    """

    source_id: str
    target_id: str
    relation_type: str
    valid_from: str
    valid_to: str | None = None
    metadata: dict[str, Any] = field(default_factory=dict)

    def is_active(self, at: str | None = None) -> bool:
        check = at or _now_iso()
        if self.valid_from > check:
            return False
        if self.valid_to is not None and self.valid_to <= check:
            return False
        return True

    def to_dict(self) -> dict[str, Any]:
        d: dict[str, Any] = {
            "source_id": self.source_id,
            "target_id": self.target_id,
            "relation_type": self.relation_type,
            "valid_from": self.valid_from,
        }
        if self.valid_to is not None:
            d["valid_to"] = self.valid_to
        if self.metadata:
            d["metadata"] = self.metadata
        return d

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> RelationEdge:
        return cls(
            source_id=data["source_id"],
            target_id=data["target_id"],
            relation_type=data.get("relation_type", ""),
            valid_from=data.get("valid_from", ""),
            valid_to=data.get("valid_to"),
            metadata=data.get("metadata", {}),
        )


@dataclass
class EdgeIndex:
    """In-memory index for fast edge lookups.

    Maintains both hierarchy and relation edges with forward/reverse indexes.
    """

    _hierarchy: list[HierarchyEdge] = field(default_factory=list)
    _relations: list[RelationEdge] = field(default_factory=list)
    # Forward/reverse indexes for hierarchy
    _children_of: dict[str, list[HierarchyEdge]] = field(default_factory=dict)
    _parent_of: dict[str, list[HierarchyEdge]] = field(default_factory=dict)
    # Forward/reverse indexes for relations
    _outgoing: dict[str, list[RelationEdge]] = field(default_factory=dict)
    _incoming: dict[str, list[RelationEdge]] = field(default_factory=dict)

    def add_hierarchy(self, edge: HierarchyEdge) -> None:
        self._hierarchy.append(edge)
        self._children_of.setdefault(edge.parent_id, []).append(edge)
        self._parent_of.setdefault(edge.child_id, []).append(edge)

    def add_relation(self, edge: RelationEdge) -> None:
        self._relations.append(edge)
        self._outgoing.setdefault(edge.source_id, []).append(edge)
        self._incoming.setdefault(edge.target_id, []).append(edge)

    def children(self, parent_id: str, at: str | None = None) -> list[HierarchyEdge]:
        """Active children of a parent at a given time."""
        return [e for e in self._children_of.get(parent_id, []) if e.is_active(at)]

    def parent(self, child_id: str, at: str | None = None) -> HierarchyEdge | None:
        """Active parent of a child (should be at most one in a tree)."""
        active = [e for e in self._parent_of.get(child_id, []) if e.is_active(at)]
        return active[-1] if active else None

    def outgoing_relations(
        self,
        entity_id: str,
        relation_type: str | None = None,
        at: str | None = None,
    ) -> list[RelationEdge]:
        """Active outgoing relations from an entity."""
        edges = [e for e in self._outgoing.get(entity_id, []) if e.is_active(at)]
        if relation_type:
            edges = [e for e in edges if e.relation_type == relation_type]
        return edges

    def incoming_relations(
        self,
        entity_id: str,
        relation_type: str | None = None,
        at: str | None = None,
    ) -> list[RelationEdge]:
        """Active incoming relations to an entity."""
        edges = [e for e in self._incoming.get(entity_id, []) if e.is_active(at)]
        if relation_type:
            edges = [e for e in edges if e.relation_type == relation_type]
        return edges

    def all_hierarchy_edges(self) -> list[HierarchyEdge]:
        return list(self._hierarchy)

    def all_relation_edges(self) -> list[RelationEdge]:
        return list(self._relations)

    def retire_hierarchy(
        self,
        parent_id: str,
        child_id: str,
        at: str | None = None,
    ) -> bool:
        """Retire the active hierarchy edge between parent and child."""
        now = at or _now_iso()
        for edge in self._children_of.get(parent_id, []):
            if edge.child_id == child_id and edge.is_active():
                edge.valid_to = now
                return True
        return False

    def retire_relation(
        self,
        source_id: str,
        target_id: str,
        relation_type: str,
        at: str | None = None,
    ) -> bool:
        """Retire an active relation edge."""
        now = at or _now_iso()
        for edge in self._outgoing.get(source_id, []):
            if (
                edge.target_id == target_id
                and edge.relation_type == relation_type
                and edge.is_active()
            ):
                edge.valid_to = now
                return True
        return False

    def to_dict(self) -> dict[str, Any]:
        return {
            "hierarchy": [e.to_dict() for e in self._hierarchy],
            "relations": [e.to_dict() for e in self._relations],
        }

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> EdgeIndex:
        index = cls()
        for edict in data.get("hierarchy", []):
            index.add_hierarchy(HierarchyEdge.from_dict(edict))
        for rdict in data.get("relations", []):
            index.add_relation(RelationEdge.from_dict(rdict))
        return index
