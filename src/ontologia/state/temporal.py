"""Temporal queries — reconstruct structural views at past timestamps.

Answers questions like "what was the hierarchy on date X?" by filtering
temporal edges and entity lifecycle states against a given timestamp.
"""

from __future__ import annotations

from typing import Any

from ontologia.entity.identity import EntityIdentity
from ontologia.entity.naming import NameIndex, NameRecord
from ontologia.structure.edges import EdgeIndex, HierarchyEdge, RelationEdge


def hierarchy_at(
    edge_index: EdgeIndex,
    at: str,
) -> list[HierarchyEdge]:
    """Get the hierarchy edges that were active at a given timestamp."""
    return [e for e in edge_index.all_hierarchy_edges() if e.is_active(at)]


def relations_at(
    edge_index: EdgeIndex,
    at: str,
    relation_type: str | None = None,
) -> list[RelationEdge]:
    """Get relation edges that were active at a given timestamp."""
    edges = [e for e in edge_index.all_relation_edges() if e.is_active(at)]
    if relation_type:
        edges = [e for e in edges if e.relation_type == relation_type]
    return edges


def entity_names_at(
    name_index: NameIndex,
    entity_id: str,
    at: str,
) -> list[NameRecord]:
    """Get the names that were active for an entity at a given timestamp."""
    all_names = name_index.all_names(entity_id)
    active: list[NameRecord] = []
    for record in all_names:
        if record.valid_from > at:
            continue
        if record.valid_to is not None and record.valid_to <= at:
            continue
        active.append(record)
    return active


def primary_name_at(
    name_index: NameIndex,
    entity_id: str,
    at: str,
) -> NameRecord | None:
    """Get the primary name for an entity at a given timestamp."""
    names = entity_names_at(name_index, entity_id, at)
    primaries = [n for n in names if n.is_primary]
    return primaries[-1] if primaries else (names[-1] if names else None)


def children_at(
    edge_index: EdgeIndex,
    parent_id: str,
    at: str,
) -> list[str]:
    """Get the child entity IDs of a parent at a given timestamp."""
    return [e.child_id for e in edge_index.children(parent_id, at=at)]


def parent_at(
    edge_index: EdgeIndex,
    child_id: str,
    at: str,
) -> str | None:
    """Get the parent entity ID of a child at a given timestamp."""
    edge = edge_index.parent(child_id, at=at)
    return edge.parent_id if edge else None


def entity_state_at(
    entity: EntityIdentity,
    edge_index: EdgeIndex,
    name_index: NameIndex,
    at: str,
) -> dict[str, Any]:
    """Build a summary dict of an entity's state at a given timestamp.

    Returns entity identity + names + structural position at time `at`.
    """
    name = primary_name_at(name_index, entity.uid, at)
    par = parent_at(edge_index, entity.uid, at)
    kids = children_at(edge_index, entity.uid, at)
    outgoing = edge_index.outgoing_relations(entity.uid, at=at)
    incoming = edge_index.incoming_relations(entity.uid, at=at)

    return {
        "entity_id": entity.uid,
        "entity_type": entity.entity_type.value,
        "lifecycle_status": entity.lifecycle_status.value,
        "display_name": name.display_name if name else None,
        "parent_id": par,
        "child_ids": kids,
        "outgoing_relations": len(outgoing),
        "incoming_relations": len(incoming),
        "at": at,
    }


def structural_diff(
    edge_index: EdgeIndex,
    entity_id: str,
    at_a: str,
    at_b: str,
) -> dict[str, Any]:
    """Compare an entity's structural position between two timestamps.

    Returns a dict describing what changed: parent, children, relations.
    """
    parent_a = parent_at(edge_index, entity_id, at_a)
    parent_b = parent_at(edge_index, entity_id, at_b)
    children_a = set(children_at(edge_index, entity_id, at_a))
    children_b = set(children_at(edge_index, entity_id, at_b))

    out_a = {(e.target_id, e.relation_type) for e in edge_index.outgoing_relations(entity_id, at=at_a)}
    out_b = {(e.target_id, e.relation_type) for e in edge_index.outgoing_relations(entity_id, at=at_b)}

    return {
        "entity_id": entity_id,
        "at_a": at_a,
        "at_b": at_b,
        "parent_changed": parent_a != parent_b,
        "parent_a": parent_a,
        "parent_b": parent_b,
        "children_added": sorted(children_b - children_a),
        "children_removed": sorted(children_a - children_b),
        "relations_added": sorted(out_b - out_a),
        "relations_removed": sorted(out_a - out_b),
    }
