"""Tension detection — find structural and semantic stress points.

Tension indicators help identify areas of the system that may need
attention: naming conflicts, volatile entities, orphaned nodes,
diverging clusters.
"""

from __future__ import annotations

from dataclasses import dataclass
from enum import Enum

from ontologia.entity.identity import EntityIdentity, LifecycleStatus
from ontologia.entity.naming import NameIndex
from ontologia.structure.edges import EdgeIndex


class TensionType(str, Enum):
    NAMING_CONFLICT = "naming_conflict"
    ORPHAN = "orphan"
    VOLATILE = "volatile"
    DIVERGENCE = "divergence"
    OVERCOUPLED = "overcoupled"


@dataclass
class TensionIndicator:
    """A detected tension in the system."""

    tension_type: TensionType
    entity_ids: list[str]
    severity: float  # 0.0 to 1.0
    description: str = ""


def detect_orphans(
    entities: dict[str, EntityIdentity],
    edge_index: EdgeIndex,
) -> list[TensionIndicator]:
    """Find active entities with no hierarchy parent and no children.

    Orphans are entities that exist but aren't connected to the structure.
    """
    tensions: list[TensionIndicator] = []
    for uid, entity in entities.items():
        if entity.lifecycle_status != LifecycleStatus.ACTIVE:
            continue
        parent = edge_index.parent(uid)
        children = edge_index.children(uid)
        if parent is None and not children:
            tensions.append(TensionIndicator(
                tension_type=TensionType.ORPHAN,
                entity_ids=[uid],
                severity=0.5,
                description=f"Entity {uid} has no structural connections",
            ))
    return tensions


def detect_naming_conflicts(
    name_index: NameIndex,
) -> list[TensionIndicator]:
    """Find cases where multiple active entities share the same slug."""
    tensions: list[TensionIndicator] = []

    for slug, entity_ids in name_index._by_slug.items():
        # Count only entities with currently active names for this slug
        active_ids: list[str] = []
        for eid in entity_ids:
            for record in name_index.active_names(eid):
                if record.slug == slug:
                    active_ids.append(eid)
                    break
        if len(active_ids) > 1:
            tensions.append(TensionIndicator(
                tension_type=TensionType.NAMING_CONFLICT,
                entity_ids=active_ids,
                severity=0.7,
                description=f"Slug '{slug}' shared by {len(active_ids)} active entities",
            ))
    return tensions


def detect_overcoupling(
    edge_index: EdgeIndex,
    threshold: int = 5,
) -> list[TensionIndicator]:
    """Find entities with too many incoming relation edges."""
    tensions: list[TensionIndicator] = []
    # Count incoming relations per entity
    incoming_count: dict[str, int] = {}
    for edge in edge_index.all_relation_edges():
        if edge.is_active():
            incoming_count[edge.target_id] = incoming_count.get(edge.target_id, 0) + 1

    for entity_id, count in incoming_count.items():
        if count >= threshold:
            severity = min(1.0, count / (threshold * 2))
            tensions.append(TensionIndicator(
                tension_type=TensionType.OVERCOUPLED,
                entity_ids=[entity_id],
                severity=severity,
                description=f"Entity {entity_id} has {count} incoming relations",
            ))
    return tensions
