"""Runtime state — the live computed view of the system.

RuntimeState is rebuilt from the registry, edge index, and variable store
each time it's requested. It's never persisted — it's always derived.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any

from ontologia.entity.identity import EntityIdentity, LifecycleStatus
from ontologia.structure.edges import EdgeIndex, HierarchyEdge, RelationEdge


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


@dataclass
class RuntimeState:
    """Live computed view of the system at the current moment."""

    timestamp: str = field(default_factory=_now_iso)
    active_entities: dict[str, EntityIdentity] = field(default_factory=dict)
    active_hierarchy: list[HierarchyEdge] = field(default_factory=list)
    active_relations: list[RelationEdge] = field(default_factory=list)
    resolved_variables: dict[str, Any] = field(default_factory=dict)
    entity_count_by_type: dict[str, int] = field(default_factory=dict)
    entity_count_by_status: dict[str, int] = field(default_factory=dict)

    def summary(self) -> dict[str, Any]:
        return {
            "timestamp": self.timestamp,
            "total_entities": len(self.active_entities),
            "by_type": dict(self.entity_count_by_type),
            "by_status": dict(self.entity_count_by_status),
            "active_hierarchy_edges": len(self.active_hierarchy),
            "active_relation_edges": len(self.active_relations),
            "resolved_variable_count": len(self.resolved_variables),
        }


def compute_runtime(
    entities: dict[str, EntityIdentity],
    edge_index: EdgeIndex,
    resolved_variables: dict[str, Any] | None = None,
    at: str | None = None,
) -> RuntimeState:
    """Compute the live runtime state from all data sources.

    Args:
        entities: All entities keyed by UID.
        edge_index: The edge index with hierarchy and relation edges.
        resolved_variables: Pre-resolved variable bindings.
        at: Temporal filter — if provided, only include edges active at this time.

    Returns:
        Computed RuntimeState.
    """
    check_time = at or _now_iso()

    # Filter active entities
    active: dict[str, EntityIdentity] = {}
    type_counts: dict[str, int] = {}
    status_counts: dict[str, int] = {}

    for uid, entity in entities.items():
        status_key = entity.lifecycle_status.value
        status_counts[status_key] = status_counts.get(status_key, 0) + 1

        type_key = entity.entity_type.value
        type_counts[type_key] = type_counts.get(type_key, 0) + 1

        if entity.lifecycle_status == LifecycleStatus.ACTIVE:
            active[uid] = entity

    # Filter active edges
    active_hierarchy = [
        e for e in edge_index.all_hierarchy_edges()
        if e.is_active(check_time)
    ]
    active_relations = [
        e for e in edge_index.all_relation_edges()
        if e.is_active(check_time)
    ]

    return RuntimeState(
        timestamp=check_time,
        active_entities=active,
        active_hierarchy=active_hierarchy,
        active_relations=active_relations,
        resolved_variables=resolved_variables or {},
        entity_count_by_type=type_counts,
        entity_count_by_status=status_counts,
    )
