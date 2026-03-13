"""Cluster detection — find groups of tightly-coupled entities.

Identifies clusters based on interaction frequency (shared edges,
shared events, co-occurring changes). Useful for discovering implicit
module boundaries and suggesting structural reorganization.
"""

from __future__ import annotations

from dataclasses import dataclass

from ontologia.inference.engine import InferenceResult, InferenceType
from ontologia.structure.edges import EdgeIndex


@dataclass
class Cluster:
    """A group of entities that interact frequently."""

    entity_ids: list[str]
    cohesion: float  # 0.0 to 1.0 — internal connection density
    label: str = ""


def detect_clusters_from_relations(
    edge_index: EdgeIndex,
    relation_type: str | None = None,
    min_cluster_size: int = 2,
) -> list[Cluster]:
    """Detect clusters based on shared relation edges.

    Uses connected component analysis on the relation graph.
    """
    # Build adjacency from active relations
    adj: dict[str, set[str]] = {}
    for edge in edge_index.all_relation_edges():
        if not edge.is_active():
            continue
        if relation_type and edge.relation_type != relation_type:
            continue
        adj.setdefault(edge.source_id, set()).add(edge.target_id)
        adj.setdefault(edge.target_id, set()).add(edge.source_id)

    # Find connected components
    visited: set[str] = set()
    clusters: list[Cluster] = []

    for node in adj:
        if node in visited:
            continue
        # BFS
        component: list[str] = []
        queue = [node]
        while queue:
            current = queue.pop(0)
            if current in visited:
                continue
            visited.add(current)
            component.append(current)
            for neighbor in adj.get(current, set()):
                if neighbor not in visited:
                    queue.append(neighbor)

        if len(component) >= min_cluster_size:
            # Cohesion = actual edges / possible edges
            n = len(component)
            max_edges = n * (n - 1) / 2
            actual = sum(
                1 for i, a in enumerate(component)
                for b in component[i + 1:]
                if b in adj.get(a, set())
            )
            cohesion = actual / max_edges if max_edges > 0 else 0.0
            clusters.append(Cluster(
                entity_ids=sorted(component),
                cohesion=cohesion,
            ))

    return sorted(clusters, key=lambda c: -c.cohesion)


def clusters_to_inferences(clusters: list[Cluster]) -> list[InferenceResult]:
    """Convert detected clusters into inference results."""
    return [
        InferenceResult(
            inference_type=InferenceType.CLUSTER,
            entity_ids=c.entity_ids,
            score=c.cohesion,
            description=f"Cluster of {len(c.entity_ids)} entities with {c.cohesion:.2f} cohesion",
        )
        for c in clusters
    ]
