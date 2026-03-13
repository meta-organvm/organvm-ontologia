"""Impact propagation — upward, downward, and lateral blast radius.

When an entity changes, the impact may propagate through the hierarchy
(upward to parents, downward to children) and through relations (lateral
to dependents/dependencies).
"""

from __future__ import annotations

from collections import deque
from dataclasses import dataclass, field

from ontologia.structure.edges import EdgeIndex


@dataclass
class PropagationPath:
    """A single propagation path from source to affected entity."""

    source_id: str
    target_id: str
    direction: str  # "upward", "downward", "lateral"
    distance: int
    path: list[str] = field(default_factory=list)


def propagate_upward(
    index: EdgeIndex,
    entity_id: str,
    max_depth: int | None = None,
) -> list[PropagationPath]:
    """Find upward impact through the hierarchy (to parents/ancestors)."""
    paths: list[PropagationPath] = []
    seen: set[str] = {entity_id}
    queue: deque[tuple[str, int, list[str]]] = deque([(entity_id, 0, [entity_id])])

    while queue:
        current, depth, trail = queue.popleft()
        if max_depth is not None and depth >= max_depth:
            continue
        parent_edge = index.parent(current)
        if parent_edge and parent_edge.parent_id not in seen:
            seen.add(parent_edge.parent_id)
            new_trail = trail + [parent_edge.parent_id]
            paths.append(PropagationPath(
                source_id=entity_id,
                target_id=parent_edge.parent_id,
                direction="upward",
                distance=depth + 1,
                path=new_trail,
            ))
            queue.append((parent_edge.parent_id, depth + 1, new_trail))

    return paths


def propagate_downward(
    index: EdgeIndex,
    entity_id: str,
    max_depth: int | None = None,
) -> list[PropagationPath]:
    """Find downward impact through the hierarchy (to children/descendants)."""
    paths: list[PropagationPath] = []
    seen: set[str] = {entity_id}
    queue: deque[tuple[str, int, list[str]]] = deque([(entity_id, 0, [entity_id])])

    while queue:
        current, depth, trail = queue.popleft()
        if max_depth is not None and depth >= max_depth:
            continue
        for child_edge in index.children(current):
            if child_edge.child_id not in seen:
                seen.add(child_edge.child_id)
                new_trail = trail + [child_edge.child_id]
                paths.append(PropagationPath(
                    source_id=entity_id,
                    target_id=child_edge.child_id,
                    direction="downward",
                    distance=depth + 1,
                    path=new_trail,
                ))
                queue.append((child_edge.child_id, depth + 1, new_trail))

    return paths


def propagate_lateral(
    index: EdgeIndex,
    entity_id: str,
    relation_type: str | None = None,
    max_depth: int | None = None,
) -> list[PropagationPath]:
    """Find lateral impact through relation edges (dependents)."""
    paths: list[PropagationPath] = []
    seen: set[str] = {entity_id}
    queue: deque[tuple[str, int, list[str]]] = deque([(entity_id, 0, [entity_id])])

    while queue:
        current, depth, trail = queue.popleft()
        if max_depth is not None and depth >= max_depth:
            continue
        for edge in index.incoming_relations(current, relation_type=relation_type):
            if edge.source_id not in seen:
                seen.add(edge.source_id)
                new_trail = trail + [edge.source_id]
                paths.append(PropagationPath(
                    source_id=entity_id,
                    target_id=edge.source_id,
                    direction="lateral",
                    distance=depth + 1,
                    path=new_trail,
                ))
                queue.append((edge.source_id, depth + 1, new_trail))

    return paths


def full_blast_radius(
    index: EdgeIndex,
    entity_id: str,
    max_depth: int = 3,
) -> list[PropagationPath]:
    """Compute the full blast radius: upward + downward + lateral."""
    paths: list[PropagationPath] = []
    paths.extend(propagate_upward(index, entity_id, max_depth=max_depth))
    paths.extend(propagate_downward(index, entity_id, max_depth=max_depth))
    paths.extend(propagate_lateral(index, entity_id, max_depth=max_depth))
    return paths
