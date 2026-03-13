"""Graph traversal functions for the structural hierarchy.

Provides temporal-aware traversal: you can walk the hierarchy as it
existed at any point in time by passing an `at` timestamp.
"""

from __future__ import annotations

from collections import deque

from ontologia.structure.edges import EdgeIndex, HierarchyEdge, RelationEdge


def macro_descent(
    index: EdgeIndex,
    root_id: str,
    at: str | None = None,
    max_depth: int | None = None,
) -> list[str]:
    """Top-down traversal: return all descendants of root_id.

    Returns entity IDs in breadth-first order, not including root_id itself.
    """
    visited: list[str] = []
    seen: set[str] = {root_id}
    queue: deque[tuple[str, int]] = deque([(root_id, 0)])

    while queue:
        current, depth = queue.popleft()
        if max_depth is not None and depth >= max_depth:
            continue
        for edge in index.children(current, at=at):
            if edge.child_id not in seen:
                seen.add(edge.child_id)
                visited.append(edge.child_id)
                queue.append((edge.child_id, depth + 1))

    return visited


def micro_ascent(
    index: EdgeIndex,
    entity_id: str,
    at: str | None = None,
) -> list[str]:
    """Bottom-up traversal: return the ancestor chain from entity to root.

    Returns [parent, grandparent, ...] in ascending order.
    """
    ancestors: list[str] = []
    seen: set[str] = {entity_id}
    current = entity_id

    while True:
        parent_edge = index.parent(current, at=at)
        if parent_edge is None:
            break
        if parent_edge.parent_id in seen:
            break  # cycle protection
        seen.add(parent_edge.parent_id)
        ancestors.append(parent_edge.parent_id)
        current = parent_edge.parent_id

    return ancestors


def dependency_trace(
    index: EdgeIndex,
    entity_id: str,
    relation_type: str | None = None,
    at: str | None = None,
    max_depth: int | None = None,
) -> list[str]:
    """Follow outgoing relation edges transitively.

    Returns all entities reachable via outgoing relations of the given type.
    """
    visited: list[str] = []
    seen: set[str] = {entity_id}
    queue: deque[tuple[str, int]] = deque([(entity_id, 0)])

    while queue:
        current, depth = queue.popleft()
        if max_depth is not None and depth >= max_depth:
            continue
        for edge in index.outgoing_relations(current, relation_type=relation_type, at=at):
            if edge.target_id not in seen:
                seen.add(edge.target_id)
                visited.append(edge.target_id)
                queue.append((edge.target_id, depth + 1))

    return visited


def reverse_dependency_trace(
    index: EdgeIndex,
    entity_id: str,
    relation_type: str | None = None,
    at: str | None = None,
    max_depth: int | None = None,
) -> list[str]:
    """Follow incoming relation edges transitively (dependents/consumers)."""
    visited: list[str] = []
    seen: set[str] = {entity_id}
    queue: deque[tuple[str, int]] = deque([(entity_id, 0)])

    while queue:
        current, depth = queue.popleft()
        if max_depth is not None and depth >= max_depth:
            continue
        for edge in index.incoming_relations(current, relation_type=relation_type, at=at):
            if edge.source_id not in seen:
                seen.add(edge.source_id)
                visited.append(edge.source_id)
                queue.append((edge.source_id, depth + 1))

    return visited


def find_roots(index: EdgeIndex, at: str | None = None) -> list[str]:
    """Find all hierarchy roots (entities with no active parent)."""
    all_children: set[str] = set()
    all_parents: set[str] = set()

    for edge in index.all_hierarchy_edges():
        if edge.is_active(at):
            all_parents.add(edge.parent_id)
            all_children.add(edge.child_id)

    return sorted(all_parents - all_children)


def find_leaves(index: EdgeIndex, at: str | None = None) -> list[str]:
    """Find all hierarchy leaves (entities with no active children)."""
    all_children: set[str] = set()
    all_parents: set[str] = set()

    for edge in index.all_hierarchy_edges():
        if edge.is_active(at):
            all_parents.add(edge.parent_id)
            all_children.add(edge.child_id)

    return sorted(all_children - all_parents)


def subtree_size(index: EdgeIndex, root_id: str, at: str | None = None) -> int:
    """Count the number of descendants (not including root)."""
    return len(macro_descent(index, root_id, at=at))
