"""Structural mutation operations — merge, split, relocate, reclassify.

Each operation is an orchestrated workflow that:
1. Creates/modifies entities
2. Records lineage (predecessor/successor relationships)
3. Transfers or closes hierarchy edges
4. Propagates variable recalculation
5. Emits events for every state change

These implement SPEC-SVSE-001 §13-17.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any

from ontologia.entity.identity import EntityIdentity, EntityType, LifecycleStatus
from ontologia.entity.lineage import LineageType
from ontologia.events import bus
from ontologia.registry.store import RegistryStore
from ontologia.structure.edges import _now_iso


@dataclass
class MutationResult:
    """Outcome of a structural mutation."""

    operation: str
    success: bool = False
    entities_created: list[str] = field(default_factory=list)
    entities_modified: list[str] = field(default_factory=list)
    lineage_records: int = 0
    edges_created: int = 0
    edges_closed: int = 0
    errors: list[str] = field(default_factory=list)

    def to_dict(self) -> dict[str, Any]:
        return {
            "operation": self.operation,
            "success": self.success,
            "entities_created": self.entities_created,
            "entities_modified": self.entities_modified,
            "lineage_records": self.lineage_records,
            "edges_created": self.edges_created,
            "edges_closed": self.edges_closed,
            "errors": self.errors,
        }


def relocate_entity(
    store: RegistryStore,
    entity_uid: str,
    new_parent_uid: str,
    source: str = "system",
) -> MutationResult:
    """Move an entity to a new parent in the hierarchy.

    1. Close existing hierarchy edge to old parent
    2. Create new hierarchy edge to new parent
    3. Emit ENTITY_RELOCATED event
    """
    result = MutationResult(operation="relocate")

    entity = store.get_entity(entity_uid)
    if not entity:
        result.errors.append(f"Entity {entity_uid} not found")
        return result

    new_parent = store.get_entity(new_parent_uid)
    if not new_parent:
        result.errors.append(f"New parent {new_parent_uid} not found")
        return result

    now = _now_iso()

    # Close existing parent edges
    old_parent_uid = None
    for edge in store.edge_index.all_hierarchy_edges():
        if edge.child_id == entity_uid and edge.is_active():
            old_parent_uid = edge.parent_id
            edge.valid_to = now
            result.edges_closed += 1

    # Create new parent edge
    store.add_hierarchy_edge(
        parent_id=new_parent_uid,
        child_id=entity_uid,
        metadata={"source": source, "old_parent": old_parent_uid or ""},
    )
    result.edges_created += 1

    bus.emit(
        "entity.relocated",
        source=source,
        subject_entity=entity_uid,
        changed_property="parent",
        previous_value=old_parent_uid,
        new_value=new_parent_uid,
    )

    result.entities_modified.append(entity_uid)
    result.success = True
    return result


def reclassify_entity(
    store: RegistryStore,
    entity_uid: str,
    new_type: EntityType,
    source: str = "system",
) -> MutationResult:
    """Change an entity's type classification.

    1. Record old type
    2. Update entity_type
    3. Emit event
    """
    result = MutationResult(operation="reclassify")

    entity = store.get_entity(entity_uid)
    if not entity:
        result.errors.append(f"Entity {entity_uid} not found")
        return result

    old_type = entity.entity_type
    entity.entity_type = new_type
    store._dirty = True

    bus.emit(
        "entity.reclassified",
        source=source,
        subject_entity=entity_uid,
        changed_property="entity_type",
        previous_value=old_type.value,
        new_value=new_type.value,
    )

    result.entities_modified.append(entity_uid)
    result.success = True
    return result


def merge_entities(
    store: RegistryStore,
    source_uids: list[str],
    successor_name: str,
    successor_type: EntityType | None = None,
    source: str = "system",
    metadata: dict[str, Any] | None = None,
) -> MutationResult:
    """Merge multiple entities into a single successor.

    1. Create successor entity
    2. Record MERGED_INTO lineage from each source to successor
    3. Deprecate source entities
    4. Transfer hierarchy children from sources to successor
    5. Emit ENTITIES_MERGED event
    """
    result = MutationResult(operation="merge")

    # Validate sources exist
    sources: list[EntityIdentity] = []
    for uid in source_uids:
        entity = store.get_entity(uid)
        if not entity:
            result.errors.append(f"Source entity {uid} not found")
            return result
        sources.append(entity)

    # Determine successor type (from first source if not specified)
    s_type = successor_type or sources[0].entity_type

    # Aggregate metadata from all sources
    merged_meta = metadata or {}
    merged_meta["predecessor_uids"] = source_uids

    # Create successor entity
    successor = store.create_entity(
        entity_type=s_type,
        display_name=successor_name,
        created_by=source,
        metadata=merged_meta,
    )
    result.entities_created.append(successor.uid)

    now = _now_iso()

    for src in sources:
        # Record lineage: source MERGED_INTO successor
        store.add_lineage(
            entity_id=src.uid,
            related_id=successor.uid,
            lineage_type=LineageType.MERGED_INTO,
            metadata={"source": source},
        )
        result.lineage_records += 1

        # Transfer hierarchy children to successor
        for edge in store.edge_index.all_hierarchy_edges():
            if edge.parent_id == src.uid and edge.is_active():
                edge.valid_to = now
                result.edges_closed += 1
                store.add_hierarchy_edge(
                    parent_id=successor.uid,
                    child_id=edge.child_id,
                    metadata={"transferred_from": src.uid, "source": source},
                )
                result.edges_created += 1

        # Inherit parent edges
        for edge in store.edge_index.all_hierarchy_edges():
            if edge.child_id == src.uid and edge.is_active():
                edge.valid_to = now
                result.edges_closed += 1
                store.add_hierarchy_edge(
                    parent_id=edge.parent_id,
                    child_id=successor.uid,
                    metadata={"inherited_from": src.uid, "source": source},
                )
                result.edges_created += 1

        # Deprecate source
        store.update_lifecycle(src.uid, LifecycleStatus.DEPRECATED, source=source)
        result.entities_modified.append(src.uid)

    bus.emit(
        "entities.merged",
        source=source,
        subject_entity=successor.uid,
        payload={
            "predecessor_uids": source_uids,
            "successor_uid": successor.uid,
        },
    )

    result.success = True
    return result


def split_entity(
    store: RegistryStore,
    source_uid: str,
    descendant_specs: list[dict[str, Any]],
    deprecate_source: bool = True,
    source: str = "system",
) -> MutationResult:
    """Split one entity into multiple descendants.

    Args:
        source_uid: Entity to split.
        descendant_specs: List of dicts with keys:
            - name (str, required): display name for descendant
            - type (EntityType, optional): defaults to source type
            - metadata (dict, optional): additional metadata
        deprecate_source: Whether to deprecate the source entity.
        source: Attribution string.

    Steps:
    1. Create each descendant entity
    2. Record SPLIT_FROM lineage from each descendant to source
    3. Optionally deprecate source
    4. Emit ENTITY_SPLIT event
    """
    result = MutationResult(operation="split")

    src_entity = store.get_entity(source_uid)
    if not src_entity:
        result.errors.append(f"Source entity {source_uid} not found")
        return result

    if not descendant_specs:
        result.errors.append("At least one descendant spec required")
        return result

    descendant_uids: list[str] = []

    for spec in descendant_specs:
        name = spec.get("name", "")
        if not name:
            result.errors.append("Descendant spec missing 'name'")
            return result

        d_type = spec.get("type", src_entity.entity_type)
        d_meta = spec.get("metadata", {})
        d_meta["split_from"] = source_uid

        descendant = store.create_entity(
            entity_type=d_type,
            display_name=name,
            created_by=source,
            metadata=d_meta,
        )
        descendant_uids.append(descendant.uid)
        result.entities_created.append(descendant.uid)

        # Record lineage: descendant SPLIT_FROM source
        store.add_lineage(
            entity_id=descendant.uid,
            related_id=source_uid,
            lineage_type=LineageType.SPLIT_FROM,
            metadata={"source": source},
        )
        result.lineage_records += 1

        # Inherit parent edges from source
        for edge in store.edge_index.all_hierarchy_edges():
            if edge.child_id == source_uid and edge.is_active():
                store.add_hierarchy_edge(
                    parent_id=edge.parent_id,
                    child_id=descendant.uid,
                    metadata={"inherited_from": source_uid, "source": source},
                )
                result.edges_created += 1

    if deprecate_source:
        store.update_lifecycle(source_uid, LifecycleStatus.DEPRECATED, source=source)
        result.entities_modified.append(source_uid)
        # Close source's parent edges
        for edge in store.edge_index.all_hierarchy_edges():
            if edge.child_id == source_uid and edge.is_active():
                edge.valid_to = _now_iso()
                result.edges_closed += 1

    bus.emit(
        "entity.split",
        source=source,
        subject_entity=source_uid,
        payload={
            "source_uid": source_uid,
            "descendant_uids": descendant_uids,
        },
    )

    result.success = True
    return result
