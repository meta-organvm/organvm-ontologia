"""High-level mutation operations on the structural registry.

Each mutation coordinates entity, naming, lineage, and edge changes
into a single logical operation. All mutations emit events and create
structure version records.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from ontologia.entity.identity import EntityIdentity, EntityType, LifecycleStatus, create_entity
from ontologia.entity.lineage import LineageIndex, LineageRecord, LineageType
from ontologia.entity.naming import NameIndex, add_name
from ontologia.events import bus
from ontologia.structure.edges import EdgeIndex, HierarchyEdge
from ontologia.structure.versioning import VersionLog, create_version


def _now_iso() -> str:
    from datetime import datetime, timezone
    return datetime.now(timezone.utc).isoformat()


@dataclass
class MutationContext:
    """Shared state for mutation operations.

    Mutations need access to all indexes to coordinate changes.
    The RegistryStore can build one of these from its internal state.
    """

    entities: dict[str, EntityIdentity]
    name_index: NameIndex
    edge_index: EdgeIndex
    lineage_index: LineageIndex
    version_log: VersionLog


def rename(
    ctx: MutationContext,
    entity_id: str,
    new_name: str,
    reason: str = "",
    source: str = "system",
) -> bool:
    """Rename an entity — retires old name, adds new primary.

    Returns True if successful, False if entity not found.
    """
    entity = ctx.entities.get(entity_id)
    if not entity:
        return False

    old_name_rec = ctx.name_index.current_name(entity_id)
    old_name = old_name_rec.display_name if old_name_rec else None

    add_name(ctx.name_index, entity_id, new_name, is_primary=True, source=source)

    version = create_version(
        change_type="entity_renamed",
        change_reason=reason or f"Renamed from {old_name} to {new_name}",
        affected_entities=[entity_id],
        details={"old_name": old_name, "new_name": new_name},
    )
    ctx.version_log.append(version)

    bus.emit(
        bus.ENTITY_RENAMED,
        source=source,
        subject_entity=entity_id,
        changed_property="display_name",
        previous_value=old_name,
        new_value=new_name,
    )
    return True


def relocate(
    ctx: MutationContext,
    entity_id: str,
    new_parent_id: str,
    reason: str = "",
    source: str = "system",
) -> bool:
    """Move an entity to a new parent in the hierarchy.

    Retires the old parent edge and creates a new one.
    Returns True if successful.
    """
    entity = ctx.entities.get(entity_id)
    if not entity:
        return False
    if new_parent_id not in ctx.entities:
        return False

    now = _now_iso()
    old_parent_edge = ctx.edge_index.parent(entity_id)
    old_parent_id = old_parent_edge.parent_id if old_parent_edge else None

    # Retire old edge
    if old_parent_edge:
        ctx.edge_index.retire_hierarchy(old_parent_edge.parent_id, entity_id, at=now)

    # Create new edge
    new_edge = HierarchyEdge(
        parent_id=new_parent_id,
        child_id=entity_id,
        valid_from=now,
    )
    ctx.edge_index.add_hierarchy(new_edge)

    version = create_version(
        change_type="entity_relocated",
        change_reason=reason or f"Moved from {old_parent_id} to {new_parent_id}",
        affected_entities=[entity_id, new_parent_id] + ([old_parent_id] if old_parent_id else []),
        details={"old_parent": old_parent_id, "new_parent": new_parent_id},
    )
    ctx.version_log.append(version)

    bus.emit(
        bus.ENTITY_RELOCATED,
        source=source,
        subject_entity=entity_id,
        changed_property="parent",
        previous_value=old_parent_id,
        new_value=new_parent_id,
    )
    return True


def merge(
    ctx: MutationContext,
    source_ids: list[str],
    merged_name: str,
    entity_type: EntityType | None = None,
    reason: str = "",
    source: str = "system",
) -> EntityIdentity | None:
    """Merge multiple entities into a new successor entity.

    Creates a new entity, records lineage from all sources, deprecates
    the source entities, and transfers hierarchy children to the new entity.

    Returns the new merged entity, or None if any source not found.
    """
    for sid in source_ids:
        if sid not in ctx.entities:
            return None

    # Infer type from first source if not specified
    if entity_type is None:
        entity_type = ctx.entities[source_ids[0]].entity_type

    # Create successor
    successor = create_entity(entity_type=entity_type, created_by=source)
    ctx.entities[successor.uid] = successor
    add_name(ctx.name_index, successor.uid, merged_name, is_primary=True, source=source)

    now = _now_iso()

    for sid in source_ids:
        # Record lineage
        ctx.lineage_index.add(LineageRecord(
            entity_id=successor.uid,
            related_id=sid,
            lineage_type=LineageType.DERIVED_FROM,
        ))
        ctx.lineage_index.add(LineageRecord(
            entity_id=sid,
            related_id=successor.uid,
            lineage_type=LineageType.MERGED_INTO,
        ))

        # Deprecate source
        ctx.entities[sid].lifecycle_status = LifecycleStatus.MERGED

        # Transfer children to successor
        for child_edge in ctx.edge_index.children(sid):
            ctx.edge_index.retire_hierarchy(sid, child_edge.child_id, at=now)
            ctx.edge_index.add_hierarchy(HierarchyEdge(
                parent_id=successor.uid,
                child_id=child_edge.child_id,
                valid_from=now,
            ))

        # Transfer parent: if source had a parent, give successor the same parent
        parent_edge = ctx.edge_index.parent(sid)
        if parent_edge:
            ctx.edge_index.retire_hierarchy(parent_edge.parent_id, sid, at=now)

    # Give successor the parent of the first source (if any)
    first_parent = ctx.edge_index.parent(source_ids[0], at=now)
    # Check historical parent since we just retired
    for edge in ctx.edge_index._parent_of.get(source_ids[0], []):
        if edge.valid_to == now:  # just retired
            ctx.edge_index.add_hierarchy(HierarchyEdge(
                parent_id=edge.parent_id,
                child_id=successor.uid,
                valid_from=now,
            ))
            break

    version = create_version(
        change_type="entity_merged",
        change_reason=reason or f"Merged {len(source_ids)} entities into {merged_name}",
        affected_entities=source_ids + [successor.uid],
        details={"source_ids": source_ids, "successor_id": successor.uid},
    )
    ctx.version_log.append(version)

    bus.emit(
        bus.ENTITY_MERGED,
        source=source,
        subject_entity=successor.uid,
        payload={"source_ids": source_ids, "merged_name": merged_name},
    )
    return successor


def split(
    ctx: MutationContext,
    entity_id: str,
    new_names: list[str],
    entity_type: EntityType | None = None,
    reason: str = "",
    source: str = "system",
) -> list[EntityIdentity]:
    """Split an entity into multiple new entities.

    Creates new entities for each name, records lineage, deprecates the
    original. Does not transfer children (caller decides allocation).

    Returns list of new entities, empty if source not found.
    """
    entity = ctx.entities.get(entity_id)
    if not entity:
        return []

    if entity_type is None:
        entity_type = entity.entity_type

    new_entities: list[EntityIdentity] = []
    for name in new_names:
        new_ent = create_entity(entity_type=entity_type, created_by=source)
        ctx.entities[new_ent.uid] = new_ent
        add_name(ctx.name_index, new_ent.uid, name, is_primary=True, source=source)

        # Lineage
        ctx.lineage_index.add(LineageRecord(
            entity_id=new_ent.uid,
            related_id=entity_id,
            lineage_type=LineageType.SPLIT_FROM,
        ))
        new_entities.append(new_ent)

    # Deprecate original
    entity.lifecycle_status = LifecycleStatus.SPLIT

    version = create_version(
        change_type="entity_split",
        change_reason=reason or f"Split into {len(new_names)} entities",
        affected_entities=[entity_id] + [e.uid for e in new_entities],
        details={"source_id": entity_id, "new_ids": [e.uid for e in new_entities]},
    )
    ctx.version_log.append(version)

    bus.emit(
        bus.ENTITY_SPLIT,
        source=source,
        subject_entity=entity_id,
        payload={"new_ids": [e.uid for e in new_entities], "new_names": new_names},
    )
    return new_entities


def deprecate(
    ctx: MutationContext,
    entity_id: str,
    successor_id: str | None = None,
    reason: str = "",
    source: str = "system",
) -> bool:
    """Deprecate an entity, optionally pointing to a successor.

    Returns True if successful, False if entity not found.
    """
    entity = ctx.entities.get(entity_id)
    if not entity:
        return False

    entity.lifecycle_status = LifecycleStatus.DEPRECATED

    if successor_id and successor_id in ctx.entities:
        ctx.lineage_index.add(LineageRecord(
            entity_id=successor_id,
            related_id=entity_id,
            lineage_type=LineageType.SUPERSEDES,
        ))

    version = create_version(
        change_type="entity_deprecated",
        change_reason=reason or "Entity deprecated",
        affected_entities=[entity_id] + ([successor_id] if successor_id else []),
        details={"successor_id": successor_id},
    )
    ctx.version_log.append(version)

    bus.emit(
        bus.ENTITY_DEPRECATED,
        source=source,
        subject_entity=entity_id,
        payload={"successor_id": successor_id, "reason": reason},
    )
    return True
