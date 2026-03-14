"""Unified registry store — manages all persistent state.

Coordinates JSON files (current state) and JSONL files (append-only logs)
in a single directory. All mutations go through the store so that events
are emitted and indexes are kept in sync.
"""

from __future__ import annotations

import json
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

from ontologia.entity.identity import (
    EntityIdentity,
    EntityType,
    LifecycleStatus,
    create_entity,
)
from ontologia.entity.naming import NameIndex, NameRecord, add_name
from ontologia.entity.resolver import EntityResolver
from ontologia.events import bus
from ontologia.structure.edges import EdgeIndex, HierarchyEdge, RelationEdge, _now_iso


def _default_store_dir() -> Path:
    return Path.home() / ".organvm" / "ontologia"


@dataclass
class RegistryStore:
    """Unified store for entities, names, edges, and events.

    File layout in store_dir:
    - entities.json   — current entity state {uid: entity_dict}
    - names.jsonl     — append-only name history
    - edges.jsonl     — append-only edge log (hierarchy + relation)
    - events.jsonl    — append-only event log (managed by events.bus)
    """

    store_dir: Path
    _entities: dict[str, EntityIdentity] = field(default_factory=dict)
    _name_index: NameIndex = field(default_factory=NameIndex)
    _edge_index: EdgeIndex = field(default_factory=EdgeIndex)
    _dirty: bool = False

    # ------------------------------------------------------------------
    # Paths
    # ------------------------------------------------------------------

    @property
    def entities_path(self) -> Path:
        return self.store_dir / "entities.json"

    @property
    def names_path(self) -> Path:
        return self.store_dir / "names.jsonl"

    @property
    def edges_path(self) -> Path:
        return self.store_dir / "edges.jsonl"

    @property
    def events_path(self) -> Path:
        return self.store_dir / "events.jsonl"

    # ------------------------------------------------------------------
    # Load / Save
    # ------------------------------------------------------------------

    def load(self) -> None:
        """Load entities from JSON and names from JSONL."""
        self.store_dir.mkdir(parents=True, exist_ok=True)

        # Load entities
        self._entities.clear()
        if self.entities_path.is_file():
            data = json.loads(self.entities_path.read_text())
            for uid, edict in data.items():
                self._entities[uid] = EntityIdentity.from_dict(edict)

        # Load names
        self._name_index = NameIndex()
        if self.names_path.is_file():
            for line in self.names_path.read_text().splitlines():
                line = line.strip()
                if not line:
                    continue
                try:
                    record = NameRecord.from_dict(json.loads(line))
                    self._name_index.add(record)
                except (json.JSONDecodeError, KeyError):
                    continue

        # Load edges
        self._edge_index = EdgeIndex()
        if self.edges_path.is_file():
            for line in self.edges_path.read_text().splitlines():
                line = line.strip()
                if not line:
                    continue
                try:
                    data = json.loads(line)
                    edge_type = data.get("edge_type", "")
                    if edge_type == "hierarchy":
                        self._edge_index.add_hierarchy(HierarchyEdge.from_dict(data))
                    elif edge_type == "relation":
                        self._edge_index.add_relation(RelationEdge.from_dict(data))
                except (json.JSONDecodeError, KeyError):
                    continue

        # Point the event bus at our events file
        bus.set_events_path(self.events_path)

        self._dirty = False

    def save(self) -> None:
        """Persist entities to JSON. Names are always appended inline."""
        self.store_dir.mkdir(parents=True, exist_ok=True)

        # Write entities
        data = {uid: entity.to_dict() for uid, entity in self._entities.items()}
        self.entities_path.write_text(
            json.dumps(data, indent=2, sort_keys=True) + "\n",
        )
        self._dirty = False

    def save_names(self) -> None:
        """Rewrite the full names JSONL from the in-memory index.

        Normally names are appended one-at-a-time via _append_name().
        This is a recovery/migration tool that rebuilds the file.
        """
        self.store_dir.mkdir(parents=True, exist_ok=True)
        lines: list[str] = []
        for entity_id in sorted(self._name_index._by_entity):
            for record in self._name_index._by_entity[entity_id]:
                lines.append(record.to_jsonl())
        self.names_path.write_text("\n".join(lines) + "\n" if lines else "")

    # ------------------------------------------------------------------
    # Entity operations
    # ------------------------------------------------------------------

    def create_entity(
        self,
        entity_type: EntityType,
        display_name: str,
        created_by: str = "system",
        metadata: dict[str, Any] | None = None,
        timestamp_ms: int | None = None,
    ) -> EntityIdentity:
        """Create a new entity with identity and initial name.

        Args:
            entity_type: What kind of entity.
            display_name: Initial display name.
            created_by: Creator identifier.
            metadata: Optional metadata dict.
            timestamp_ms: Optional deterministic timestamp for UID.

        Returns:
            The new EntityIdentity.
        """
        entity = create_entity(
            entity_type=entity_type,
            created_by=created_by,
            metadata=metadata,
            timestamp_ms=timestamp_ms,
        )
        self._entities[entity.uid] = entity
        self._dirty = True

        # Create initial name record
        name_record = add_name(
            self._name_index,
            entity.uid,
            display_name,
            is_primary=True,
            source=created_by,
        )
        self._append_name(name_record)

        # Emit event
        bus.emit(
            bus.ENTITY_CREATED,
            source=created_by,
            subject_entity=entity.uid,
            payload={
                "entity_type": entity_type.value,
                "display_name": display_name,
            },
        )

        return entity

    def get_entity(self, uid: str) -> EntityIdentity | None:
        """Get an entity by UID."""
        return self._entities.get(uid)

    def update_lifecycle(
        self,
        uid: str,
        new_status: LifecycleStatus,
        source: str = "system",
    ) -> bool:
        """Update an entity's lifecycle status.

        Returns True if updated, False if entity not found.
        """
        entity = self._entities.get(uid)
        if not entity:
            return False

        old_status = entity.lifecycle_status
        entity.lifecycle_status = new_status
        self._dirty = True

        bus.emit(
            bus.ENTITY_DEPRECATED if new_status == LifecycleStatus.DEPRECATED
            else bus.ENTITY_ARCHIVED if new_status == LifecycleStatus.ARCHIVED
            else "entity.lifecycle_changed",
            source=source,
            subject_entity=uid,
            changed_property="lifecycle_status",
            previous_value=old_status.value,
            new_value=new_status.value,
        )
        return True

    def rename_entity(
        self,
        uid: str,
        new_name: str,
        source: str = "system",
    ) -> NameRecord | None:
        """Rename an entity — retires old primary name, adds new one.

        Returns the new NameRecord, or None if entity not found.
        """
        entity = self._entities.get(uid)
        if not entity:
            return None

        old_name = self._name_index.current_name(uid)
        old_display = old_name.display_name if old_name else None

        record = add_name(
            self._name_index,
            uid,
            new_name,
            is_primary=True,
            source=source,
        )
        self._append_name(record)

        bus.emit(
            bus.ENTITY_RENAMED,
            source=source,
            subject_entity=uid,
            changed_property="display_name",
            previous_value=old_display,
            new_value=new_name,
        )
        return record

    # ------------------------------------------------------------------
    # Name operations
    # ------------------------------------------------------------------

    def add_alias(
        self,
        uid: str,
        alias_name: str,
        source: str = "system",
    ) -> NameRecord | None:
        """Add a non-primary alias to an entity."""
        if uid not in self._entities:
            return None

        record = add_name(
            self._name_index,
            uid,
            alias_name,
            is_primary=False,
            source=source,
        )
        self._append_name(record)

        bus.emit(
            bus.NAME_ADDED,
            source=source,
            subject_entity=uid,
            new_value=alias_name,
        )
        return record

    def current_name(self, uid: str, at: str | None = None) -> NameRecord | None:
        """Get the current primary name for an entity."""
        return self._name_index.current_name(uid, at=at)

    def name_history(self, uid: str) -> list[NameRecord]:
        """Get full name history for an entity."""
        return self._name_index.all_names(uid)

    # ------------------------------------------------------------------
    # Resolver
    # ------------------------------------------------------------------

    def resolver(self) -> EntityResolver:
        """Build an EntityResolver from current state."""
        return EntityResolver(dict(self._entities), self._name_index)

    # ------------------------------------------------------------------
    # Query
    # ------------------------------------------------------------------

    @property
    def entity_count(self) -> int:
        return len(self._entities)

    def list_entities(
        self,
        entity_type: EntityType | None = None,
        lifecycle_status: LifecycleStatus | None = None,
    ) -> list[EntityIdentity]:
        """List entities with optional filters."""
        results: list[EntityIdentity] = []
        for entity in self._entities.values():
            if entity_type and entity.entity_type != entity_type:
                continue
            if lifecycle_status and entity.lifecycle_status != lifecycle_status:
                continue
            results.append(entity)
        return results

    def events(
        self,
        since: str | None = None,
        event_type: str | None = None,
        subject_entity: str | None = None,
        limit: int = 500,
    ) -> list[bus.OntologiaEvent]:
        """Query the event log."""
        return bus.replay(
            since=since,
            event_type=event_type,
            subject_entity=subject_entity,
            limit=limit,
            path=self.events_path,
        )

    # ------------------------------------------------------------------
    # Edge operations
    # ------------------------------------------------------------------

    @property
    def edge_index(self) -> EdgeIndex:
        """The in-memory edge index (hierarchy + relation edges)."""
        return self._edge_index

    def add_hierarchy_edge(
        self,
        parent_id: str,
        child_id: str,
        metadata: dict[str, Any] | None = None,
    ) -> HierarchyEdge:
        """Create and persist a hierarchy edge (parent→child)."""
        edge = HierarchyEdge(
            parent_id=parent_id,
            child_id=child_id,
            valid_from=_now_iso(),
            metadata=metadata or {},
        )
        self._edge_index.add_hierarchy(edge)
        self._append_edge(edge, "hierarchy")
        return edge

    def add_relation_edge(
        self,
        source_id: str,
        target_id: str,
        relation_type: str,
        metadata: dict[str, Any] | None = None,
    ) -> RelationEdge:
        """Create and persist a relation edge (source→target)."""
        edge = RelationEdge(
            source_id=source_id,
            target_id=target_id,
            relation_type=relation_type,
            valid_from=_now_iso(),
            metadata=metadata or {},
        )
        self._edge_index.add_relation(edge)
        self._append_edge(edge, "relation")
        return edge

    def save_edges(self) -> None:
        """Rewrite the full edges JSONL from the in-memory EdgeIndex.

        Recovery/migration tool — analogous to save_names().
        """
        self.store_dir.mkdir(parents=True, exist_ok=True)
        lines: list[str] = []
        for edge in self._edge_index.all_hierarchy_edges():
            d = edge.to_dict()
            d["edge_type"] = "hierarchy"
            lines.append(json.dumps(d, separators=(",", ":")))
        for edge in self._edge_index.all_relation_edges():
            d = edge.to_dict()
            d["edge_type"] = "relation"
            lines.append(json.dumps(d, separators=(",", ":")))
        self.edges_path.write_text("\n".join(lines) + "\n" if lines else "")

    def _append_edge(self, edge: HierarchyEdge | RelationEdge, edge_type: str) -> None:
        """Append a single edge record to the JSONL file."""
        self.store_dir.mkdir(parents=True, exist_ok=True)
        d = edge.to_dict()
        d["edge_type"] = edge_type
        with self.edges_path.open("a") as f:
            f.write(json.dumps(d, separators=(",", ":")) + "\n")

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def _append_name(self, record: NameRecord) -> None:
        """Append a single name record to the JSONL file."""
        self.store_dir.mkdir(parents=True, exist_ok=True)
        with self.names_path.open("a") as f:
            f.write(record.to_jsonl() + "\n")


def open_store(store_dir: Path | None = None) -> RegistryStore:
    """Open (or create) a registry store and load its state.

    Args:
        store_dir: Directory for store files. Defaults to ~/.organvm/ontologia/.

    Returns:
        A loaded RegistryStore ready for use.
    """
    path = store_dir or _default_store_dir()
    store = RegistryStore(store_dir=path)
    store.load()
    return store
