"""Entity resolver — find entities by UID, current name, or historical alias.

The resolver is the primary query interface for entity lookup. It searches
in order: exact UID match → current primary name → slug match → historical
alias. This makes the transition from name-based to UID-based lookup
seamless — existing code can pass a repo name and get back the entity.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from ontologia.entity.identity import EntityIdentity, EntityType
from ontologia.entity.naming import NameIndex, NameRecord


@dataclass
class ResolvedEntity:
    """Result of entity resolution — identity plus current name."""

    identity: EntityIdentity
    current_name: NameRecord | None
    matched_by: str  # "uid", "primary_name", "slug", "alias"


class EntityResolver:
    """Resolve entities by any identifier.

    Backed by an entity dict (uid → EntityIdentity) and a NameIndex.
    """

    def __init__(
        self,
        entities: dict[str, EntityIdentity],
        name_index: NameIndex,
    ) -> None:
        self._entities = entities
        self._names = name_index

    @property
    def entity_count(self) -> int:
        return len(self._entities)

    def resolve(
        self,
        identifier: str,
        at: str | None = None,
        entity_type: EntityType | None = None,
    ) -> ResolvedEntity | None:
        """Resolve an identifier to an entity.

        Search order:
        1. Exact UID match
        2. Current primary name (display_name, case-insensitive)
        3. Slug match
        4. Historical alias (display_name that has been retired)

        Args:
            identifier: UID, name, or slug to look up.
            at: Optional timestamp for temporal resolution.
            entity_type: Optional filter by entity type.

        Returns:
            ResolvedEntity or None if not found.
        """
        # 1. Exact UID match
        if identifier in self._entities:
            entity = self._entities[identifier]
            if entity_type and entity.entity_type != entity_type:
                return None
            name = self._names.current_name(identifier, at=at)
            return ResolvedEntity(identity=entity, current_name=name, matched_by="uid")

        # 2. Current primary name (case-insensitive)
        entity_ids = self._names.resolve_display_name(identifier)
        for eid in entity_ids:
            entity = self._entities.get(eid)
            if not entity:
                continue
            if entity_type and entity.entity_type != entity_type:
                continue
            name = self._names.current_name(eid, at=at)
            if name and name.display_name.lower() == identifier.lower() and name.is_primary:
                return ResolvedEntity(identity=entity, current_name=name, matched_by="primary_name")

        # 3. Slug match
        slug_ids = self._names.resolve_slug(identifier)
        for eid in slug_ids:
            entity = self._entities.get(eid)
            if not entity:
                continue
            if entity_type and entity.entity_type != entity_type:
                continue
            name = self._names.current_name(eid, at=at)
            return ResolvedEntity(identity=entity, current_name=name, matched_by="slug")

        # 4. Historical alias (any display_name match, even retired)
        for eid in entity_ids:
            entity = self._entities.get(eid)
            if not entity:
                continue
            if entity_type and entity.entity_type != entity_type:
                continue
            name = self._names.current_name(eid, at=at)
            return ResolvedEntity(identity=entity, current_name=name, matched_by="alias")

        return None

    def resolve_all(
        self,
        identifier: str,
        entity_type: EntityType | None = None,
    ) -> list[ResolvedEntity]:
        """Resolve an identifier, returning all matches (not just first).

        Useful when a name/slug maps to multiple entities (e.g., after a split).
        """
        results: list[ResolvedEntity] = []
        seen: set[str] = set()

        # UID match
        if identifier in self._entities:
            entity = self._entities[identifier]
            if not entity_type or entity.entity_type == entity_type:
                name = self._names.current_name(identifier)
                results.append(
                    ResolvedEntity(identity=entity, current_name=name, matched_by="uid"),
                )
                seen.add(identifier)

        # Name + slug matches
        for eid in self._names.resolve_display_name(identifier):
            if eid in seen:
                continue
            entity = self._entities.get(eid)
            if not entity:
                continue
            if entity_type and entity.entity_type != entity_type:
                continue
            name = self._names.current_name(eid)
            results.append(
                ResolvedEntity(identity=entity, current_name=name, matched_by="alias"),
            )
            seen.add(eid)

        for eid in self._names.resolve_slug(identifier):
            if eid in seen:
                continue
            entity = self._entities.get(eid)
            if not entity:
                continue
            if entity_type and entity.entity_type != entity_type:
                continue
            name = self._names.current_name(eid)
            results.append(
                ResolvedEntity(identity=entity, current_name=name, matched_by="slug"),
            )
            seen.add(eid)

        return results

    def get(self, uid: str) -> EntityIdentity | None:
        """Direct UID lookup — no name resolution."""
        return self._entities.get(uid)

    def list_by_type(self, entity_type: EntityType) -> list[EntityIdentity]:
        """List all entities of a given type."""
        return [e for e in self._entities.values() if e.entity_type == entity_type]

    def list_all(self) -> list[EntityIdentity]:
        """List all registered entities."""
        return list(self._entities.values())
