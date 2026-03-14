"""Bootstrap migration — convert registry-v2.json into ontologia entities.

Reads the existing registry and creates EntityIdentity + NameRecord for
every organ and repo. Idempotent: skips entities that already exist
(matched by metadata tags).
"""

from __future__ import annotations

import json
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

from ontologia.entity.identity import EntityType
from ontologia.events import bus
from ontologia.registry.store import RegistryStore


@dataclass
class BootstrapResult:
    """Summary of a bootstrap migration run."""

    organs_created: int = 0
    repos_created: int = 0
    organs_skipped: int = 0
    repos_skipped: int = 0
    hierarchy_edges_created: int = 0
    errors: list[str] = field(default_factory=list)

    @property
    def total_created(self) -> int:
        return self.organs_created + self.repos_created

    @property
    def total_skipped(self) -> int:
        return self.organs_skipped + self.repos_skipped

    def to_dict(self) -> dict[str, Any]:
        return {
            "organs_created": self.organs_created,
            "repos_created": self.repos_created,
            "organs_skipped": self.organs_skipped,
            "repos_skipped": self.repos_skipped,
            "hierarchy_edges_created": self.hierarchy_edges_created,
            "total_created": self.total_created,
            "total_skipped": self.total_skipped,
            "errors": self.errors,
        }


def _existing_organ_keys(store: RegistryStore) -> set[str]:
    """Collect registry_key metadata from existing organ entities."""
    keys: set[str] = set()
    for entity in store.list_entities(entity_type=EntityType.ORGAN):
        rk = entity.metadata.get("registry_key")
        if rk:
            keys.add(rk)
    return keys


def _existing_repo_tags(store: RegistryStore) -> set[str]:
    """Collect org/name tags from existing repo entities."""
    tags: set[str] = set()
    for entity in store.list_entities(entity_type=EntityType.REPO):
        org = entity.metadata.get("org", "")
        name = entity.metadata.get("name", "")
        if org and name:
            tags.add(f"{org}/{name}")
    return tags


def bootstrap_from_registry(
    store: RegistryStore,
    registry_path: Path,
    created_by: str = "bootstrap",
) -> BootstrapResult:
    """Migrate registry-v2.json into ontologia entities.

    Creates organ entities and repo entities with initial names.
    Idempotent — skips entities whose metadata tags already exist.

    Args:
        store: The ontologia RegistryStore to populate.
        registry_path: Path to registry-v2.json.
        created_by: Attribution string for created entities.

    Returns:
        BootstrapResult with counts and any errors.
    """
    result = BootstrapResult()

    try:
        registry_data = json.loads(registry_path.read_text())
    except (json.JSONDecodeError, FileNotFoundError) as exc:
        result.errors.append(f"Failed to read registry: {exc}")
        return result

    organs = registry_data.get("organs", {})
    if not organs:
        result.errors.append("No organs found in registry")
        return result

    existing_organ_keys = _existing_organ_keys(store)
    existing_repo_tags = _existing_repo_tags(store)

    # Create organ entities
    for organ_key, organ_data in organs.items():
        if organ_key in existing_organ_keys:
            result.organs_skipped += 1
            continue

        organ_name = organ_data.get("name", organ_key)
        try:
            store.create_entity(
                entity_type=EntityType.ORGAN,
                display_name=organ_name,
                created_by=created_by,
                metadata={
                    "registry_key": organ_key,
                    "description": organ_data.get("description", ""),
                },
            )
            result.organs_created += 1
        except Exception as exc:
            result.errors.append(f"Failed to create organ {organ_key}: {exc}")

    # Create repo entities
    for organ_key, organ_data in organs.items():
        for repo in organ_data.get("repositories", []):
            repo_name = repo.get("name", "")
            org = repo.get("org", "")
            tag = f"{org}/{repo_name}"

            if tag in existing_repo_tags:
                result.repos_skipped += 1
                continue

            try:
                store.create_entity(
                    entity_type=EntityType.REPO,
                    display_name=repo_name,
                    created_by=created_by,
                    metadata={
                        "organ_key": organ_key,
                        "org": org,
                        "name": repo_name,
                        "tier": repo.get("tier", ""),
                        "promotion_status": repo.get("promotion_status", ""),
                        "implementation_status": repo.get("implementation_status", ""),
                        "public": repo.get("public", False),
                    },
                )
                result.repos_created += 1
            except Exception as exc:
                result.errors.append(f"Failed to create repo {tag}: {exc}")

    # Create hierarchy edges (organ→repo)
    _bootstrap_hierarchy_edges(store, result)

    # Persist entities
    store.save()

    # Emit completion event
    bus.emit(
        bus.BOOTSTRAP_COMPLETED,
        source=created_by,
        payload=result.to_dict(),
    )

    return result


def _bootstrap_hierarchy_edges(
    store: RegistryStore,
    result: BootstrapResult,
) -> None:
    """Create hierarchy edges from organ→repo relationships.

    Builds organ_key→UID mapping, then creates an edge for each repo
    whose organ_key maps to an existing organ entity. Skips edges
    that already exist (idempotent).
    """
    # Build organ_key → organ UID mapping
    organ_uids: dict[str, str] = {}
    for entity in store.list_entities(entity_type=EntityType.ORGAN):
        rk = entity.metadata.get("registry_key", "")
        if rk:
            organ_uids[rk] = entity.uid

    if not organ_uids:
        return

    # Build set of existing hierarchy edges for dedup
    existing_edges: set[tuple[str, str]] = set()
    for edge in store.edge_index.all_hierarchy_edges():
        if edge.is_active():
            existing_edges.add((edge.parent_id, edge.child_id))

    # Create organ→repo edges
    for entity in store.list_entities(entity_type=EntityType.REPO):
        organ_key = entity.metadata.get("organ_key", "")
        parent_uid = organ_uids.get(organ_key)
        if not parent_uid:
            continue
        if (parent_uid, entity.uid) in existing_edges:
            continue
        store.add_hierarchy_edge(
            parent_id=parent_uid,
            child_id=entity.uid,
            metadata={"source": "bootstrap", "organ_key": organ_key},
        )
        result.hierarchy_edges_created += 1
