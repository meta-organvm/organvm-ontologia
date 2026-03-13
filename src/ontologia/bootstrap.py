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

    # Persist entities
    store.save()

    # Emit completion event
    bus.emit(
        bus.BOOTSTRAP_COMPLETED,
        source=created_by,
        payload=result.to_dict(),
    )

    return result
