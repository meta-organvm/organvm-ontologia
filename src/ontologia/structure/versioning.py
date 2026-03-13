"""Structure versioning — track structural changes over time.

Every structural mutation (edge added/removed, hierarchy reorganization)
creates a StructureVersion record in an append-only JSONL log. This
provides a complete audit trail of how the system's topology evolved.
"""

from __future__ import annotations

import json
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from ontologia._ulid import generate_ulid


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


@dataclass
class StructureVersion:
    """A snapshot of a structural change."""

    version_id: str
    timestamp: str
    change_type: str  # "hierarchy_added", "hierarchy_retired", "relation_added", etc.
    change_reason: str
    affected_entities: list[str] = field(default_factory=list)
    details: dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        return {
            "version_id": self.version_id,
            "timestamp": self.timestamp,
            "change_type": self.change_type,
            "change_reason": self.change_reason,
            "affected_entities": self.affected_entities,
            "details": self.details,
        }

    def to_jsonl(self) -> str:
        return json.dumps(self.to_dict(), separators=(",", ":"))

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> StructureVersion:
        return cls(
            version_id=data.get("version_id", ""),
            timestamp=data.get("timestamp", ""),
            change_type=data.get("change_type", ""),
            change_reason=data.get("change_reason", ""),
            affected_entities=data.get("affected_entities", []),
            details=data.get("details", {}),
        )


def create_version(
    change_type: str,
    change_reason: str,
    affected_entities: list[str] | None = None,
    details: dict[str, Any] | None = None,
) -> StructureVersion:
    """Create a new StructureVersion with a fresh ULID."""
    return StructureVersion(
        version_id=f"sv_{generate_ulid()}",
        timestamp=_now_iso(),
        change_type=change_type,
        change_reason=change_reason,
        affected_entities=affected_entities or [],
        details=details or {},
    )


class VersionLog:
    """Append-only JSONL log of structural changes."""

    def __init__(self, path: Path) -> None:
        self._path = path
        self._versions: list[StructureVersion] = []

    @property
    def path(self) -> Path:
        return self._path

    def load(self) -> None:
        """Load existing versions from the JSONL file."""
        self._versions.clear()
        if not self._path.is_file():
            return
        for line in self._path.read_text().splitlines():
            line = line.strip()
            if not line:
                continue
            try:
                self._versions.append(StructureVersion.from_dict(json.loads(line)))
            except (json.JSONDecodeError, KeyError):
                continue

    def append(self, version: StructureVersion) -> None:
        """Append a version to the log (both in-memory and on disk)."""
        self._versions.append(version)
        self._path.parent.mkdir(parents=True, exist_ok=True)
        with self._path.open("a") as f:
            f.write(version.to_jsonl() + "\n")

    def all(self) -> list[StructureVersion]:
        return list(self._versions)

    def by_entity(self, entity_id: str) -> list[StructureVersion]:
        """Get all versions that affected a specific entity."""
        return [v for v in self._versions if entity_id in v.affected_entities]

    def by_type(self, change_type: str) -> list[StructureVersion]:
        return [v for v in self._versions if v.change_type == change_type]

    def recent(self, n: int = 20) -> list[StructureVersion]:
        return self._versions[-n:]

    @property
    def count(self) -> int:
        return len(self._versions)
