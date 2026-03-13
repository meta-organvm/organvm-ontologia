"""Registry sensor -- detect changes in registry-v2.json."""

from __future__ import annotations

import contextlib
import hashlib
import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from ontologia.sensing.interfaces import RawSignal

# Relative path from workspace root to the registry file.
_REGISTRY_REL = Path("meta-organvm") / "organvm-corpvs-testamentvm" / "registry-v2.json"


class RegistrySensor:
    """Watch registry-v2.json for structural changes."""

    def __init__(self, workspace: Path) -> None:
        self._workspace = workspace
        self._registry_path = workspace / _REGISTRY_REL
        self._last_hash: str | None = None
        self._last_mtime: float | None = None
        self._last_repos: dict[str, dict[str, Any]] = {}

    @property
    def name(self) -> str:
        return "registry"

    def is_available(self) -> bool:
        return self._registry_path.is_file()

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    @staticmethod
    def _content_hash(data: bytes) -> str:
        return hashlib.sha256(data).hexdigest()

    @staticmethod
    def _extract_repos(registry: dict[str, Any]) -> dict[str, dict[str, Any]]:
        """Build a flat map of repo_name -> subset of fields we track."""
        repos: dict[str, dict[str, Any]] = {}
        organs = registry.get("organs", {})
        for _organ_key, organ_data in organs.items():
            for repo in organ_data.get("repositories", []):
                rname = repo.get("name", "")
                if rname:
                    repos[rname] = {
                        "promotion_status": repo.get("promotion_status"),
                        "implementation_status": repo.get("implementation_status"),
                        "tier": repo.get("tier"),
                        "public": repo.get("public"),
                        "ci_workflow": repo.get("ci_workflow"),
                    }
        return repos

    # ------------------------------------------------------------------
    # Scan
    # ------------------------------------------------------------------

    def scan(self) -> list[RawSignal]:
        if not self.is_available():
            return []

        raw = self._registry_path.read_bytes()
        current_hash = self._content_hash(raw)
        current_mtime = self._registry_path.stat().st_mtime

        # First run -- seed state, no signals.
        if self._last_hash is None:
            self._last_hash = current_hash
            self._last_mtime = current_mtime
            with contextlib.suppress(json.JSONDecodeError, KeyError):
                self._last_repos = self._extract_repos(json.loads(raw))
            return []

        # Nothing changed.
        if current_hash == self._last_hash:
            return []

        now = datetime.now(timezone.utc).isoformat()
        signals: list[RawSignal] = []

        try:
            current_repos = self._extract_repos(json.loads(raw))
        except (json.JSONDecodeError, KeyError):
            # File is corrupt -- emit a single anomaly signal and bail.
            signals.append(RawSignal(
                sensor_name=self.name,
                signal_type="registry_updated",
                entity_id="registry-v2",
                details={"error": "parse_failure"},
                timestamp=now,
                confidence=0.5,
            ))
            self._last_hash = current_hash
            self._last_mtime = current_mtime
            return signals

        prev_names = set(self._last_repos.keys())
        curr_names = set(current_repos.keys())

        # New repos
        for rname in sorted(curr_names - prev_names):
            signals.append(RawSignal(
                sensor_name=self.name,
                signal_type="file_created",
                entity_id=rname,
                details={"event": "repo_added"},
                timestamp=now,
            ))

        # Removed repos
        for rname in sorted(prev_names - curr_names):
            signals.append(RawSignal(
                sensor_name=self.name,
                signal_type="file_deleted",
                entity_id=rname,
                details={"event": "repo_removed"},
                timestamp=now,
            ))

        # Changed fields on existing repos
        for rname in sorted(prev_names & curr_names):
            prev = self._last_repos[rname]
            curr = current_repos[rname]
            for field_name in sorted(set(prev.keys()) | set(curr.keys())):
                old_val = prev.get(field_name)
                new_val = curr.get(field_name)
                if old_val != new_val:
                    sig_type = (
                        "promotion_changed"
                        if field_name == "promotion_status"
                        else "registry_updated"
                    )
                    signals.append(RawSignal(
                        sensor_name=self.name,
                        signal_type=sig_type,
                        entity_id=rname,
                        details={
                            "field": field_name,
                            "previous_value": old_val,
                            "value": new_val,
                        },
                        timestamp=now,
                    ))

        self._last_hash = current_hash
        self._last_mtime = current_mtime
        self._last_repos = current_repos
        return signals
