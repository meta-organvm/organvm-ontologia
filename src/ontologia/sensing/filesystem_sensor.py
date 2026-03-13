"""Filesystem sensor -- detect workspace structure changes."""

from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path

from ontologia.sensing.interfaces import RawSignal


class FilesystemSensor:
    """Watch workspace for appearing/disappearing repo directories."""

    def __init__(self, workspace: Path) -> None:
        self._workspace = workspace
        self._last_repos: set[str] | None = None

    @property
    def name(self) -> str:
        return "filesystem"

    def is_available(self) -> bool:
        return self._workspace.is_dir()

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def _discover_repos(self) -> set[str]:
        """Find all directories that look like repos (have .git or seed.yaml)."""
        repos: set[str] = set()
        for depth_1 in self._workspace.iterdir():
            if not depth_1.is_dir() or depth_1.name.startswith("."):
                continue
            # Direct repo at depth 1
            if (depth_1 / ".git").exists() or (depth_1 / "seed.yaml").exists():
                repos.add(depth_1.name)
            # Depth 2: organ-dir/repo or superproject/submodule
            for depth_2 in depth_1.iterdir():
                if not depth_2.is_dir() or depth_2.name.startswith("."):
                    continue
                if (depth_2 / ".git").exists() or (depth_2 / "seed.yaml").exists():
                    repos.add(depth_2.name)
        return repos

    # ------------------------------------------------------------------
    # Scan
    # ------------------------------------------------------------------

    def scan(self) -> list[RawSignal]:
        if not self.is_available():
            return []

        current_repos = self._discover_repos()

        # First run -- seed state, no signals.
        if self._last_repos is None:
            self._last_repos = current_repos
            return []

        now = datetime.now(timezone.utc).isoformat()
        signals: list[RawSignal] = []

        # New repos appearing
        for rname in sorted(current_repos - self._last_repos):
            signals.append(RawSignal(
                sensor_name=self.name,
                signal_type="file_created",
                entity_id=rname,
                details={"event": "repo_appeared", "value": rname},
                timestamp=now,
            ))

        # Repos disappearing
        for rname in sorted(self._last_repos - current_repos):
            signals.append(RawSignal(
                sensor_name=self.name,
                signal_type="file_deleted",
                entity_id=rname,
                details={"event": "repo_disappeared", "value": rname},
                timestamp=now,
            ))

        self._last_repos = current_repos
        return signals
