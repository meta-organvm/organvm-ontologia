"""CI sensor -- detect CI workflow presence and changes."""

from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path

from ontologia.sensing.interfaces import RawSignal


class CISensor:
    """Scan workspace repos for .github/workflows/ CI presence."""

    def __init__(self, workspace: Path) -> None:
        self._workspace = workspace
        self._last_state: dict[str, bool] | None = None

    @property
    def name(self) -> str:
        return "ci"

    def is_available(self) -> bool:
        return True

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def _scan_ci_state(self) -> dict[str, bool]:
        """Build a map of repo_name -> has_ci for all repos in workspace."""
        state: dict[str, bool] = {}
        for depth_1 in self._workspace.iterdir():
            if not depth_1.is_dir() or depth_1.name.startswith("."):
                continue
            # Direct repo at depth 1
            if (depth_1 / ".git").exists() or (depth_1 / "seed.yaml").exists():
                state[depth_1.name] = self._has_ci(depth_1)
            # Depth 2: organ-dir/repo or superproject/submodule
            for depth_2 in depth_1.iterdir():
                if not depth_2.is_dir() or depth_2.name.startswith("."):
                    continue
                if (depth_2 / ".git").exists() or (depth_2 / "seed.yaml").exists():
                    state[depth_2.name] = self._has_ci(depth_2)
        return state

    @staticmethod
    def _has_ci(repo_path: Path) -> bool:
        """Check if a repo has at least one CI workflow YAML."""
        workflows_dir = repo_path / ".github" / "workflows"
        if not workflows_dir.is_dir():
            return False
        return any(
            f.suffix in (".yml", ".yaml")
            for f in workflows_dir.iterdir()
            if f.is_file()
        )

    # ------------------------------------------------------------------
    # Scan
    # ------------------------------------------------------------------

    def scan(self) -> list[RawSignal]:
        current_state = self._scan_ci_state()

        # First run -- seed state, no signals.
        if self._last_state is None:
            self._last_state = current_state
            return []

        now = datetime.now(timezone.utc).isoformat()
        signals: list[RawSignal] = []

        all_repos = set(self._last_state.keys()) | set(current_state.keys())

        for repo_name in sorted(all_repos):
            had_ci = self._last_state.get(repo_name, False)
            has_ci = current_state.get(repo_name, False)

            if not had_ci and has_ci:
                signals.append(RawSignal(
                    sensor_name=self.name,
                    signal_type="file_created",
                    entity_id=repo_name,
                    details={"event": "ci_added", "value": True},
                    timestamp=now,
                ))
            elif had_ci and not has_ci:
                signals.append(RawSignal(
                    sensor_name=self.name,
                    signal_type="file_deleted",
                    entity_id=repo_name,
                    details={"event": "ci_removed", "value": False},
                    timestamp=now,
                ))

        self._last_state = current_state
        return signals
