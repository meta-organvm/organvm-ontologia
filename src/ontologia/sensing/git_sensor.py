"""Git sensor -- detect recent git activity across workspace repos."""

from __future__ import annotations

import shutil
import subprocess
from datetime import datetime, timezone
from pathlib import Path

from ontologia.sensing.interfaces import RawSignal


class GitSensor:
    """Scan workspace repos for recent git commits."""

    def __init__(self, workspace: Path, hours: int = 24) -> None:
        self._workspace = workspace
        self._hours = hours

    @property
    def name(self) -> str:
        return "git"

    def is_available(self) -> bool:
        return shutil.which("git") is not None

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def _find_repos(self) -> list[Path]:
        """Walk workspace for .git directories up to depth 3."""
        repos: list[Path] = []
        for depth_1 in self._workspace.iterdir():
            if not depth_1.is_dir() or depth_1.name.startswith("."):
                continue
            # Depth 1: direct repo
            if (depth_1 / ".git").exists():
                repos.append(depth_1)
            # Depth 2-3: organ-dir/repo or superproject/submodule
            for depth_2 in depth_1.iterdir():
                if not depth_2.is_dir() or depth_2.name.startswith("."):
                    continue
                if (depth_2 / ".git").exists():
                    repos.append(depth_2)
                for depth_3 in depth_2.iterdir():
                    if not depth_3.is_dir() or depth_3.name.startswith("."):
                        continue
                    if (depth_3 / ".git").exists():
                        repos.append(depth_3)
        return repos

    def _recent_commits(self, repo_path: Path) -> list[str]:
        """Return oneline commit summaries from the last N hours."""
        try:
            result = subprocess.run(
                [
                    "git", "-C", str(repo_path),
                    "log", "--oneline", f"--since={self._hours} hours ago",
                ],
                capture_output=True,
                text=True,
                timeout=10,
                check=False,
            )
            if result.returncode != 0:
                return []
            return [ln.strip() for ln in result.stdout.splitlines() if ln.strip()]
        except (subprocess.TimeoutExpired, FileNotFoundError, OSError):
            return []

    # ------------------------------------------------------------------
    # Scan
    # ------------------------------------------------------------------

    def scan(self) -> list[RawSignal]:
        if not self.is_available():
            return []

        now = datetime.now(timezone.utc).isoformat()
        signals: list[RawSignal] = []

        for repo_path in self._find_repos():
            commits = self._recent_commits(repo_path)
            if not commits:
                continue
            signals.append(RawSignal(
                sensor_name=self.name,
                signal_type="git_commit",
                entity_id=repo_path.name,
                details={
                    "commit_count": len(commits),
                    "value": commits[0],  # most recent commit summary
                    "path": str(repo_path),
                },
                timestamp=now,
            ))

        return signals
