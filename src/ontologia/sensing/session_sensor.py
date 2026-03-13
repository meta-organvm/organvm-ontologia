"""Session sensor -- detect agent session activity from claims log."""

from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path

from ontologia.sensing.interfaces import RawSignal

_DEFAULT_CLAIMS_PATH = Path.home() / ".organvm" / "claims.jsonl"


class SessionSensor:
    """Watch the multi-agent claims JSONL for session activity."""

    def __init__(self, claims_path: Path | None = None) -> None:
        self._claims_path = claims_path or _DEFAULT_CLAIMS_PATH
        self._last_line_count: int = 0

    @property
    def name(self) -> str:
        return "session"

    def is_available(self) -> bool:
        return self._claims_path.is_file()

    # ------------------------------------------------------------------
    # Scan
    # ------------------------------------------------------------------

    def scan(self) -> list[RawSignal]:
        if not self.is_available():
            return []

        lines = self._claims_path.read_text().splitlines()

        # First run -- seed state, no signals.
        if self._last_line_count == 0:
            self._last_line_count = len(lines)
            return []

        # Only look at new lines since last scan.
        new_lines = lines[self._last_line_count:]
        self._last_line_count = len(lines)

        if not new_lines:
            return []

        now = datetime.now(timezone.utc).isoformat()
        signals: list[RawSignal] = []

        for line in new_lines:
            line = line.strip()
            if not line:
                continue
            try:
                entry = json.loads(line)
            except json.JSONDecodeError:
                continue

            action = entry.get("action", "")
            agent = entry.get("agent", entry.get("handle", ""))

            if action in ("punch_in", "punch_out") and agent:
                signals.append(RawSignal(
                    sensor_name=self.name,
                    signal_type="registry_updated",
                    entity_id=agent,
                    details={
                        "event": action,
                        "value": action,
                        "organ": entry.get("organ", ""),
                        "repo": entry.get("repo", ""),
                        "scope": entry.get("scope", ""),
                    },
                    timestamp=entry.get("timestamp", now),
                ))

        return signals
