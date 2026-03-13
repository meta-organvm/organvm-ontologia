"""Tests for ontologia.sensing.scanner — scan orchestration."""

from __future__ import annotations

import pytest

from ontologia.sensing.interfaces import RawSignal
from ontologia.sensing.scanner import scan_all, scan_and_emit


class _StubSensor:
    """Test sensor producing configurable signals."""

    def __init__(self, name: str, signals: list[RawSignal], available: bool = True):
        self._name = name
        self._signals = signals
        self._available = available

    @property
    def name(self) -> str:
        return self._name

    def is_available(self) -> bool:
        return self._available

    def scan(self) -> list[RawSignal]:
        return self._signals


class _FailingSensor:
    """Sensor that raises on scan."""

    @property
    def name(self) -> str:
        return "failing"

    def is_available(self) -> bool:
        return True

    def scan(self) -> list[RawSignal]:
        raise RuntimeError("sensor failure")


# ---------------------------------------------------------------------------
# scan_all
# ---------------------------------------------------------------------------


class TestScanAll:
    def test_empty_sensors(self, tmp_path):
        result = scan_all(tmp_path, sensors=[])
        assert result == []

    def test_single_sensor_with_signals(self, tmp_path):
        sig = RawSignal(sensor_name="test", signal_type="file_created", entity_id="repo-a")
        sensor = _StubSensor("test", [sig])
        result = scan_all(tmp_path, sensors=[sensor])
        assert len(result) >= 1

    def test_unavailable_sensor_skipped(self, tmp_path):
        sig = RawSignal(sensor_name="test", signal_type="file_created")
        sensor = _StubSensor("test", [sig], available=False)
        result = scan_all(tmp_path, sensors=[sensor])
        assert result == []

    def test_failing_sensor_skipped(self, tmp_path):
        """A sensor that raises is silently skipped."""
        good_sig = RawSignal(sensor_name="good", signal_type="file_created", entity_id="x")
        good = _StubSensor("good", [good_sig])
        bad = _FailingSensor()
        result = scan_all(tmp_path, sensors=[bad, good])
        assert len(result) >= 1

    def test_multiple_sensors_combined(self, tmp_path):
        s1 = RawSignal(sensor_name="a", signal_type="file_created", entity_id="r1")
        s2 = RawSignal(sensor_name="b", signal_type="git_commit", entity_id="r2")
        sensors = [_StubSensor("a", [s1]), _StubSensor("b", [s2])]
        result = scan_all(tmp_path, sensors=sensors)
        assert len(result) >= 2


# ---------------------------------------------------------------------------
# scan_and_emit
# ---------------------------------------------------------------------------


class TestScanAndEmit:
    def test_emits_events_to_bus(self, tmp_path):
        sig = RawSignal(sensor_name="test", signal_type="file_created", entity_id="repo-a")
        sensor = _StubSensor("test", [sig])
        count = scan_and_emit(tmp_path, sensors=[sensor])
        assert count >= 1

    def test_returns_zero_with_no_signals(self, tmp_path):
        sensor = _StubSensor("test", [])
        count = scan_and_emit(tmp_path, sensors=[sensor])
        assert count == 0

    def test_returns_zero_when_no_sensors(self, tmp_path):
        count = scan_and_emit(tmp_path, sensors=[])
        assert count == 0


# ---------------------------------------------------------------------------
# Individual sensor constructors
# ---------------------------------------------------------------------------


class TestSensorConstruction:
    def test_filesystem_sensor(self, tmp_path):
        from ontologia.sensing.filesystem_sensor import FilesystemSensor

        sensor = FilesystemSensor(tmp_path)
        assert sensor.name == "filesystem"
        assert sensor.is_available() is True

    def test_ci_sensor(self, tmp_path):
        from ontologia.sensing.ci_sensor import CISensor

        sensor = CISensor(tmp_path)
        assert sensor.name == "ci"
        assert sensor.is_available() is True

    def test_session_sensor_unavailable_by_default(self, tmp_path):
        from ontologia.sensing.session_sensor import SessionSensor

        sensor = SessionSensor(claims_path=tmp_path / "nonexistent.jsonl")
        assert sensor.name == "session"
        assert sensor.is_available() is False

    def test_git_sensor(self, tmp_path):
        from ontologia.sensing.git_sensor import GitSensor

        sensor = GitSensor(tmp_path)
        assert sensor.name == "git"

    def test_registry_sensor(self, tmp_path):
        from ontologia.sensing.registry_sensor import RegistrySensor

        sensor = RegistrySensor(tmp_path)
        assert sensor.name == "registry"


# ---------------------------------------------------------------------------
# Filesystem sensor scan
# ---------------------------------------------------------------------------


class TestFilesystemSensor:
    def test_first_scan_seeds(self, tmp_path):
        from ontologia.sensing.filesystem_sensor import FilesystemSensor

        # Create a repo-like dir
        (tmp_path / "repo-a" / ".git").mkdir(parents=True)
        sensor = FilesystemSensor(tmp_path)
        signals = sensor.scan()
        assert signals == []

    def test_detects_new_repo(self, tmp_path):
        from ontologia.sensing.filesystem_sensor import FilesystemSensor

        (tmp_path / "repo-a" / ".git").mkdir(parents=True)
        sensor = FilesystemSensor(tmp_path)
        sensor.scan()  # seed

        (tmp_path / "repo-b" / ".git").mkdir(parents=True)
        signals = sensor.scan()
        assert len(signals) == 1
        assert signals[0].entity_id == "repo-b"
        assert signals[0].signal_type == "file_created"

    def test_detects_removed_repo(self, tmp_path):
        import shutil

        from ontologia.sensing.filesystem_sensor import FilesystemSensor

        (tmp_path / "repo-a" / ".git").mkdir(parents=True)
        (tmp_path / "repo-b" / ".git").mkdir(parents=True)
        sensor = FilesystemSensor(tmp_path)
        sensor.scan()  # seed

        shutil.rmtree(tmp_path / "repo-b")
        signals = sensor.scan()
        assert len(signals) == 1
        assert signals[0].entity_id == "repo-b"
        assert signals[0].signal_type == "file_deleted"


# ---------------------------------------------------------------------------
# CI sensor scan
# ---------------------------------------------------------------------------


class TestCISensor:
    def test_first_scan_seeds(self, tmp_path):
        from ontologia.sensing.ci_sensor import CISensor

        sensor = CISensor(tmp_path)
        assert sensor.scan() == []

    def test_detects_ci_added(self, tmp_path):
        from ontologia.sensing.ci_sensor import CISensor

        repo = tmp_path / "repo-a"
        (repo / ".git").mkdir(parents=True)
        sensor = CISensor(tmp_path)
        sensor.scan()  # seed (no CI)

        (repo / ".github" / "workflows").mkdir(parents=True)
        (repo / ".github" / "workflows" / "ci.yml").write_text("name: CI")
        signals = sensor.scan()
        assert len(signals) == 1
        assert signals[0].details["event"] == "ci_added"


# ---------------------------------------------------------------------------
# Session sensor scan
# ---------------------------------------------------------------------------


class TestSessionSensor:
    def test_first_scan_seeds(self, tmp_path):
        import json

        from ontologia.sensing.session_sensor import SessionSensor

        claims = tmp_path / "claims.jsonl"
        claims.write_text(json.dumps({"action": "punch_in", "agent": "claude-helm"}) + "\n")
        sensor = SessionSensor(claims_path=claims)
        signals = sensor.scan()
        assert signals == []

    def test_detects_new_claims(self, tmp_path):
        import json

        from ontologia.sensing.session_sensor import SessionSensor

        claims = tmp_path / "claims.jsonl"
        claims.write_text(json.dumps({"action": "punch_in", "agent": "claude-helm"}) + "\n")
        sensor = SessionSensor(claims_path=claims)
        sensor.scan()  # seed

        # Append new claim
        with claims.open("a") as f:
            f.write(json.dumps({"action": "punch_out", "agent": "claude-helm"}) + "\n")
        signals = sensor.scan()
        assert len(signals) == 1
        assert signals[0].entity_id == "claude-helm"
        assert signals[0].details["event"] == "punch_out"
