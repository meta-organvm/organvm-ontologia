"""Integration tests — full sensor → scanner → bus pipeline.

These tests verify the complete chain: a sensor detects a change,
the scanner normalizes it, and scan_and_emit writes events to the
ontologia event bus JSONL file.
"""

from __future__ import annotations

import json
from pathlib import Path

import pytest

from ontologia.events import bus
from ontologia.sensing.interfaces import NormalizedChange, RawSignal
from ontologia.sensing.scanner import scan_all, scan_and_emit


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


class _StubSensor:
    """Configurable sensor for integration testing."""

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


# ---------------------------------------------------------------------------
# scan_all → normalization integration
# ---------------------------------------------------------------------------


class TestScanAllIntegration:
    def test_signals_normalized_to_changes(self, tmp_path):
        """Raw signals become NormalizedChange objects."""
        sig = RawSignal(
            sensor_name="test",
            signal_type="file_created",
            entity_id="repo-a",
        )
        sensor = _StubSensor("test", [sig])
        changes = scan_all(tmp_path, sensors=[sensor])
        assert len(changes) >= 1
        assert all(isinstance(c, NormalizedChange) for c in changes)

    def test_multiple_sensors_combined(self, tmp_path):
        """Changes from different sensors are merged into one list."""
        s1 = RawSignal(sensor_name="fs", signal_type="file_created", entity_id="repo-a")
        s2 = RawSignal(sensor_name="ci", signal_type="file_modified", entity_id="repo-b")
        sensors = [
            _StubSensor("fs", [s1]),
            _StubSensor("ci", [s2]),
        ]
        changes = scan_all(tmp_path, sensors=sensors)
        assert len(changes) >= 2
        entity_ids = {c.entity_id for c in changes}
        assert "repo-a" in entity_ids
        assert "repo-b" in entity_ids

    def test_source_sensor_preserved(self, tmp_path):
        """NormalizedChange.source_sensor reflects the originating sensor."""
        sig = RawSignal(
            sensor_name="registry",
            signal_type="promotion_changed",
            entity_id="repo-x",
            details={"field": "promotion_status", "value": "CANDIDATE"},
        )
        sensor = _StubSensor("registry", [sig])
        changes = scan_all(tmp_path, sensors=[sensor])
        assert any(c.source_sensor == "registry" for c in changes)


# ---------------------------------------------------------------------------
# scan_and_emit → event bus integration
# ---------------------------------------------------------------------------


class TestScanAndEmitIntegration:
    def test_events_written_to_bus(self, tmp_path):
        """scan_and_emit writes events to the ontologia event bus file."""
        sig = RawSignal(
            sensor_name="test",
            signal_type="file_created",
            entity_id="repo-new",
        )
        sensor = _StubSensor("test", [sig])

        events_path = tmp_path / "events.jsonl"
        bus.set_events_path(events_path)

        count = scan_and_emit(tmp_path, sensors=[sensor])
        assert count >= 1

        # Verify events landed in the JSONL file
        lines = events_path.read_text().strip().splitlines()
        assert len(lines) >= 1
        event = json.loads(lines[-1])
        assert "sensor." in event["event_type"]
        assert "sensor:" in event["source"]

    def test_event_type_format(self, tmp_path):
        """Event types follow 'sensor.<change_type>' format."""
        sig = RawSignal(
            sensor_name="test",
            signal_type="file_created",
            entity_id="repo-a",
        )
        sensor = _StubSensor("test", [sig])

        events_path = tmp_path / "events.jsonl"
        bus.set_events_path(events_path)

        scan_and_emit(tmp_path, sensors=[sensor])
        lines = events_path.read_text().strip().splitlines()
        for line in lines:
            event = json.loads(line)
            assert event["event_type"].startswith("sensor.")

    def test_source_includes_sensor_name(self, tmp_path):
        """Event source is 'sensor:<sensor_name>'."""
        sig = RawSignal(
            sensor_name="myfs",
            signal_type="file_deleted",
            entity_id="repo-gone",
        )
        sensor = _StubSensor("myfs", [sig])

        events_path = tmp_path / "events.jsonl"
        bus.set_events_path(events_path)

        scan_and_emit(tmp_path, sensors=[sensor])
        lines = events_path.read_text().strip().splitlines()
        event = json.loads(lines[-1])
        assert event["source"] == "sensor:myfs"

    def test_subject_entity_in_event(self, tmp_path):
        """Events carry the entity_id as subject_entity."""
        sig = RawSignal(
            sensor_name="test",
            signal_type="git_commit",
            entity_id="important-repo",
        )
        sensor = _StubSensor("test", [sig])

        events_path = tmp_path / "events.jsonl"
        bus.set_events_path(events_path)

        scan_and_emit(tmp_path, sensors=[sensor])
        lines = events_path.read_text().strip().splitlines()
        event = json.loads(lines[-1])
        assert event.get("subject_entity") == "important-repo"

    def test_zero_events_with_no_signals(self, tmp_path):
        """Empty sensor scan produces zero events."""
        sensor = _StubSensor("empty", [])

        events_path = tmp_path / "events.jsonl"
        bus.set_events_path(events_path)

        count = scan_and_emit(tmp_path, sensors=[sensor])
        assert count == 0

    def test_zero_events_with_no_sensors(self, tmp_path):
        """No sensors produces zero events."""
        events_path = tmp_path / "events.jsonl"
        bus.set_events_path(events_path)

        count = scan_and_emit(tmp_path, sensors=[])
        assert count == 0


# ---------------------------------------------------------------------------
# Real sensor integration (filesystem)
# ---------------------------------------------------------------------------


class TestFilesystemSensorIntegration:
    def test_full_pipeline_with_filesystem_sensor(self, tmp_path):
        """FilesystemSensor → scan_and_emit produces real events."""
        from ontologia.sensing.filesystem_sensor import FilesystemSensor

        # Create initial repo structure
        (tmp_path / "repo-a" / ".git").mkdir(parents=True)
        sensor = FilesystemSensor(tmp_path)
        sensor.scan()  # seed

        # Add a new repo
        (tmp_path / "repo-b" / ".git").mkdir(parents=True)

        events_path = tmp_path / "events.jsonl"
        bus.set_events_path(events_path)

        count = scan_and_emit(tmp_path, sensors=[sensor])
        assert count >= 1

        lines = events_path.read_text().strip().splitlines()
        assert len(lines) >= 1
        event = json.loads(lines[-1])
        assert event.get("subject_entity") == "repo-b"


# ---------------------------------------------------------------------------
# Real sensor integration (CI)
# ---------------------------------------------------------------------------


class TestCISensorIntegration:
    def test_ci_added_flows_to_bus(self, tmp_path):
        """CISensor detects CI addition → event appears in bus."""
        from ontologia.sensing.ci_sensor import CISensor

        repo = tmp_path / "repo-a"
        (repo / ".git").mkdir(parents=True)
        sensor = CISensor(tmp_path)
        sensor.scan()  # seed

        # Add CI
        (repo / ".github" / "workflows").mkdir(parents=True)
        (repo / ".github" / "workflows" / "ci.yml").write_text("name: CI")

        events_path = tmp_path / "events.jsonl"
        bus.set_events_path(events_path)

        count = scan_and_emit(tmp_path, sensors=[sensor])
        assert count >= 1


# ---------------------------------------------------------------------------
# Real sensor integration (registry)
# ---------------------------------------------------------------------------


class TestRegistrySensorIntegration:
    def _write_reg(self, path: Path, organs: dict) -> None:
        data = {"version": "2.0", "schema_version": "0.5", "organs": organs}
        path.write_text(json.dumps(data))

    def test_registry_change_flows_to_bus(self, tmp_path):
        """RegistrySensor detects new repo → event appears in bus."""
        from ontologia.sensing.registry_sensor import RegistrySensor

        reg_dir = tmp_path / "meta-organvm" / "organvm-corpvs-testamentvm"
        reg_dir.mkdir(parents=True)
        reg_file = reg_dir / "registry-v2.json"

        self._write_reg(reg_file, {
            "ORGAN-I": {
                "name": "Test",
                "repositories": [{
                    "name": "repo-a",
                    "org": "test",
                    "status": "ACTIVE",
                    "promotion_status": "LOCAL",
                    "implementation_status": "ACTIVE",
                    "tier": "standard",
                    "public": False,
                }],
            },
        })

        sensor = RegistrySensor(tmp_path)
        sensor.scan()  # seed

        # Add a repo
        self._write_reg(reg_file, {
            "ORGAN-I": {
                "name": "Test",
                "repositories": [
                    {
                        "name": "repo-a",
                        "org": "test",
                        "status": "ACTIVE",
                        "promotion_status": "LOCAL",
                        "implementation_status": "ACTIVE",
                        "tier": "standard",
                        "public": False,
                    },
                    {
                        "name": "repo-b",
                        "org": "test",
                        "status": "ACTIVE",
                        "promotion_status": "LOCAL",
                        "implementation_status": "ACTIVE",
                        "tier": "standard",
                        "public": False,
                    },
                ],
            },
        })

        events_path = tmp_path / "events.jsonl"
        bus.set_events_path(events_path)

        count = scan_and_emit(tmp_path, sensors=[sensor])
        assert count >= 1

        lines = events_path.read_text().strip().splitlines()
        found = any(
            json.loads(line).get("subject_entity") == "repo-b"
            for line in lines
        )
        assert found
