"""Tests for ontologia.sensing.registry_sensor — registry change detection."""

from __future__ import annotations

import json

import pytest

from ontologia.sensing.registry_sensor import RegistrySensor


def _write_registry(path, organs: dict) -> None:
    """Write a minimal registry file."""
    data = {"version": "2.0", "schema_version": "0.5", "organs": organs}
    path.write_text(json.dumps(data))


def _make_organ(repos: list[dict]) -> dict:
    return {"name": "Test", "repositories": repos}


def _make_repo(name: str, **kwargs) -> dict:
    base = {
        "name": name,
        "org": "test-org",
        "status": "ACTIVE",
        "promotion_status": "LOCAL",
        "implementation_status": "ACTIVE",
        "tier": "standard",
        "public": False,
    }
    base.update(kwargs)
    return base


class TestRegistrySensorBasics:
    def test_name(self, tmp_path):
        sensor = RegistrySensor(tmp_path)
        assert sensor.name == "registry"

    def test_not_available_when_no_file(self, tmp_path):
        sensor = RegistrySensor(tmp_path)
        assert sensor.is_available() is False

    def test_available_when_file_exists(self, tmp_path):
        reg_dir = tmp_path / "meta-organvm" / "organvm-corpvs-testamentvm"
        reg_dir.mkdir(parents=True)
        reg_file = reg_dir / "registry-v2.json"
        _write_registry(reg_file, {})
        sensor = RegistrySensor(tmp_path)
        assert sensor.is_available() is True

    def test_scan_no_file_returns_empty(self, tmp_path):
        sensor = RegistrySensor(tmp_path)
        assert sensor.scan() == []


class TestRegistrySensorScanning:
    @pytest.fixture
    def reg_path(self, tmp_path):
        d = tmp_path / "meta-organvm" / "organvm-corpvs-testamentvm"
        d.mkdir(parents=True)
        return d / "registry-v2.json"

    def test_first_scan_returns_empty(self, tmp_path, reg_path):
        """First scan seeds state — returns no signals."""
        _write_registry(reg_path, {
            "ORGAN-I": _make_organ([_make_repo("repo-a")]),
        })
        sensor = RegistrySensor(tmp_path)
        signals = sensor.scan()
        assert signals == []

    def test_no_change_returns_empty(self, tmp_path, reg_path):
        """Second scan with no changes returns no signals."""
        _write_registry(reg_path, {
            "ORGAN-I": _make_organ([_make_repo("repo-a")]),
        })
        sensor = RegistrySensor(tmp_path)
        sensor.scan()  # seed
        signals = sensor.scan()  # no change
        assert signals == []

    def test_detects_new_repo(self, tmp_path, reg_path):
        """Adding a repo produces a file_created signal."""
        _write_registry(reg_path, {
            "ORGAN-I": _make_organ([_make_repo("repo-a")]),
        })
        sensor = RegistrySensor(tmp_path)
        sensor.scan()  # seed

        _write_registry(reg_path, {
            "ORGAN-I": _make_organ([_make_repo("repo-a"), _make_repo("repo-b")]),
        })
        signals = sensor.scan()
        assert len(signals) == 1
        assert signals[0].signal_type == "file_created"
        assert signals[0].entity_id == "repo-b"

    def test_detects_removed_repo(self, tmp_path, reg_path):
        """Removing a repo produces a file_deleted signal."""
        _write_registry(reg_path, {
            "ORGAN-I": _make_organ([_make_repo("repo-a"), _make_repo("repo-b")]),
        })
        sensor = RegistrySensor(tmp_path)
        sensor.scan()  # seed

        _write_registry(reg_path, {
            "ORGAN-I": _make_organ([_make_repo("repo-a")]),
        })
        signals = sensor.scan()
        assert len(signals) == 1
        assert signals[0].signal_type == "file_deleted"
        assert signals[0].entity_id == "repo-b"

    def test_detects_promotion_change(self, tmp_path, reg_path):
        """Changing promotion_status produces a promotion_changed signal."""
        _write_registry(reg_path, {
            "ORGAN-I": _make_organ([_make_repo("repo-a", promotion_status="LOCAL")]),
        })
        sensor = RegistrySensor(tmp_path)
        sensor.scan()  # seed

        _write_registry(reg_path, {
            "ORGAN-I": _make_organ([_make_repo("repo-a", promotion_status="CANDIDATE")]),
        })
        signals = sensor.scan()
        assert len(signals) == 1
        assert signals[0].signal_type == "promotion_changed"
        assert signals[0].details["field"] == "promotion_status"
        assert signals[0].details["previous_value"] == "LOCAL"
        assert signals[0].details["value"] == "CANDIDATE"

    def test_detects_field_change(self, tmp_path, reg_path):
        """Changing tier produces a registry_updated signal."""
        _write_registry(reg_path, {
            "ORGAN-I": _make_organ([_make_repo("repo-a", tier="standard")]),
        })
        sensor = RegistrySensor(tmp_path)
        sensor.scan()

        _write_registry(reg_path, {
            "ORGAN-I": _make_organ([_make_repo("repo-a", tier="flagship")]),
        })
        signals = sensor.scan()
        assert any(s.signal_type == "registry_updated" for s in signals)

    def test_corrupt_file_emits_anomaly(self, tmp_path, reg_path):
        """Corrupt JSON produces a parse_failure signal."""
        _write_registry(reg_path, {"ORGAN-I": _make_organ([_make_repo("repo-a")])})
        sensor = RegistrySensor(tmp_path)
        sensor.scan()

        reg_path.write_text("not valid json at all{{{")
        signals = sensor.scan()
        assert len(signals) == 1
        assert signals[0].details.get("error") == "parse_failure"
