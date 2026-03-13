"""Tests for the sensing layer: interfaces, detection, normalization."""

import pytest

from ontologia.sensing.interfaces import (
    ChangeType,
    NormalizedChange,
    RawSignal,
    Sensor,
)
from ontologia.sensing.detection import (
    detect_anomaly,
    detect_relation_changes,
    detect_state_changes,
)
from ontologia.sensing.normalization import normalize_batch, normalize_signal


# ---------------------------------------------------------------------------
# Interfaces
# ---------------------------------------------------------------------------

class TestChangeType:
    def test_enum_values(self):
        assert ChangeType.STATE == "state"
        assert ChangeType.RELATION == "relation"
        assert ChangeType.HIERARCHY == "hierarchy"
        assert ChangeType.SEMANTIC == "semantic"
        assert ChangeType.ANOMALY == "anomaly"


class TestRawSignal:
    def test_create_minimal(self):
        sig = RawSignal(sensor_name="fs", signal_type="file_modified")
        assert sig.sensor_name == "fs"
        assert sig.entity_id is None
        assert sig.confidence == 1.0
        assert sig.details == {}

    def test_create_full(self):
        sig = RawSignal(
            sensor_name="git",
            signal_type="git_commit",
            entity_id="ent_repo_ABC",
            details={"sha": "abc123"},
            timestamp="2026-01-01T00:00:00Z",
            confidence=0.9,
        )
        assert sig.entity_id == "ent_repo_ABC"
        assert sig.details["sha"] == "abc123"


class TestNormalizedChange:
    def test_to_dict(self):
        change = NormalizedChange(
            change_type=ChangeType.STATE,
            entity_id="ent_repo_ABC",
            property_name="status",
            previous_value="active",
            new_value="deprecated",
            confidence=0.95,
            source_sensor="registry",
        )
        d = change.to_dict()
        assert d["change_type"] == "state"
        assert d["entity_id"] == "ent_repo_ABC"
        assert d["property_name"] == "status"
        assert d["previous_value"] == "active"
        assert d["new_value"] == "deprecated"

    def test_to_dict_minimal(self):
        change = NormalizedChange(
            change_type=ChangeType.ANOMALY,
            entity_id="ent_repo_X",
        )
        d = change.to_dict()
        assert "property_name" not in d
        assert "previous_value" not in d


class TestSensorProtocol:
    def test_protocol_check(self):
        """A class implementing name, scan, is_available satisfies Sensor."""
        class MockSensor:
            @property
            def name(self) -> str:
                return "mock"
            def scan(self) -> list[RawSignal]:
                return []
            def is_available(self) -> bool:
                return True

        assert isinstance(MockSensor(), Sensor)

    def test_non_sensor_rejected(self):
        class NotASensor:
            pass
        assert not isinstance(NotASensor(), Sensor)


# ---------------------------------------------------------------------------
# Detection
# ---------------------------------------------------------------------------

class TestDetectStateChanges:
    def test_no_changes(self):
        prev = {"status": "active", "count": 5}
        curr = {"status": "active", "count": 5}
        assert detect_state_changes("e1", prev, curr) == []

    def test_value_changed(self):
        prev = {"status": "active"}
        curr = {"status": "deprecated"}
        changes = detect_state_changes("e1", prev, curr)
        assert len(changes) == 1
        assert changes[0].property_name == "status"
        assert changes[0].previous_value == "active"
        assert changes[0].new_value == "deprecated"

    def test_key_added(self):
        prev = {"a": 1}
        curr = {"a": 1, "b": 2}
        changes = detect_state_changes("e1", prev, curr)
        assert len(changes) == 1
        assert changes[0].property_name == "b"
        assert changes[0].previous_value is None
        assert changes[0].new_value == 2

    def test_key_removed(self):
        prev = {"a": 1, "b": 2}
        curr = {"a": 1}
        changes = detect_state_changes("e1", prev, curr)
        assert len(changes) == 1
        assert changes[0].property_name == "b"
        assert changes[0].new_value is None

    def test_multiple_changes(self):
        prev = {"a": 1, "b": 2, "c": 3}
        curr = {"a": 1, "b": 99, "d": 4}
        changes = detect_state_changes("e1", prev, curr)
        props = {c.property_name for c in changes}
        assert props == {"b", "c", "d"}


class TestDetectRelationChanges:
    def test_no_changes(self):
        assert detect_relation_changes("e1", {"r1", "r2"}, {"r1", "r2"}) == []

    def test_added(self):
        changes = detect_relation_changes("e1", {"r1"}, {"r1", "r2"})
        assert len(changes) == 1
        assert changes[0].property_name == "relation_added"
        assert changes[0].new_value == "r2"

    def test_removed(self):
        changes = detect_relation_changes("e1", {"r1", "r2"}, {"r1"})
        assert len(changes) == 1
        assert changes[0].property_name == "relation_removed"
        assert changes[0].previous_value == "r2"

    def test_mixed(self):
        changes = detect_relation_changes("e1", {"r1", "r2"}, {"r2", "r3"})
        assert len(changes) == 2


class TestDetectAnomaly:
    def test_within_range(self):
        assert detect_anomaly("e1", 5.0, (0.0, 10.0)) is None

    def test_above_range(self):
        result = detect_anomaly("e1", 15.0, (0.0, 10.0))
        assert result is not None
        assert result.change_type == ChangeType.ANOMALY
        assert result.new_value == 15.0
        assert result.confidence > 0

    def test_below_range(self):
        result = detect_anomaly("e1", -5.0, (0.0, 10.0))
        assert result is not None
        assert result.change_type == ChangeType.ANOMALY

    def test_boundary_values(self):
        # At exact boundary: low is inclusive, high is inclusive
        assert detect_anomaly("e1", 0.0, (0.0, 10.0)) is None
        assert detect_anomaly("e1", 10.0, (0.0, 10.0)) is None


# ---------------------------------------------------------------------------
# Normalization
# ---------------------------------------------------------------------------

class TestNormalizeSignal:
    def test_known_signal_type(self):
        sig = RawSignal(
            sensor_name="fs",
            signal_type="file_modified",
            entity_id="ent_repo_A",
            details={"value": "changed"},
        )
        result = normalize_signal(sig)
        assert result is not None
        assert result.change_type == ChangeType.STATE
        assert result.entity_id == "ent_repo_A"
        assert result.new_value == "changed"

    def test_unknown_signal_type(self):
        sig = RawSignal(sensor_name="x", signal_type="unknown_type", entity_id="e1")
        assert normalize_signal(sig) is None

    def test_no_entity_id(self):
        sig = RawSignal(sensor_name="x", signal_type="file_modified")
        assert normalize_signal(sig) is None

    def test_hierarchy_signal(self):
        sig = RawSignal(
            sensor_name="git",
            signal_type="git_branch_created",
            entity_id="ent_repo_B",
        )
        result = normalize_signal(sig)
        assert result is not None
        assert result.change_type == ChangeType.HIERARCHY

    def test_anomaly_signal(self):
        sig = RawSignal(
            sensor_name="metrics",
            signal_type="metric_spike",
            entity_id="ent_repo_C",
            confidence=0.8,
        )
        result = normalize_signal(sig)
        assert result is not None
        assert result.change_type == ChangeType.ANOMALY
        assert result.confidence == 0.8

    def test_semantic_signal(self):
        sig = RawSignal(
            sensor_name="content",
            signal_type="content_drift",
            entity_id="ent_doc_D",
        )
        result = normalize_signal(sig)
        assert result is not None
        assert result.change_type == ChangeType.SEMANTIC


class TestNormalizeBatch:
    def test_empty(self):
        assert normalize_batch([]) == []

    def test_filters_bad_signals(self):
        signals = [
            RawSignal(sensor_name="a", signal_type="file_modified", entity_id="e1"),
            RawSignal(sensor_name="b", signal_type="unknown", entity_id="e2"),
            RawSignal(sensor_name="c", signal_type="git_commit"),  # no entity_id
            RawSignal(sensor_name="d", signal_type="edge_added", entity_id="e3"),
        ]
        results = normalize_batch(signals)
        assert len(results) == 2
        assert results[0].entity_id == "e1"
        assert results[1].entity_id == "e3"
