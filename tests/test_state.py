"""Tests for the state layer: snapshot, runtime, temporal, recovery."""

import json
from pathlib import Path

import pytest

from ontologia.entity.identity import (
    EntityIdentity,
    EntityType,
    LifecycleStatus,
    create_entity,
)
from ontologia.entity.naming import NameIndex, add_name
from ontologia.events import bus
from ontologia.state.recovery import RecoveredState, recover_from_events, verify_recovery
from ontologia.state.runtime import RuntimeState, compute_runtime
from ontologia.state.snapshot import (
    StateSnapshot,
    compare_snapshots,
    create_snapshot,
    has_drift,
)
from ontologia.state.temporal import (
    children_at,
    entity_names_at,
    entity_state_at,
    hierarchy_at,
    parent_at,
    primary_name_at,
    relations_at,
    structural_diff,
)
from ontologia.structure.edges import EdgeIndex, HierarchyEdge, RelationEdge


# ---------------------------------------------------------------------------
# Snapshot
# ---------------------------------------------------------------------------

class TestStateSnapshot:
    def test_create_snapshot(self):
        snap = create_snapshot(
            "ent_repo_A",
            properties={"status": "active"},
            metric_values={"loc": 1000.0},
            structural_fingerprint="abc123",
            variable_bindings={"TIER": "standard"},
        )
        assert snap.snapshot_id.startswith("snap_")
        assert snap.entity_id == "ent_repo_A"
        assert snap.properties["status"] == "active"
        assert snap.metric_values["loc"] == 1000.0
        assert snap.structural_fingerprint == "abc123"

    def test_serialization_roundtrip(self):
        snap = create_snapshot("e1", properties={"a": 1}, metric_values={"m": 2.5})
        d = snap.to_dict()
        snap2 = StateSnapshot.from_dict(d)
        assert snap2.snapshot_id == snap.snapshot_id
        assert snap2.properties == {"a": 1}
        assert snap2.metric_values == {"m": 2.5}

    def test_minimal_serialization(self):
        snap = StateSnapshot(snapshot_id="s1", entity_id="e1", timestamp="t1")
        d = snap.to_dict()
        assert "properties" not in d
        assert "metric_values" not in d


class TestCompareSnapshots:
    def test_identical(self):
        a = create_snapshot("e1", properties={"x": 1}, metric_values={"m": 5.0})
        b = StateSnapshot(
            snapshot_id="s2", entity_id="e1", timestamp="t2",
            properties={"x": 1}, metric_values={"m": 5.0},
        )
        diff = compare_snapshots(a, b)
        assert diff["changed_properties"] == {}
        assert diff["changed_metrics"] == {}
        assert diff["fingerprint_changed"] is False

    def test_property_changed(self):
        a = create_snapshot("e1", properties={"status": "active"})
        b = StateSnapshot(
            snapshot_id="s2", entity_id="e1", timestamp="t2",
            properties={"status": "deprecated"},
        )
        diff = compare_snapshots(a, b)
        assert "status" in diff["changed_properties"]
        assert diff["changed_properties"]["status"]["old"] == "active"
        assert diff["changed_properties"]["status"]["new"] == "deprecated"

    def test_metric_changed(self):
        a = create_snapshot("e1", metric_values={"loc": 100.0})
        b = StateSnapshot(
            snapshot_id="s2", entity_id="e1", timestamp="t2",
            metric_values={"loc": 200.0},
        )
        diff = compare_snapshots(a, b)
        assert "loc" in diff["changed_metrics"]

    def test_fingerprint_changed(self):
        a = create_snapshot("e1", structural_fingerprint="aaa")
        b = StateSnapshot(
            snapshot_id="s2", entity_id="e1", timestamp="t2",
            structural_fingerprint="bbb",
        )
        diff = compare_snapshots(a, b)
        assert diff["fingerprint_changed"] is True

    def test_variable_changed(self):
        a = create_snapshot("e1", variable_bindings={"TIER": "standard"})
        b = StateSnapshot(
            snapshot_id="s2", entity_id="e1", timestamp="t2",
            variable_bindings={"TIER": "flagship"},
        )
        diff = compare_snapshots(a, b)
        assert "TIER" in diff["changed_variables"]


class TestHasDrift:
    def test_no_drift(self):
        a = create_snapshot("e1", properties={"x": 1})
        b = StateSnapshot(
            snapshot_id="s2", entity_id="e1", timestamp="t2",
            properties={"x": 1},
        )
        assert has_drift(a, b) is False

    def test_drift_from_properties(self):
        a = create_snapshot("e1", properties={"x": 1})
        b = StateSnapshot(
            snapshot_id="s2", entity_id="e1", timestamp="t2",
            properties={"x": 2},
        )
        assert has_drift(a, b) is True


# ---------------------------------------------------------------------------
# Runtime
# ---------------------------------------------------------------------------

class TestRuntimeState:
    def test_compute_empty(self):
        rt = compute_runtime({}, EdgeIndex())
        assert rt.active_entities == {}
        assert rt.active_hierarchy == []
        assert rt.active_relations == []

    def test_compute_with_entities(self):
        entities = {
            "e1": EntityIdentity(uid="e1", entity_type=EntityType.REPO),
            "e2": EntityIdentity(
                uid="e2", entity_type=EntityType.REPO,
                lifecycle_status=LifecycleStatus.ARCHIVED,
            ),
            "e3": EntityIdentity(uid="e3", entity_type=EntityType.ORGAN),
        }
        rt = compute_runtime(entities, EdgeIndex())
        # Only active entities
        assert len(rt.active_entities) == 2
        assert "e1" in rt.active_entities
        assert "e3" in rt.active_entities
        assert "e2" not in rt.active_entities
        # Type counts include all
        assert rt.entity_count_by_type["repo"] == 2
        assert rt.entity_count_by_type["organ"] == 1
        # Status counts include all
        assert rt.entity_count_by_status["active"] == 2
        assert rt.entity_count_by_status["archived"] == 1

    def test_compute_with_edges(self):
        idx = EdgeIndex()
        idx.add_hierarchy(HierarchyEdge("root", "a", "2025-01-01T00:00:00Z"))
        idx.add_relation(RelationEdge("a", "b", "depends_on", "2025-01-01T00:00:00Z"))
        rt = compute_runtime({}, idx)
        assert len(rt.active_hierarchy) == 1
        assert len(rt.active_relations) == 1

    def test_compute_with_retired_edges(self):
        idx = EdgeIndex()
        idx.add_hierarchy(HierarchyEdge(
            "root", "a", "2025-01-01T00:00:00Z", valid_to="2025-06-01T00:00:00Z",
        ))
        idx.add_hierarchy(HierarchyEdge("root", "b", "2025-01-01T00:00:00Z"))
        rt = compute_runtime({}, idx)
        assert len(rt.active_hierarchy) == 1  # only the active one

    def test_summary(self):
        entities = {
            "e1": EntityIdentity(uid="e1", entity_type=EntityType.REPO),
        }
        rt = compute_runtime(entities, EdgeIndex(), resolved_variables={"X": 1})
        s = rt.summary()
        assert s["total_entities"] == 1
        assert s["resolved_variable_count"] == 1


# ---------------------------------------------------------------------------
# Temporal
# ---------------------------------------------------------------------------

class TestTemporalQueries:
    @pytest.fixture
    def temporal_setup(self):
        """Edge index with time-varying structure."""
        idx = EdgeIndex()
        # root → A (Jan-Jun 2025)
        idx.add_hierarchy(HierarchyEdge(
            "root", "A", "2025-01-01T00:00:00Z", valid_to="2025-06-01T00:00:00Z",
        ))
        # root → B (always active)
        idx.add_hierarchy(HierarchyEdge("root", "B", "2025-01-01T00:00:00Z"))
        # root2 → A (from Jun 2025 — A moved)
        idx.add_hierarchy(HierarchyEdge("root2", "A", "2025-06-01T00:00:00Z"))
        # Relation: A → B (always)
        idx.add_relation(RelationEdge("A", "B", "depends_on", "2025-01-01T00:00:00Z"))
        # Relation: A → C (only first half of 2025)
        idx.add_relation(RelationEdge(
            "A", "C", "produces_for", "2025-01-01T00:00:00Z",
            valid_to="2025-06-01T00:00:00Z",
        ))
        return idx

    def test_hierarchy_at_early(self, temporal_setup):
        edges = hierarchy_at(temporal_setup, "2025-03-01T00:00:00Z")
        parent_child = {(e.parent_id, e.child_id) for e in edges}
        assert ("root", "A") in parent_child
        assert ("root", "B") in parent_child
        assert ("root2", "A") not in parent_child  # not yet

    def test_hierarchy_at_late(self, temporal_setup):
        edges = hierarchy_at(temporal_setup, "2025-09-01T00:00:00Z")
        parent_child = {(e.parent_id, e.child_id) for e in edges}
        assert ("root", "A") not in parent_child  # expired
        assert ("root2", "A") in parent_child      # now active
        assert ("root", "B") in parent_child        # still active

    def test_relations_at(self, temporal_setup):
        early = relations_at(temporal_setup, "2025-03-01T00:00:00Z")
        assert len(early) == 2
        late = relations_at(temporal_setup, "2025-09-01T00:00:00Z")
        assert len(late) == 1  # only depends_on remains

    def test_relations_at_filtered(self, temporal_setup):
        early = relations_at(
            temporal_setup, "2025-03-01T00:00:00Z", relation_type="produces_for",
        )
        assert len(early) == 1

    def test_children_at(self, temporal_setup):
        early = children_at(temporal_setup, "root", "2025-03-01T00:00:00Z")
        assert set(early) == {"A", "B"}
        late = children_at(temporal_setup, "root", "2025-09-01T00:00:00Z")
        assert set(late) == {"B"}

    def test_parent_at(self, temporal_setup):
        early = parent_at(temporal_setup, "A", "2025-03-01T00:00:00Z")
        assert early == "root"
        late = parent_at(temporal_setup, "A", "2025-09-01T00:00:00Z")
        assert late == "root2"

    def test_structural_diff(self, temporal_setup):
        diff = structural_diff(
            temporal_setup, "A",
            "2025-03-01T00:00:00Z", "2025-09-01T00:00:00Z",
        )
        assert diff["parent_changed"] is True
        assert diff["parent_a"] == "root"
        assert diff["parent_b"] == "root2"
        assert len(diff["relations_removed"]) == 1  # produces_for expired


class TestTemporalNames:
    def test_entity_names_at(self):
        from ontologia.entity.naming import NameRecord

        idx = NameIndex()
        # Create name with explicit past valid_from
        rec1 = NameRecord(
            entity_id="e1", display_name="Original Name", slug="original-name",
            valid_from="2025-01-01T00:00:00Z", is_primary=True, source="system",
        )
        idx.add(rec1)

        # Retire old, add new (simulating rename at June)
        rec1.valid_to = "2025-06-01T00:00:00Z"
        rec2 = NameRecord(
            entity_id="e1", display_name="New Name", slug="new-name",
            valid_from="2025-06-01T00:00:00Z", is_primary=True, source="system",
        )
        idx.add(rec2)

        early = entity_names_at(idx, "e1", "2025-03-01T00:00:00Z")
        assert any(n.display_name == "Original Name" for n in early)

        late = entity_names_at(idx, "e1", "2025-09-01T00:00:00Z")
        assert any(n.display_name == "New Name" for n in late)
        assert not any(n.display_name == "Original Name" for n in late)

    def test_primary_name_at(self):
        from ontologia.entity.naming import NameRecord

        idx = NameIndex()
        rec = NameRecord(
            entity_id="e1", display_name="V1", slug="v1",
            valid_from="2025-01-01T00:00:00Z", is_primary=True, source="system",
        )
        idx.add(rec)
        name = primary_name_at(idx, "e1", "2025-03-01T00:00:00Z")
        assert name is not None
        assert name.display_name == "V1"

    def test_entity_state_at(self):
        from ontologia.entity.naming import NameRecord

        entity = EntityIdentity(uid="e1", entity_type=EntityType.REPO)
        idx = EdgeIndex()
        idx.add_hierarchy(HierarchyEdge("root", "e1", "2025-01-01T00:00:00Z"))
        name_idx = NameIndex()
        rec = NameRecord(
            entity_id="e1", display_name="My Repo", slug="my-repo",
            valid_from="2025-01-01T00:00:00Z", is_primary=True, source="system",
        )
        name_idx.add(rec)

        state = entity_state_at(entity, idx, name_idx, "2025-06-01T00:00:00Z")
        assert state["entity_id"] == "e1"
        assert state["display_name"] == "My Repo"
        assert state["parent_id"] == "root"


# ---------------------------------------------------------------------------
# Recovery
# ---------------------------------------------------------------------------

class TestRecovery:
    def _write_events(self, path: Path, events: list[dict]) -> None:
        path.parent.mkdir(parents=True, exist_ok=True)
        with path.open("w") as f:
            for e in events:
                f.write(json.dumps(e) + "\n")

    def test_recover_entity_creation(self, tmp_path):
        events_path = tmp_path / "events.jsonl"
        self._write_events(events_path, [
            {
                "event_type": "entity.created",
                "source": "bootstrap",
                "subject_entity": "ent_repo_ABC",
                "timestamp": "2025-01-01T00:00:00Z",
                "payload": {
                    "entity_type": "repo",
                    "display_name": "My Repo",
                },
            },
        ])
        state = recover_from_events(events_path)
        assert state.events_replayed == 1
        assert "ent_repo_ABC" in state.entities
        assert state.entities["ent_repo_ABC"].entity_type == EntityType.REPO

        # Name should be recovered too
        name = state.name_index.current_name("ent_repo_ABC")
        assert name is not None
        assert name.display_name == "My Repo"

    def test_recover_rename(self, tmp_path):
        events_path = tmp_path / "events.jsonl"
        self._write_events(events_path, [
            {
                "event_type": "entity.created",
                "source": "bootstrap",
                "subject_entity": "e1",
                "timestamp": "2025-01-01T00:00:00Z",
                "payload": {"entity_type": "repo", "display_name": "Old Name"},
            },
            {
                "event_type": "entity.renamed",
                "source": "user",
                "subject_entity": "e1",
                "new_value": "New Name",
                "timestamp": "2025-06-01T00:00:00Z",
            },
        ])
        state = recover_from_events(events_path)
        assert state.events_replayed == 2
        name = state.name_index.current_name("e1")
        assert name is not None
        assert name.display_name == "New Name"

    def test_recover_lifecycle_change(self, tmp_path):
        events_path = tmp_path / "events.jsonl"
        self._write_events(events_path, [
            {
                "event_type": "entity.created",
                "source": "system",
                "subject_entity": "e1",
                "timestamp": "2025-01-01T00:00:00Z",
                "payload": {"entity_type": "repo", "display_name": "Test"},
            },
            {
                "event_type": "entity.deprecated",
                "source": "policy",
                "subject_entity": "e1",
                "timestamp": "2025-06-01T00:00:00Z",
            },
        ])
        state = recover_from_events(events_path)
        assert state.entities["e1"].lifecycle_status == LifecycleStatus.DEPRECATED

    def test_recover_up_to_timestamp(self, tmp_path):
        events_path = tmp_path / "events.jsonl"
        self._write_events(events_path, [
            {
                "event_type": "entity.created",
                "source": "system",
                "subject_entity": "e1",
                "timestamp": "2025-01-01T00:00:00Z",
                "payload": {"entity_type": "repo", "display_name": "Repo"},
            },
            {
                "event_type": "entity.deprecated",
                "source": "system",
                "subject_entity": "e1",
                "timestamp": "2025-09-01T00:00:00Z",
            },
        ])
        # Recover only up to March — entity should still be active
        state = recover_from_events(events_path, up_to="2025-06-01T00:00:00Z")
        assert state.events_replayed == 1
        assert state.entities["e1"].lifecycle_status == LifecycleStatus.ACTIVE

    def test_recover_empty_log(self, tmp_path):
        events_path = tmp_path / "events.jsonl"
        events_path.write_text("")
        state = recover_from_events(events_path)
        assert state.events_replayed == 0
        assert state.entities == {}

    def test_recover_alias_addition(self, tmp_path):
        events_path = tmp_path / "events.jsonl"
        self._write_events(events_path, [
            {
                "event_type": "entity.created",
                "source": "system",
                "subject_entity": "e1",
                "timestamp": "2025-01-01T00:00:00Z",
                "payload": {"entity_type": "repo", "display_name": "Main"},
            },
            {
                "event_type": "name.added",
                "source": "user",
                "subject_entity": "e1",
                "new_value": "Alias One",
                "timestamp": "2025-03-01T00:00:00Z",
            },
        ])
        state = recover_from_events(events_path)
        all_names = state.name_index.all_names("e1")
        assert len(all_names) == 2

    def test_verify_recovery_match(self, tmp_path):
        events_path = tmp_path / "events.jsonl"
        self._write_events(events_path, [
            {
                "event_type": "entity.created",
                "source": "system",
                "subject_entity": "e1",
                "timestamp": "2025-01-01T00:00:00Z",
                "payload": {"entity_type": "repo", "display_name": "Test"},
            },
        ])
        expected = {
            "e1": EntityIdentity(uid="e1", entity_type=EntityType.REPO),
        }
        result = verify_recovery(events_path, expected)
        assert result["match"] is True

    def test_verify_recovery_mismatch(self, tmp_path):
        events_path = tmp_path / "events.jsonl"
        self._write_events(events_path, [
            {
                "event_type": "entity.created",
                "source": "system",
                "subject_entity": "e1",
                "timestamp": "2025-01-01T00:00:00Z",
                "payload": {"entity_type": "repo", "display_name": "Test"},
            },
        ])
        expected = {
            "e1": EntityIdentity(uid="e1", entity_type=EntityType.ORGAN),  # wrong type
        }
        result = verify_recovery(events_path, expected)
        assert result["match"] is False
        assert len(result["mismatched_fields"]) > 0
