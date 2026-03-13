"""Tests for the inference layer: engine, clusters, propagation, tension."""

import pytest

from ontologia.entity.identity import EntityIdentity, EntityType, LifecycleStatus
from ontologia.entity.naming import NameIndex, add_name
from ontologia.inference.clusters import (
    Cluster,
    clusters_to_inferences,
    detect_clusters_from_relations,
)
from ontologia.inference.engine import InferenceResult, InferenceType
from ontologia.inference.propagation import (
    PropagationPath,
    full_blast_radius,
    propagate_downward,
    propagate_lateral,
    propagate_upward,
)
from ontologia.inference.tension import (
    TensionIndicator,
    TensionType,
    detect_naming_conflicts,
    detect_orphans,
    detect_overcoupling,
)
from ontologia.structure.edges import EdgeIndex, HierarchyEdge, RelationEdge


# ---------------------------------------------------------------------------
# Shared fixtures
# ---------------------------------------------------------------------------

def _make_edge_index_tree() -> EdgeIndex:
    """Build: root → A → B, root → C."""
    idx = EdgeIndex()
    idx.add_hierarchy(HierarchyEdge("root", "A", "2025-01-01T00:00:00Z"))
    idx.add_hierarchy(HierarchyEdge("A", "B", "2025-01-01T00:00:00Z"))
    idx.add_hierarchy(HierarchyEdge("root", "C", "2025-01-01T00:00:00Z"))
    return idx


def _make_edge_index_with_relations() -> EdgeIndex:
    """Tree + relation edges for testing."""
    idx = _make_edge_index_tree()
    idx.add_relation(RelationEdge("A", "C", "depends_on", "2025-01-01T00:00:00Z"))
    idx.add_relation(RelationEdge("B", "A", "depends_on", "2025-01-01T00:00:00Z"))
    return idx


# ---------------------------------------------------------------------------
# Engine
# ---------------------------------------------------------------------------

class TestInferenceResult:
    def test_create(self):
        r = InferenceResult(
            inference_type=InferenceType.CLUSTER,
            entity_ids=["a", "b"],
            score=0.75,
            description="test cluster",
        )
        assert r.inference_type == InferenceType.CLUSTER
        assert r.score == 0.75

    def test_to_dict(self):
        r = InferenceResult(
            inference_type=InferenceType.INSTABILITY,
            entity_ids=["x"],
            score=0.5,
        )
        d = r.to_dict()
        assert d["inference_type"] == "instability"
        assert d["entity_ids"] == ["x"]


# ---------------------------------------------------------------------------
# Clusters
# ---------------------------------------------------------------------------

class TestClusterDetection:
    def test_no_relations(self):
        idx = EdgeIndex()
        clusters = detect_clusters_from_relations(idx)
        assert clusters == []

    def test_single_cluster(self):
        idx = EdgeIndex()
        idx.add_relation(RelationEdge("A", "B", "depends_on", "2025-01-01T00:00:00Z"))
        idx.add_relation(RelationEdge("B", "C", "depends_on", "2025-01-01T00:00:00Z"))
        clusters = detect_clusters_from_relations(idx)
        assert len(clusters) == 1
        assert set(clusters[0].entity_ids) == {"A", "B", "C"}

    def test_two_clusters(self):
        idx = EdgeIndex()
        idx.add_relation(RelationEdge("A", "B", "depends_on", "2025-01-01T00:00:00Z"))
        idx.add_relation(RelationEdge("C", "D", "depends_on", "2025-01-01T00:00:00Z"))
        clusters = detect_clusters_from_relations(idx)
        assert len(clusters) == 2

    def test_min_cluster_size(self):
        idx = EdgeIndex()
        idx.add_relation(RelationEdge("A", "B", "depends_on", "2025-01-01T00:00:00Z"))
        assert detect_clusters_from_relations(idx, min_cluster_size=3) == []
        assert len(detect_clusters_from_relations(idx, min_cluster_size=2)) == 1

    def test_filter_by_relation_type(self):
        idx = EdgeIndex()
        idx.add_relation(RelationEdge("A", "B", "depends_on", "2025-01-01T00:00:00Z"))
        idx.add_relation(RelationEdge("C", "D", "produces_for", "2025-01-01T00:00:00Z"))
        clusters = detect_clusters_from_relations(idx, relation_type="depends_on")
        assert len(clusters) == 1
        assert set(clusters[0].entity_ids) == {"A", "B"}

    def test_cohesion_calculation(self):
        """Triangle graph should have cohesion 1.0."""
        idx = EdgeIndex()
        idx.add_relation(RelationEdge("A", "B", "depends_on", "2025-01-01T00:00:00Z"))
        idx.add_relation(RelationEdge("B", "C", "depends_on", "2025-01-01T00:00:00Z"))
        idx.add_relation(RelationEdge("A", "C", "depends_on", "2025-01-01T00:00:00Z"))
        clusters = detect_clusters_from_relations(idx)
        assert len(clusters) == 1
        assert clusters[0].cohesion == 1.0

    def test_clusters_to_inferences(self):
        clusters = [Cluster(entity_ids=["A", "B"], cohesion=0.8)]
        results = clusters_to_inferences(clusters)
        assert len(results) == 1
        assert results[0].inference_type == InferenceType.CLUSTER
        assert results[0].score == 0.8


# ---------------------------------------------------------------------------
# Propagation
# ---------------------------------------------------------------------------

class TestPropagateUpward:
    def test_upward_from_leaf(self):
        idx = _make_edge_index_tree()
        paths = propagate_upward(idx, "B")
        # B → A → root
        assert len(paths) == 2
        assert paths[0].target_id == "A"
        assert paths[1].target_id == "root"

    def test_upward_from_root(self):
        idx = _make_edge_index_tree()
        assert propagate_upward(idx, "root") == []

    def test_upward_max_depth(self):
        idx = _make_edge_index_tree()
        paths = propagate_upward(idx, "B", max_depth=1)
        assert len(paths) == 1
        assert paths[0].target_id == "A"


class TestPropagateDownward:
    def test_downward_from_root(self):
        idx = _make_edge_index_tree()
        paths = propagate_downward(idx, "root")
        targets = {p.target_id for p in paths}
        assert targets == {"A", "B", "C"}

    def test_downward_from_leaf(self):
        idx = _make_edge_index_tree()
        assert propagate_downward(idx, "B") == []

    def test_downward_max_depth(self):
        idx = _make_edge_index_tree()
        paths = propagate_downward(idx, "root", max_depth=1)
        targets = {p.target_id for p in paths}
        assert targets == {"A", "C"}  # only direct children


class TestPropagateLateral:
    def test_lateral_through_relations(self):
        idx = _make_edge_index_with_relations()
        # A has incoming from B (depends_on B→A)
        paths = propagate_lateral(idx, "A")
        assert any(p.target_id == "B" for p in paths)

    def test_lateral_no_relations(self):
        idx = _make_edge_index_tree()  # no relations
        assert propagate_lateral(idx, "A") == []

    def test_lateral_with_type_filter(self):
        idx = _make_edge_index_with_relations()
        paths = propagate_lateral(idx, "C", relation_type="depends_on")
        assert any(p.target_id == "A" for p in paths)


class TestFullBlastRadius:
    def test_blast_radius_combines_all(self):
        idx = _make_edge_index_with_relations()
        paths = full_blast_radius(idx, "A")
        # Upward: root. Downward: B. Lateral: from B's depends_on.
        assert len(paths) >= 2
        directions = {p.direction for p in paths}
        assert "upward" in directions
        assert "downward" in directions

    def test_path_tracking(self):
        idx = _make_edge_index_tree()
        paths = propagate_downward(idx, "root")
        for p in paths:
            assert p.path[0] == "root"
            assert p.path[-1] == p.target_id


# ---------------------------------------------------------------------------
# Tension
# ---------------------------------------------------------------------------

class TestDetectOrphans:
    def test_orphan_detected(self):
        entities = {
            "e1": EntityIdentity(uid="e1", entity_type=EntityType.REPO),
            "e2": EntityIdentity(uid="e2", entity_type=EntityType.REPO),
        }
        idx = EdgeIndex()
        idx.add_hierarchy(HierarchyEdge("root", "e1", "2025-01-01T00:00:00Z"))
        # e2 has no edges at all
        tensions = detect_orphans(entities, idx)
        assert len(tensions) == 1
        assert tensions[0].entity_ids == ["e2"]
        assert tensions[0].tension_type == TensionType.ORPHAN

    def test_no_orphans(self):
        entities = {
            "e1": EntityIdentity(uid="e1", entity_type=EntityType.REPO),
        }
        idx = EdgeIndex()
        idx.add_hierarchy(HierarchyEdge("root", "e1", "2025-01-01T00:00:00Z"))
        assert detect_orphans(entities, idx) == []

    def test_skip_inactive_entities(self):
        entities = {
            "e1": EntityIdentity(
                uid="e1", entity_type=EntityType.REPO,
                lifecycle_status=LifecycleStatus.ARCHIVED,
            ),
        }
        idx = EdgeIndex()
        assert detect_orphans(entities, idx) == []


class TestDetectNamingConflicts:
    def test_conflict_detected(self):
        idx = NameIndex()
        add_name(idx, "e1", "my-project", is_primary=True)
        add_name(idx, "e2", "my-project", is_primary=True)
        tensions = detect_naming_conflicts(idx)
        assert len(tensions) == 1
        assert set(tensions[0].entity_ids) == {"e1", "e2"}
        assert tensions[0].tension_type == TensionType.NAMING_CONFLICT

    def test_no_conflict(self):
        idx = NameIndex()
        add_name(idx, "e1", "project-a", is_primary=True)
        add_name(idx, "e2", "project-b", is_primary=True)
        assert detect_naming_conflicts(idx) == []


class TestDetectOvercoupling:
    def test_overcoupled_detected(self):
        idx = EdgeIndex()
        # Entity "hub" has 6 incoming relations
        for i in range(6):
            idx.add_relation(
                RelationEdge(f"e{i}", "hub", "depends_on", "2025-01-01T00:00:00Z"),
            )
        tensions = detect_overcoupling(idx, threshold=5)
        assert len(tensions) == 1
        assert tensions[0].entity_ids == ["hub"]
        assert tensions[0].tension_type == TensionType.OVERCOUPLED

    def test_below_threshold(self):
        idx = EdgeIndex()
        for i in range(3):
            idx.add_relation(
                RelationEdge(f"e{i}", "hub", "depends_on", "2025-01-01T00:00:00Z"),
            )
        assert detect_overcoupling(idx, threshold=5) == []

    def test_severity_scales(self):
        idx = EdgeIndex()
        for i in range(10):
            idx.add_relation(
                RelationEdge(f"e{i}", "hub", "depends_on", "2025-01-01T00:00:00Z"),
            )
        tensions = detect_overcoupling(idx, threshold=5)
        assert tensions[0].severity == 1.0  # 10 / (5*2) = 1.0
