"""Tests for metrics, observations, and rollups."""

from pathlib import Path

from ontologia.metrics.metric import (
    AggregationPolicy,
    MetricDefinition,
    MetricType,
    create_metric,
)
from ontologia.metrics.observations import Observation, ObservationStore
from ontologia.metrics.rollups import RollupResult, rollup_for_entity, rollup_tree
from ontologia.structure.edges import EdgeIndex, HierarchyEdge


# ── Metric definitions ──────────────────────────────────────────────

class TestMetricDefinition:
    def test_create_metric(self):
        m = create_metric("repo_count", MetricType.GAUGE, unit="repos")
        assert m.metric_id.startswith("met_")
        assert m.name == "repo_count"
        assert m.metric_type == MetricType.GAUGE

    def test_roundtrip(self):
        m = create_metric("test_metric", MetricType.COUNTER, description="A counter")
        d = m.to_dict()
        restored = MetricDefinition.from_dict(d)
        assert restored.metric_id == m.metric_id
        assert restored.metric_type == MetricType.COUNTER

    def test_default_aggregation(self):
        m = create_metric("x")
        assert m.aggregation == AggregationPolicy.SUM


# ── Observations ────────────────────────────────────────────────────

class TestObservation:
    def test_roundtrip(self):
        obs = Observation(
            metric_id="met_1", entity_id="ent_repo_A",
            value=42.0, source="test",
        )
        d = obs.to_dict()
        restored = Observation.from_dict(d)
        assert restored.value == 42.0
        assert restored.metric_id == "met_1"

    def test_jsonl_no_newlines(self):
        obs = Observation(metric_id="m", entity_id="e", value=1.0)
        assert "\n" not in obs.to_jsonl()


class TestObservationStore:
    def test_record_and_count(self, tmp_path: Path):
        store = ObservationStore(tmp_path / "obs.jsonl")
        store.observe("met_1", "ent_A", 10.0)
        store.observe("met_1", "ent_A", 20.0)
        assert store.count == 2

    def test_persistence(self, tmp_path: Path):
        path = tmp_path / "obs.jsonl"
        s1 = ObservationStore(path)
        s1.observe("met_1", "ent_A", 10.0)
        s1.observe("met_1", "ent_A", 20.0)

        s2 = ObservationStore(path)
        s2.load()
        assert s2.count == 2

    def test_latest(self, tmp_path: Path):
        store = ObservationStore(tmp_path / "obs.jsonl")
        store.observe("met_1", "ent_A", 10.0)
        store.observe("met_1", "ent_A", 20.0)
        store.observe("met_1", "ent_B", 30.0)
        latest = store.latest("met_1", "ent_A")
        assert latest is not None
        assert latest.value == 20.0

    def test_latest_not_found(self, tmp_path: Path):
        store = ObservationStore(tmp_path / "obs.jsonl")
        assert store.latest("met_1", "ent_X") is None

    def test_query_by_metric(self, tmp_path: Path):
        store = ObservationStore(tmp_path / "obs.jsonl")
        store.observe("met_1", "ent_A", 10.0)
        store.observe("met_2", "ent_A", 20.0)
        results = store.query(metric_id="met_1")
        assert len(results) == 1

    def test_query_by_entity(self, tmp_path: Path):
        store = ObservationStore(tmp_path / "obs.jsonl")
        store.observe("met_1", "ent_A", 10.0)
        store.observe("met_1", "ent_B", 20.0)
        results = store.query(entity_id="ent_A")
        assert len(results) == 1

    def test_query_with_limit(self, tmp_path: Path):
        store = ObservationStore(tmp_path / "obs.jsonl")
        for i in range(10):
            store.observe("met_1", "ent_A", float(i))
        results = store.query(limit=3)
        assert len(results) == 3
        assert results[-1].value == 9.0  # last 3

    def test_time_series(self, tmp_path: Path):
        store = ObservationStore(tmp_path / "obs.jsonl")
        store.record(Observation(
            metric_id="met_1", entity_id="ent_A", value=10.0,
            timestamp="2026-01-01T00:00:00+00:00",
        ))
        store.record(Observation(
            metric_id="met_1", entity_id="ent_A", value=20.0,
            timestamp="2026-02-01T00:00:00+00:00",
        ))
        ts = store.time_series("met_1", "ent_A")
        assert len(ts) == 2
        assert ts[0] == ("2026-01-01T00:00:00+00:00", 10.0)

    def test_empty_load(self, tmp_path: Path):
        store = ObservationStore(tmp_path / "nonexistent.jsonl")
        store.load()
        assert store.count == 0


# ── Rollups ─────────────────────────────────────────────────────────

def _make_hierarchy_and_obs(tmp_path: Path) -> tuple[EdgeIndex, ObservationStore, str]:
    """Build: root → [a, b], with observations at a=10, b=20."""
    idx = EdgeIndex()
    idx.add_hierarchy(HierarchyEdge(
        parent_id="root", child_id="a", valid_from="2026-01-01T00:00:00+00:00",
    ))
    idx.add_hierarchy(HierarchyEdge(
        parent_id="root", child_id="b", valid_from="2026-01-01T00:00:00+00:00",
    ))

    obs = ObservationStore(tmp_path / "obs.jsonl")
    met_id = "met_test"
    obs.observe(met_id, "a", 10.0)
    obs.observe(met_id, "b", 20.0)
    return idx, obs, met_id


class TestRollupForEntity:
    def test_sum(self, tmp_path: Path):
        idx, obs, met_id = _make_hierarchy_and_obs(tmp_path)
        metric = MetricDefinition(
            metric_id=met_id, name="test",
            metric_type=MetricType.GAUGE,
            aggregation=AggregationPolicy.SUM,
        )
        result = rollup_for_entity("root", metric, idx, obs)
        assert result.value == 30.0
        assert result.child_count == 2

    def test_avg(self, tmp_path: Path):
        idx, obs, met_id = _make_hierarchy_and_obs(tmp_path)
        metric = MetricDefinition(
            metric_id=met_id, name="test",
            metric_type=MetricType.GAUGE,
            aggregation=AggregationPolicy.AVG,
        )
        result = rollup_for_entity("root", metric, idx, obs)
        assert result.value == 15.0

    def test_max(self, tmp_path: Path):
        idx, obs, met_id = _make_hierarchy_and_obs(tmp_path)
        metric = MetricDefinition(
            metric_id=met_id, name="test",
            metric_type=MetricType.GAUGE,
            aggregation=AggregationPolicy.MAX,
        )
        result = rollup_for_entity("root", metric, idx, obs)
        assert result.value == 20.0

    def test_min(self, tmp_path: Path):
        idx, obs, met_id = _make_hierarchy_and_obs(tmp_path)
        metric = MetricDefinition(
            metric_id=met_id, name="test",
            metric_type=MetricType.GAUGE,
            aggregation=AggregationPolicy.MIN,
        )
        result = rollup_for_entity("root", metric, idx, obs)
        assert result.value == 10.0

    def test_count(self, tmp_path: Path):
        idx, obs, met_id = _make_hierarchy_and_obs(tmp_path)
        metric = MetricDefinition(
            metric_id=met_id, name="test",
            metric_type=MetricType.GAUGE,
            aggregation=AggregationPolicy.COUNT,
        )
        result = rollup_for_entity("root", metric, idx, obs)
        assert result.value == 2.0

    def test_no_children(self, tmp_path: Path):
        idx = EdgeIndex()
        obs = ObservationStore(tmp_path / "obs.jsonl")
        metric = MetricDefinition(
            metric_id="met_x", name="test",
            metric_type=MetricType.GAUGE,
        )
        result = rollup_for_entity("lonely", metric, idx, obs)
        assert result.value == 0.0
        assert result.child_count == 0


class TestRollupTree:
    def test_two_level_tree(self, tmp_path: Path):
        # root → [organ_a, organ_b], organ_a → [repo_1, repo_2]
        idx = EdgeIndex()
        idx.add_hierarchy(HierarchyEdge(
            parent_id="root", child_id="organ_a", valid_from="2026-01-01T00:00:00+00:00",
        ))
        idx.add_hierarchy(HierarchyEdge(
            parent_id="root", child_id="organ_b", valid_from="2026-01-01T00:00:00+00:00",
        ))
        idx.add_hierarchy(HierarchyEdge(
            parent_id="organ_a", child_id="repo_1", valid_from="2026-01-01T00:00:00+00:00",
        ))
        idx.add_hierarchy(HierarchyEdge(
            parent_id="organ_a", child_id="repo_2", valid_from="2026-01-01T00:00:00+00:00",
        ))

        obs = ObservationStore(tmp_path / "obs.jsonl")
        met_id = "met_count"
        obs.observe(met_id, "repo_1", 5.0)
        obs.observe(met_id, "repo_2", 3.0)
        obs.observe(met_id, "organ_b", 10.0)

        metric = MetricDefinition(
            metric_id=met_id, name="count",
            metric_type=MetricType.GAUGE,
            aggregation=AggregationPolicy.SUM,
        )
        results = rollup_tree("root", metric, idx, obs)

        assert "organ_a" in results
        assert results["organ_a"].value == 8.0  # 5 + 3
        assert "root" in results
        assert results["root"].value == 18.0  # 8 + 10

    def test_leaf_only(self, tmp_path: Path):
        idx = EdgeIndex()
        obs = ObservationStore(tmp_path / "obs.jsonl")
        met_id = "met_x"
        obs.observe(met_id, "leaf", 42.0)
        metric = MetricDefinition(
            metric_id=met_id, name="x",
            metric_type=MetricType.GAUGE,
        )
        results = rollup_tree("leaf", metric, idx, obs)
        # Leaf has no children, so no rollup entry (returns own value upstream)
        assert len(results) == 0
