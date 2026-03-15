"""Tests for unified RegistryStore integration — variables, lineage, metrics, mutations.

Verifies that the store persists and loads all five data types:
entities, names, edges, variables, lineage, metrics, and observations.
Also tests the orchestrated mutation operations (merge, split, relocate, reclassify).
"""

from __future__ import annotations

from ontologia.entity.identity import EntityType, LifecycleStatus
from ontologia.entity.lineage import LineageType
from ontologia.metrics.metric import AggregationPolicy, MetricDefinition, MetricType
from ontologia.registry.mutations import (
    MutationResult,
    merge_entities,
    reclassify_entity,
    relocate_entity,
    split_entity,
)
from ontologia.registry.store import RegistryStore, open_store
from ontologia.variables.variable import Constraint, Mutability, Scope, Variable, VariableType


# ---------------------------------------------------------------------------
# Variable persistence
# ---------------------------------------------------------------------------


class TestVariablePersistence:
    def test_set_and_resolve(self, store: RegistryStore):
        var = Variable(
            key="DEFAULT_LANGUAGE",
            value="en",
            var_type=VariableType.STRING,
            mutability=Mutability.RUNTIME,
            scope=Scope.GLOBAL,
        )
        ok, msg = store.set_variable(var)
        assert ok
        assert store.resolve_variable("DEFAULT_LANGUAGE") == "en"

    def test_inheritance_cascade(self, store: RegistryStore):
        """Repo-level override wins over global default."""
        organ = store.create_entity(EntityType.ORGAN, "Theoria")
        store.set_variable(Variable(
            key="LANGUAGE", value="en", scope=Scope.GLOBAL,
        ))
        store.set_variable(Variable(
            key="LANGUAGE", value="la", scope=Scope.ORGAN, entity_id=organ.uid,
        ))
        # Resolve at organ scope → should get "la"
        result = store.variable_store.resolve(
            "LANGUAGE", scope=Scope.ORGAN, entity_chain=[organ.uid],
        )
        assert result.value == "la"
        assert result.resolved_from == "direct"

    def test_round_trip(self, store_dir):
        """Variables survive save/reload cycle."""
        s1 = RegistryStore(store_dir=store_dir)
        s1.load()
        s1.set_variable(Variable(key="TEST_KEY", value=42, var_type=VariableType.INTEGER))
        s1.save()

        s2 = open_store(store_dir)
        assert s2.resolve_variable("TEST_KEY") == 42

    def test_constant_immutability(self, store: RegistryStore):
        store.set_variable(Variable(
            key="SYSTEM_UID", value="sys_001", mutability=Mutability.CONSTANT,
        ))
        ok, msg = store.set_variable(Variable(
            key="SYSTEM_UID", value="sys_002", mutability=Mutability.CONSTANT,
        ))
        assert not ok
        assert "constant" in msg.lower()

    def test_constraint_validation(self, store: RegistryStore):
        var = Variable(
            key="TIER",
            value="diamond",
            constraint=Constraint(allowed_values=["flagship", "standard", "infrastructure"]),
        )
        ok, msg = store.set_variable(var)
        assert not ok
        assert "allowed" in msg.lower()


# ---------------------------------------------------------------------------
# Lineage persistence
# ---------------------------------------------------------------------------


class TestLineagePersistence:
    def test_add_and_query(self, store: RegistryStore):
        a = store.create_entity(EntityType.REPO, "repo-a")
        b = store.create_entity(EntityType.REPO, "repo-b")
        store.add_lineage(b.uid, a.uid, LineageType.DERIVED_FROM)

        preds = store.lineage_index.predecessors(b.uid)
        assert len(preds) == 1
        assert preds[0].related_id == a.uid

    def test_round_trip(self, store_dir):
        s1 = RegistryStore(store_dir=store_dir)
        s1.load()
        a = s1.create_entity(EntityType.REPO, "alpha")
        b = s1.create_entity(EntityType.REPO, "beta")
        s1.add_lineage(b.uid, a.uid, LineageType.SUPERSEDES)
        s1.save()

        s2 = open_store(store_dir)
        succs = s2.lineage_index.successors(a.uid)
        assert len(succs) == 1
        assert succs[0].entity_id == b.uid


# ---------------------------------------------------------------------------
# Metric + Observation persistence
# ---------------------------------------------------------------------------


class TestMetricObservationPersistence:
    def test_register_and_observe(self, store: RegistryStore):
        met = MetricDefinition(
            metric_id="met_test_count",
            name="Test Count",
            metric_type=MetricType.GAUGE,
            unit="count",
            aggregation=AggregationPolicy.SUM,
        )
        store.register_metric(met)
        assert store.get_metric("met_test_count") is not None

        entity = store.create_entity(EntityType.REPO, "test-repo")
        obs = store.record_observation("met_test_count", entity.uid, 42.0)
        assert obs.value == 42.0

        latest = store.observation_store.latest("met_test_count", entity.uid)
        assert latest is not None
        assert latest.value == 42.0

    def test_metric_round_trip(self, store_dir):
        s1 = RegistryStore(store_dir=store_dir)
        s1.load()
        met = MetricDefinition(
            metric_id="met_lines",
            name="Line Count",
            metric_type=MetricType.COUNTER,
            aggregation=AggregationPolicy.SUM,
        )
        s1.register_metric(met)
        s1.save()

        s2 = open_store(store_dir)
        loaded = s2.get_metric("met_lines")
        assert loaded is not None
        assert loaded.metric_type == MetricType.COUNTER

    def test_observation_round_trip(self, store_dir):
        s1 = RegistryStore(store_dir=store_dir)
        s1.load()
        e = s1.create_entity(EntityType.REPO, "r")
        s1.record_observation("met_x", e.uid, 99.0)
        s1.save()

        s2 = open_store(store_dir)
        latest = s2.observation_store.latest("met_x", e.uid)
        assert latest is not None
        assert latest.value == 99.0

    def test_time_series(self, store: RegistryStore):
        entity = store.create_entity(EntityType.REPO, "ts-repo")
        for i in range(5):
            store.record_observation("met_series", entity.uid, float(i * 10))
        series = store.observation_store.time_series("met_series", entity.uid)
        assert len(series) == 5
        assert series[-1][1] == 40.0


# ---------------------------------------------------------------------------
# Mutation: relocate
# ---------------------------------------------------------------------------


class TestRelocate:
    def test_relocate_entity(self, store: RegistryStore):
        organ_a = store.create_entity(EntityType.ORGAN, "Organ-A")
        organ_b = store.create_entity(EntityType.ORGAN, "Organ-B")
        repo = store.create_entity(EntityType.REPO, "my-repo")
        store.add_hierarchy_edge(organ_a.uid, repo.uid)

        result = relocate_entity(store, repo.uid, organ_b.uid)
        assert result.success
        assert result.edges_closed == 1
        assert result.edges_created == 1

        # New parent should be organ_b
        active_parents = [
            e for e in store.edge_index.all_hierarchy_edges()
            if e.child_id == repo.uid and e.is_active()
        ]
        assert len(active_parents) == 1
        assert active_parents[0].parent_id == organ_b.uid

    def test_relocate_missing_entity(self, store: RegistryStore):
        organ = store.create_entity(EntityType.ORGAN, "Org")
        result = relocate_entity(store, "nonexistent", organ.uid)
        assert not result.success


# ---------------------------------------------------------------------------
# Mutation: reclassify
# ---------------------------------------------------------------------------


class TestReclassify:
    def test_reclassify_entity(self, store: RegistryStore):
        entity = store.create_entity(EntityType.REPO, "some-entity")
        assert entity.entity_type == EntityType.REPO

        result = reclassify_entity(store, entity.uid, EntityType.MODULE)
        assert result.success

        refreshed = store.get_entity(entity.uid)
        assert refreshed.entity_type == EntityType.MODULE

    def test_reclassify_missing(self, store: RegistryStore):
        result = reclassify_entity(store, "nonexistent", EntityType.DOCUMENT)
        assert not result.success


# ---------------------------------------------------------------------------
# Mutation: merge
# ---------------------------------------------------------------------------


class TestMerge:
    def test_merge_two_entities(self, store: RegistryStore):
        a = store.create_entity(EntityType.REPO, "repo-alpha")
        b = store.create_entity(EntityType.REPO, "repo-beta")
        child = store.create_entity(EntityType.MODULE, "child-mod")
        store.add_hierarchy_edge(a.uid, child.uid)

        result = merge_entities(store, [a.uid, b.uid], "repo-combined")
        assert result.success
        assert len(result.entities_created) == 1
        successor_uid = result.entities_created[0]

        # Sources should be deprecated
        assert store.get_entity(a.uid).lifecycle_status == LifecycleStatus.DEPRECATED
        assert store.get_entity(b.uid).lifecycle_status == LifecycleStatus.DEPRECATED

        # Lineage should record both sources
        preds = store.lineage_index.full_lineage(a.uid)
        merged_records = [r for r in preds if r.lineage_type == LineageType.MERGED_INTO]
        assert len(merged_records) == 1
        assert merged_records[0].related_id == successor_uid

        # Child should be transferred to successor
        active_parents = [
            e for e in store.edge_index.all_hierarchy_edges()
            if e.child_id == child.uid and e.is_active()
        ]
        parent_ids = {e.parent_id for e in active_parents}
        assert successor_uid in parent_ids

    def test_merge_missing_source(self, store: RegistryStore):
        a = store.create_entity(EntityType.REPO, "exists")
        result = merge_entities(store, [a.uid, "nonexistent"], "merged")
        assert not result.success


# ---------------------------------------------------------------------------
# Mutation: split
# ---------------------------------------------------------------------------


class TestSplit:
    def test_split_into_two(self, store: RegistryStore):
        parent = store.create_entity(EntityType.ORGAN, "parent-organ")
        src = store.create_entity(EntityType.REPO, "big-repo")
        store.add_hierarchy_edge(parent.uid, src.uid)

        result = split_entity(store, src.uid, [
            {"name": "big-repo-core"},
            {"name": "big-repo-utils"},
        ])
        assert result.success
        assert len(result.entities_created) == 2
        assert result.lineage_records == 2

        # Source should be deprecated
        assert store.get_entity(src.uid).lifecycle_status == LifecycleStatus.DEPRECATED

        # Each descendant should have lineage back to source
        for d_uid in result.entities_created:
            preds = store.lineage_index.predecessors(d_uid)
            assert len(preds) == 1
            assert preds[0].related_id == src.uid
            assert preds[0].lineage_type == LineageType.SPLIT_FROM

        # Descendants should inherit parent edges
        for d_uid in result.entities_created:
            parents = [
                e for e in store.edge_index.all_hierarchy_edges()
                if e.child_id == d_uid and e.is_active()
            ]
            assert len(parents) == 1
            assert parents[0].parent_id == parent.uid

    def test_split_no_descendants(self, store: RegistryStore):
        src = store.create_entity(EntityType.REPO, "repo")
        result = split_entity(store, src.uid, [])
        assert not result.success

    def test_split_missing_source(self, store: RegistryStore):
        result = split_entity(store, "nonexistent", [{"name": "x"}])
        assert not result.success

    def test_split_without_deprecation(self, store: RegistryStore):
        src = store.create_entity(EntityType.REPO, "repo")
        result = split_entity(store, src.uid, [{"name": "a"}], deprecate_source=False)
        assert result.success
        assert store.get_entity(src.uid).lifecycle_status == LifecycleStatus.ACTIVE
