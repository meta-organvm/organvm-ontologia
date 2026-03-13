"""Tests for hierarchy and relation edges."""

from ontologia.structure.edges import (
    EdgeIndex,
    HierarchyEdge,
    RelationEdge,
    RelationType,
)


class TestHierarchyEdge:
    def test_active_current(self):
        edge = HierarchyEdge(parent_id="p", child_id="c", valid_from="2026-01-01T00:00:00+00:00")
        assert edge.is_active()

    def test_active_retired(self):
        edge = HierarchyEdge(
            parent_id="p", child_id="c",
            valid_from="2026-01-01T00:00:00+00:00",
            valid_to="2026-02-01T00:00:00+00:00",
        )
        assert not edge.is_active()

    def test_active_at_timestamp(self):
        edge = HierarchyEdge(
            parent_id="p", child_id="c",
            valid_from="2026-01-01T00:00:00+00:00",
            valid_to="2026-06-01T00:00:00+00:00",
        )
        assert edge.is_active(at="2026-03-15T00:00:00+00:00")
        assert not edge.is_active(at="2026-07-01T00:00:00+00:00")

    def test_roundtrip(self):
        edge = HierarchyEdge(
            parent_id="p1", child_id="c1",
            valid_from="2026-01-01T00:00:00+00:00",
            metadata={"note": "test"},
        )
        d = edge.to_dict()
        restored = HierarchyEdge.from_dict(d)
        assert restored.parent_id == "p1"
        assert restored.child_id == "c1"
        assert restored.metadata == {"note": "test"}

    def test_to_dict_excludes_none(self):
        edge = HierarchyEdge(parent_id="p", child_id="c", valid_from="2026-01-01T00:00:00+00:00")
        d = edge.to_dict()
        assert "valid_to" not in d
        assert "metadata" not in d


class TestRelationEdge:
    def test_active(self):
        edge = RelationEdge(
            source_id="s", target_id="t",
            relation_type=RelationType.DEPENDS_ON,
            valid_from="2026-01-01T00:00:00+00:00",
        )
        assert edge.is_active()

    def test_roundtrip(self):
        edge = RelationEdge(
            source_id="s1", target_id="t1",
            relation_type="depends_on",
            valid_from="2026-01-01T00:00:00+00:00",
        )
        d = edge.to_dict()
        restored = RelationEdge.from_dict(d)
        assert restored.source_id == "s1"
        assert restored.relation_type == "depends_on"


class TestEdgeIndex:
    def _make_index(self) -> EdgeIndex:
        idx = EdgeIndex()
        # organ → repo hierarchy
        idx.add_hierarchy(HierarchyEdge(
            parent_id="organ_meta", child_id="repo_engine",
            valid_from="2026-01-01T00:00:00+00:00",
        ))
        idx.add_hierarchy(HierarchyEdge(
            parent_id="organ_meta", child_id="repo_dashboard",
            valid_from="2026-01-01T00:00:00+00:00",
        ))
        # relation
        idx.add_relation(RelationEdge(
            source_id="repo_dashboard", target_id="repo_engine",
            relation_type="depends_on",
            valid_from="2026-01-01T00:00:00+00:00",
        ))
        return idx

    def test_children(self):
        idx = self._make_index()
        kids = idx.children("organ_meta")
        assert len(kids) == 2
        child_ids = {e.child_id for e in kids}
        assert "repo_engine" in child_ids
        assert "repo_dashboard" in child_ids

    def test_children_empty(self):
        idx = self._make_index()
        assert idx.children("repo_engine") == []

    def test_parent(self):
        idx = self._make_index()
        parent = idx.parent("repo_engine")
        assert parent is not None
        assert parent.parent_id == "organ_meta"

    def test_parent_none(self):
        idx = self._make_index()
        assert idx.parent("organ_meta") is None

    def test_outgoing_relations(self):
        idx = self._make_index()
        rels = idx.outgoing_relations("repo_dashboard")
        assert len(rels) == 1
        assert rels[0].target_id == "repo_engine"

    def test_outgoing_relations_filtered(self):
        idx = self._make_index()
        rels = idx.outgoing_relations("repo_dashboard", relation_type="depends_on")
        assert len(rels) == 1
        rels = idx.outgoing_relations("repo_dashboard", relation_type="produces_for")
        assert len(rels) == 0

    def test_incoming_relations(self):
        idx = self._make_index()
        rels = idx.incoming_relations("repo_engine")
        assert len(rels) == 1
        assert rels[0].source_id == "repo_dashboard"

    def test_retire_hierarchy(self):
        idx = self._make_index()
        assert idx.retire_hierarchy("organ_meta", "repo_engine")
        assert len(idx.children("organ_meta")) == 1  # only dashboard left

    def test_retire_hierarchy_not_found(self):
        idx = self._make_index()
        assert not idx.retire_hierarchy("organ_meta", "nonexistent")

    def test_retire_relation(self):
        idx = self._make_index()
        assert idx.retire_relation("repo_dashboard", "repo_engine", "depends_on")
        assert len(idx.outgoing_relations("repo_dashboard")) == 0

    def test_retire_relation_not_found(self):
        idx = self._make_index()
        assert not idx.retire_relation("repo_dashboard", "repo_engine", "produces_for")

    def test_temporal_children(self):
        idx = EdgeIndex()
        # Old edge (retired)
        idx.add_hierarchy(HierarchyEdge(
            parent_id="p", child_id="c1",
            valid_from="2026-01-01T00:00:00+00:00",
            valid_to="2026-03-01T00:00:00+00:00",
        ))
        # New edge
        idx.add_hierarchy(HierarchyEdge(
            parent_id="p", child_id="c2",
            valid_from="2026-03-01T00:00:00+00:00",
        ))
        # At Feb: only c1
        feb = idx.children("p", at="2026-02-01T00:00:00+00:00")
        assert len(feb) == 1
        assert feb[0].child_id == "c1"
        # At April: only c2
        apr = idx.children("p", at="2026-04-01T00:00:00+00:00")
        assert len(apr) == 1
        assert apr[0].child_id == "c2"

    def test_roundtrip(self):
        idx = self._make_index()
        d = idx.to_dict()
        restored = EdgeIndex.from_dict(d)
        assert len(restored.all_hierarchy_edges()) == 2
        assert len(restored.all_relation_edges()) == 1
