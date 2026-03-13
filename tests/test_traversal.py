"""Tests for graph traversal functions."""

from ontologia.structure.edges import EdgeIndex, HierarchyEdge, RelationEdge
from ontologia.structure.traversal import (
    dependency_trace,
    find_leaves,
    find_roots,
    macro_descent,
    micro_ascent,
    reverse_dependency_trace,
    subtree_size,
)


def _make_tree() -> EdgeIndex:
    """Build a tree: root → [a, b], a → [c, d], b → [e]"""
    idx = EdgeIndex()
    for parent, child in [("root", "a"), ("root", "b"), ("a", "c"), ("a", "d"), ("b", "e")]:
        idx.add_hierarchy(HierarchyEdge(
            parent_id=parent, child_id=child,
            valid_from="2026-01-01T00:00:00+00:00",
        ))
    return idx


def _make_dep_graph() -> EdgeIndex:
    """Build: a → b → c, a → c (diamond dependency)."""
    idx = EdgeIndex()
    for src, tgt in [("a", "b"), ("b", "c"), ("a", "c")]:
        idx.add_relation(RelationEdge(
            source_id=src, target_id=tgt,
            relation_type="depends_on",
            valid_from="2026-01-01T00:00:00+00:00",
        ))
    return idx


class TestMacroDescent:
    def test_full_tree(self):
        idx = _make_tree()
        desc = macro_descent(idx, "root")
        assert set(desc) == {"a", "b", "c", "d", "e"}

    def test_subtree(self):
        idx = _make_tree()
        desc = macro_descent(idx, "a")
        assert set(desc) == {"c", "d"}

    def test_leaf(self):
        idx = _make_tree()
        assert macro_descent(idx, "c") == []

    def test_max_depth(self):
        idx = _make_tree()
        # depth=1: only direct children of root
        desc = macro_descent(idx, "root", max_depth=1)
        assert set(desc) == {"a", "b"}

    def test_nonexistent_root(self):
        idx = _make_tree()
        assert macro_descent(idx, "nonexistent") == []


class TestMicroAscent:
    def test_leaf_to_root(self):
        idx = _make_tree()
        ancestors = micro_ascent(idx, "c")
        assert ancestors == ["a", "root"]

    def test_root_has_no_ancestors(self):
        idx = _make_tree()
        assert micro_ascent(idx, "root") == []

    def test_depth_one(self):
        idx = _make_tree()
        ancestors = micro_ascent(idx, "a")
        assert ancestors == ["root"]


class TestDependencyTrace:
    def test_transitive(self):
        idx = _make_dep_graph()
        deps = dependency_trace(idx, "a")
        assert set(deps) == {"b", "c"}

    def test_from_leaf(self):
        idx = _make_dep_graph()
        assert dependency_trace(idx, "c") == []

    def test_max_depth(self):
        idx = _make_dep_graph()
        deps = dependency_trace(idx, "a", max_depth=1)
        assert set(deps) == {"b", "c"}  # both direct from a

    def test_with_type_filter(self):
        idx = _make_dep_graph()
        assert dependency_trace(idx, "a", relation_type="depends_on") == ["b", "c"]
        assert dependency_trace(idx, "a", relation_type="produces_for") == []


class TestReverseDependencyTrace:
    def test_reverse(self):
        idx = _make_dep_graph()
        dependents = reverse_dependency_trace(idx, "c")
        assert set(dependents) == {"a", "b"}

    def test_no_dependents(self):
        idx = _make_dep_graph()
        assert reverse_dependency_trace(idx, "a") == []


class TestFindRootsAndLeaves:
    def test_roots(self):
        idx = _make_tree()
        assert find_roots(idx) == ["root"]

    def test_leaves(self):
        idx = _make_tree()
        assert find_leaves(idx) == ["c", "d", "e"]

    def test_empty_index(self):
        idx = EdgeIndex()
        assert find_roots(idx) == []
        assert find_leaves(idx) == []


class TestSubtreeSize:
    def test_root(self):
        idx = _make_tree()
        assert subtree_size(idx, "root") == 5

    def test_subtree(self):
        idx = _make_tree()
        assert subtree_size(idx, "a") == 2

    def test_leaf(self):
        idx = _make_tree()
        assert subtree_size(idx, "c") == 0


class TestTemporalTraversal:
    def test_temporal_descent(self):
        idx = EdgeIndex()
        # c1 was child of root until March
        idx.add_hierarchy(HierarchyEdge(
            parent_id="root", child_id="c1",
            valid_from="2026-01-01T00:00:00+00:00",
            valid_to="2026-03-01T00:00:00+00:00",
        ))
        # c2 became child of root in March
        idx.add_hierarchy(HierarchyEdge(
            parent_id="root", child_id="c2",
            valid_from="2026-03-01T00:00:00+00:00",
        ))
        feb = macro_descent(idx, "root", at="2026-02-01T00:00:00+00:00")
        assert feb == ["c1"]
        apr = macro_descent(idx, "root", at="2026-04-01T00:00:00+00:00")
        assert apr == ["c2"]
