"""Tests for edge persistence in RegistryStore (edges.jsonl)."""

from __future__ import annotations

import json

import pytest

from ontologia.registry.store import RegistryStore, open_store
from ontologia.structure.edges import EdgeIndex, HierarchyEdge, RelationEdge


class TestEdgesPath:
    def test_edges_path_property(self, store: RegistryStore):
        assert store.edges_path == store.store_dir / "edges.jsonl"


class TestAddHierarchyEdge:
    def test_adds_to_index(self, store: RegistryStore):
        store.add_hierarchy_edge("parent_1", "child_1")
        ei = store.edge_index
        assert len(ei.all_hierarchy_edges()) == 1
        edge = ei.all_hierarchy_edges()[0]
        assert edge.parent_id == "parent_1"
        assert edge.child_id == "child_1"
        assert edge.valid_from  # non-empty timestamp

    def test_appends_to_file(self, store: RegistryStore):
        store.add_hierarchy_edge("p1", "c1")
        lines = store.edges_path.read_text().strip().splitlines()
        assert len(lines) == 1
        data = json.loads(lines[0])
        assert data["edge_type"] == "hierarchy"
        assert data["parent_id"] == "p1"
        assert data["child_id"] == "c1"

    def test_with_metadata(self, store: RegistryStore):
        store.add_hierarchy_edge("p1", "c1", metadata={"source": "bootstrap"})
        ei = store.edge_index
        edge = ei.all_hierarchy_edges()[0]
        assert edge.metadata == {"source": "bootstrap"}

    def test_multiple_edges(self, store: RegistryStore):
        store.add_hierarchy_edge("p1", "c1")
        store.add_hierarchy_edge("p1", "c2")
        store.add_hierarchy_edge("p2", "c3")
        ei = store.edge_index
        assert len(ei.all_hierarchy_edges()) == 3
        lines = store.edges_path.read_text().strip().splitlines()
        assert len(lines) == 3


class TestAddRelationEdge:
    def test_adds_to_index(self, store: RegistryStore):
        store.add_relation_edge("src_1", "tgt_1", "produces_for")
        ei = store.edge_index
        assert len(ei.all_relation_edges()) == 1
        edge = ei.all_relation_edges()[0]
        assert edge.source_id == "src_1"
        assert edge.target_id == "tgt_1"
        assert edge.relation_type == "produces_for"

    def test_appends_to_file(self, store: RegistryStore):
        store.add_relation_edge("s1", "t1", "depends_on")
        lines = store.edges_path.read_text().strip().splitlines()
        assert len(lines) == 1
        data = json.loads(lines[0])
        assert data["edge_type"] == "relation"
        assert data["source_id"] == "s1"
        assert data["target_id"] == "t1"
        assert data["relation_type"] == "depends_on"

    def test_with_metadata(self, store: RegistryStore):
        store.add_relation_edge("s1", "t1", "consumes_from", metadata={"artifact": "registry"})
        edge = store.edge_index.all_relation_edges()[0]
        assert edge.metadata == {"artifact": "registry"}


class TestLoadEdges:
    def test_load_populates_edge_index(self, store: RegistryStore):
        # Write edges, then reload
        store.add_hierarchy_edge("p1", "c1")
        store.add_relation_edge("s1", "t1", "produces_for")

        # Create a new store pointing to same dir and load
        store2 = RegistryStore(store_dir=store.store_dir)
        store2.load()
        ei = store2.edge_index
        assert len(ei.all_hierarchy_edges()) == 1
        assert len(ei.all_relation_edges()) == 1

    def test_empty_edges_file(self, store: RegistryStore):
        """Empty or missing edges.jsonl results in empty EdgeIndex."""
        ei = store.edge_index
        assert len(ei.all_hierarchy_edges()) == 0
        assert len(ei.all_relation_edges()) == 0

    def test_missing_edges_file(self, store_dir):
        """No edges.jsonl at all — graceful handling."""
        s = RegistryStore(store_dir=store_dir)
        s.load()
        assert len(s.edge_index.all_hierarchy_edges()) == 0

    def test_malformed_lines_skipped(self, store: RegistryStore):
        """Malformed JSONL lines are skipped without crashing."""
        store.edges_path.write_text(
            '{"edge_type":"hierarchy","parent_id":"p1","child_id":"c1","valid_from":"2020-01-01T00:00:00+00:00"}\n'
            'not-json\n'
            '{"edge_type":"relation","source_id":"s1","target_id":"t1","relation_type":"depends_on","valid_from":"2020-01-01T00:00:00+00:00"}\n'
        )
        store2 = RegistryStore(store_dir=store.store_dir)
        store2.load()
        ei = store2.edge_index
        assert len(ei.all_hierarchy_edges()) == 1
        assert len(ei.all_relation_edges()) == 1


class TestSaveEdges:
    def test_rewrites_full_file(self, store: RegistryStore):
        store.add_hierarchy_edge("p1", "c1")
        store.add_hierarchy_edge("p1", "c2")
        store.add_relation_edge("s1", "t1", "produces_for")

        # Rewrite from in-memory state
        store.save_edges()

        lines = store.edges_path.read_text().strip().splitlines()
        assert len(lines) == 3

        # Verify round-trip: reload and check
        store2 = RegistryStore(store_dir=store.store_dir)
        store2.load()
        assert len(store2.edge_index.all_hierarchy_edges()) == 2
        assert len(store2.edge_index.all_relation_edges()) == 1

    def test_empty_index_writes_empty_file(self, store: RegistryStore):
        store.save_edges()
        content = store.edges_path.read_text()
        assert content == ""


class TestEdgeIndexProperty:
    def test_returns_edge_index(self, store: RegistryStore):
        ei = store.edge_index
        assert isinstance(ei, EdgeIndex)

    def test_same_reference(self, store: RegistryStore):
        """Repeated access returns the same object."""
        assert store.edge_index is store.edge_index


class TestOpenStoreWithEdges:
    def test_open_store_loads_edges(self, store_dir):
        """open_store() loads edges alongside entities and names."""
        # Pre-populate edges file
        edge_line = json.dumps({
            "edge_type": "hierarchy",
            "parent_id": "p1",
            "child_id": "c1",
            "valid_from": "2020-01-01T00:00:00+00:00",
        })
        (store_dir / "edges.jsonl").write_text(edge_line + "\n")

        s = open_store(store_dir)
        assert len(s.edge_index.all_hierarchy_edges()) == 1


class TestBootstrapHierarchyEdges:
    """Test that bootstrap_from_registry creates hierarchy edges."""

    def _write_registry(self, path, organs):
        path.write_text(json.dumps({"version": "2.0", "organs": organs}))

    def test_creates_hierarchy_edges(self, store, tmp_path):
        from ontologia.bootstrap import bootstrap_from_registry

        reg = tmp_path / "registry.json"
        self._write_registry(reg, {
            "ORGAN-I": {
                "name": "Theoria",
                "repositories": [
                    {"name": "repo-a", "org": "ivviiviivvi"},
                    {"name": "repo-b", "org": "ivviiviivvi"},
                ],
            },
        })

        result = bootstrap_from_registry(store, reg)
        assert result.organs_created == 1
        assert result.repos_created == 2
        assert result.hierarchy_edges_created == 2

        ei = store.edge_index
        assert len(ei.all_hierarchy_edges()) == 2

    def test_hierarchy_edges_persisted(self, store, tmp_path):
        from ontologia.bootstrap import bootstrap_from_registry

        reg = tmp_path / "registry.json"
        self._write_registry(reg, {
            "META": {
                "name": "Meta",
                "repositories": [{"name": "engine", "org": "meta-organvm"}],
            },
        })

        bootstrap_from_registry(store, reg)

        # Reload and verify edges survived
        store2 = RegistryStore(store_dir=store.store_dir)
        store2.load()
        assert len(store2.edge_index.all_hierarchy_edges()) == 1

    def test_idempotent_edges(self, store, tmp_path):
        from ontologia.bootstrap import bootstrap_from_registry

        reg = tmp_path / "registry.json"
        self._write_registry(reg, {
            "ORGAN-I": {
                "name": "Theoria",
                "repositories": [{"name": "repo-a", "org": "ivviiviivvi"}],
            },
        })

        # Run twice
        r1 = bootstrap_from_registry(store, reg)
        assert r1.hierarchy_edges_created == 1

        r2 = bootstrap_from_registry(store, reg)
        # Entities are skipped, but edges should also be skipped
        assert r2.hierarchy_edges_created == 0

    def test_multiple_organs(self, store, tmp_path):
        from ontologia.bootstrap import bootstrap_from_registry

        reg = tmp_path / "registry.json"
        self._write_registry(reg, {
            "ORGAN-I": {
                "name": "Theoria",
                "repositories": [{"name": "repo-a", "org": "org-i"}],
            },
            "ORGAN-II": {
                "name": "Poiesis",
                "repositories": [
                    {"name": "repo-b", "org": "org-ii"},
                    {"name": "repo-c", "org": "org-ii"},
                ],
            },
        })

        result = bootstrap_from_registry(store, reg)
        assert result.hierarchy_edges_created == 3

        # Verify parent relationships
        ei = store.edge_index
        for edge in ei.all_hierarchy_edges():
            child = store.get_entity(edge.child_id)
            parent = store.get_entity(edge.parent_id)
            assert child is not None
            assert parent is not None
