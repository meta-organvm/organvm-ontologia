"""Tests for high-level mutation operations."""

from pathlib import Path

from ontologia.entity.identity import EntityIdentity, EntityType, LifecycleStatus, create_entity
from ontologia.entity.lineage import LineageIndex, LineageType
from ontologia.entity.naming import NameIndex, add_name
from ontologia.events import bus
from ontologia.events.mutations import (
    MutationContext,
    deprecate,
    merge,
    relocate,
    rename,
    split,
)
from ontologia.structure.edges import EdgeIndex, HierarchyEdge
from ontologia.structure.versioning import VersionLog


def _make_context(tmp_path: Path) -> MutationContext:
    """Build a mutation context with two repos under one organ."""
    entities: dict[str, EntityIdentity] = {}
    name_index = NameIndex()
    edge_index = EdgeIndex()
    lineage_index = LineageIndex()
    version_log = VersionLog(tmp_path / "versions.jsonl")

    # Create entities
    organ = create_entity(EntityType.ORGAN, timestamp_ms=1000)
    entities[organ.uid] = organ
    add_name(name_index, organ.uid, "Meta", valid_from="2026-01-01T00:00:00+00:00")

    repo_a = create_entity(EntityType.REPO, timestamp_ms=2000)
    entities[repo_a.uid] = repo_a
    add_name(name_index, repo_a.uid, "repo-alpha", valid_from="2026-01-01T00:00:00+00:00")

    repo_b = create_entity(EntityType.REPO, timestamp_ms=3000)
    entities[repo_b.uid] = repo_b
    add_name(name_index, repo_b.uid, "repo-beta", valid_from="2026-01-01T00:00:00+00:00")

    # Hierarchy: organ → repo_a, organ → repo_b
    edge_index.add_hierarchy(HierarchyEdge(
        parent_id=organ.uid, child_id=repo_a.uid,
        valid_from="2026-01-01T00:00:00+00:00",
    ))
    edge_index.add_hierarchy(HierarchyEdge(
        parent_id=organ.uid, child_id=repo_b.uid,
        valid_from="2026-01-01T00:00:00+00:00",
    ))

    return MutationContext(
        entities=entities,
        name_index=name_index,
        edge_index=edge_index,
        lineage_index=lineage_index,
        version_log=version_log,
    )


def _entity_ids(ctx: MutationContext) -> list[str]:
    return sorted(ctx.entities.keys())


class TestRename:
    def test_rename_success(self, tmp_path: Path):
        ctx = _make_context(tmp_path)
        repo_uid = [uid for uid, e in ctx.entities.items() if e.entity_type == EntityType.REPO][0]
        assert rename(ctx, repo_uid, "new-name", source="test")
        current = ctx.name_index.current_name(repo_uid)
        assert current is not None
        assert current.display_name == "new-name"

    def test_rename_emits_version(self, tmp_path: Path):
        ctx = _make_context(tmp_path)
        repo_uid = [uid for uid, e in ctx.entities.items() if e.entity_type == EntityType.REPO][0]
        rename(ctx, repo_uid, "new-name")
        assert ctx.version_log.count == 1
        assert ctx.version_log.all()[0].change_type == "entity_renamed"

    def test_rename_nonexistent(self, tmp_path: Path):
        ctx = _make_context(tmp_path)
        assert not rename(ctx, "fake_uid", "new-name")

    def test_rename_preserves_history(self, tmp_path: Path):
        ctx = _make_context(tmp_path)
        repo_uid = [uid for uid, e in ctx.entities.items() if e.entity_type == EntityType.REPO][0]
        rename(ctx, repo_uid, "name-v2")
        rename(ctx, repo_uid, "name-v3")
        all_names = ctx.name_index.all_names(repo_uid)
        assert len(all_names) == 3  # original + v2 + v3


class TestRelocate:
    def test_relocate_success(self, tmp_path: Path):
        ctx = _make_context(tmp_path)
        # Create a second organ
        organ2 = create_entity(EntityType.ORGAN, timestamp_ms=4000)
        ctx.entities[organ2.uid] = organ2
        add_name(ctx.name_index, organ2.uid, "Theoria", valid_from="2026-01-01T00:00:00+00:00")

        repo_uid = [uid for uid, e in ctx.entities.items() if e.entity_type == EntityType.REPO][0]
        assert relocate(ctx, repo_uid, organ2.uid)
        # New parent should be organ2
        parent = ctx.edge_index.parent(repo_uid)
        assert parent is not None
        assert parent.parent_id == organ2.uid

    def test_relocate_emits_version(self, tmp_path: Path):
        ctx = _make_context(tmp_path)
        organ2 = create_entity(EntityType.ORGAN, timestamp_ms=4000)
        ctx.entities[organ2.uid] = organ2
        repo_uid = [uid for uid, e in ctx.entities.items() if e.entity_type == EntityType.REPO][0]
        relocate(ctx, repo_uid, organ2.uid)
        assert ctx.version_log.count == 1
        assert ctx.version_log.all()[0].change_type == "entity_relocated"

    def test_relocate_nonexistent_entity(self, tmp_path: Path):
        ctx = _make_context(tmp_path)
        organ_uid = [uid for uid, e in ctx.entities.items() if e.entity_type == EntityType.ORGAN][0]
        assert not relocate(ctx, "fake", organ_uid)

    def test_relocate_nonexistent_parent(self, tmp_path: Path):
        ctx = _make_context(tmp_path)
        repo_uid = [uid for uid, e in ctx.entities.items() if e.entity_type == EntityType.REPO][0]
        assert not relocate(ctx, repo_uid, "fake_parent")


class TestMerge:
    def test_merge_two_repos(self, tmp_path: Path):
        ctx = _make_context(tmp_path)
        repo_uids = [uid for uid, e in ctx.entities.items() if e.entity_type == EntityType.REPO]
        assert len(repo_uids) == 2

        result = merge(ctx, repo_uids, "merged-repo", source="test")
        assert result is not None
        assert result.entity_type == EntityType.REPO

        # Sources should be MERGED
        for uid in repo_uids:
            assert ctx.entities[uid].lifecycle_status == LifecycleStatus.MERGED

    def test_merge_creates_lineage(self, tmp_path: Path):
        ctx = _make_context(tmp_path)
        repo_uids = [uid for uid, e in ctx.entities.items() if e.entity_type == EntityType.REPO]
        result = merge(ctx, repo_uids, "merged-repo")

        # Successor has DERIVED_FROM lineage from both sources
        preds = ctx.lineage_index.predecessors(result.uid)
        assert len(preds) == 2

        # Sources have MERGED_INTO lineage
        for uid in repo_uids:
            records = [r for r in ctx.lineage_index.full_lineage(uid)
                       if r.lineage_type == LineageType.MERGED_INTO]
            assert len(records) >= 1

    def test_merge_nonexistent(self, tmp_path: Path):
        ctx = _make_context(tmp_path)
        assert merge(ctx, ["fake1", "fake2"], "merged") is None

    def test_merge_emits_version(self, tmp_path: Path):
        ctx = _make_context(tmp_path)
        repo_uids = [uid for uid, e in ctx.entities.items() if e.entity_type == EntityType.REPO]
        merge(ctx, repo_uids, "merged-repo")
        versions = ctx.version_log.by_type("entity_merged")
        assert len(versions) == 1


class TestSplit:
    def test_split_into_two(self, tmp_path: Path):
        ctx = _make_context(tmp_path)
        repo_uid = [uid for uid, e in ctx.entities.items() if e.entity_type == EntityType.REPO][0]
        results = split(ctx, repo_uid, ["part-a", "part-b"])
        assert len(results) == 2
        assert ctx.entities[repo_uid].lifecycle_status == LifecycleStatus.SPLIT

    def test_split_creates_lineage(self, tmp_path: Path):
        ctx = _make_context(tmp_path)
        repo_uid = [uid for uid, e in ctx.entities.items() if e.entity_type == EntityType.REPO][0]
        results = split(ctx, repo_uid, ["part-a", "part-b"])
        for new_ent in results:
            preds = ctx.lineage_index.predecessors(new_ent.uid)
            assert len(preds) == 1
            assert preds[0].lineage_type == LineageType.SPLIT_FROM

    def test_split_nonexistent(self, tmp_path: Path):
        ctx = _make_context(tmp_path)
        assert split(ctx, "fake", ["a", "b"]) == []

    def test_split_emits_version(self, tmp_path: Path):
        ctx = _make_context(tmp_path)
        repo_uid = [uid for uid, e in ctx.entities.items() if e.entity_type == EntityType.REPO][0]
        split(ctx, repo_uid, ["part-a", "part-b"])
        assert ctx.version_log.count == 1


class TestDeprecate:
    def test_deprecate_success(self, tmp_path: Path):
        ctx = _make_context(tmp_path)
        repo_uid = [uid for uid, e in ctx.entities.items() if e.entity_type == EntityType.REPO][0]
        assert deprecate(ctx, repo_uid, reason="no longer needed")
        assert ctx.entities[repo_uid].lifecycle_status == LifecycleStatus.DEPRECATED

    def test_deprecate_with_successor(self, tmp_path: Path):
        ctx = _make_context(tmp_path)
        repo_uids = [uid for uid, e in ctx.entities.items() if e.entity_type == EntityType.REPO]
        deprecate(ctx, repo_uids[0], successor_id=repo_uids[1])
        # Check lineage: successor supersedes the deprecated entity
        records = ctx.lineage_index.full_lineage(repo_uids[1])
        assert any(r.lineage_type == LineageType.SUPERSEDES for r in records)

    def test_deprecate_nonexistent(self, tmp_path: Path):
        ctx = _make_context(tmp_path)
        assert not deprecate(ctx, "fake")

    def test_deprecate_emits_version(self, tmp_path: Path):
        ctx = _make_context(tmp_path)
        repo_uid = [uid for uid, e in ctx.entities.items() if e.entity_type == EntityType.REPO][0]
        deprecate(ctx, repo_uid)
        assert ctx.version_log.count == 1
