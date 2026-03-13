"""Tests for structure versioning."""

from pathlib import Path

from ontologia.structure.versioning import VersionLog, create_version


class TestCreateVersion:
    def test_basic(self):
        v = create_version("hierarchy_added", "Added repo to organ")
        assert v.version_id.startswith("sv_")
        assert v.change_type == "hierarchy_added"
        assert v.timestamp

    def test_with_details(self):
        v = create_version(
            "entity_relocated",
            "Moved to new organ",
            affected_entities=["e1", "e2"],
            details={"old_parent": "o1", "new_parent": "o2"},
        )
        assert v.affected_entities == ["e1", "e2"]
        assert v.details["old_parent"] == "o1"


class TestVersionLog:
    def test_append_and_count(self, tmp_path: Path):
        log = VersionLog(tmp_path / "versions.jsonl")
        assert log.count == 0
        v = create_version("test", "test reason")
        log.append(v)
        assert log.count == 1

    def test_persistence(self, tmp_path: Path):
        path = tmp_path / "versions.jsonl"
        log1 = VersionLog(path)
        log1.append(create_version("type_a", "reason a", affected_entities=["e1"]))
        log1.append(create_version("type_b", "reason b"))

        log2 = VersionLog(path)
        log2.load()
        assert log2.count == 2
        assert log2.all()[0].change_type == "type_a"

    def test_by_entity(self, tmp_path: Path):
        log = VersionLog(tmp_path / "v.jsonl")
        log.append(create_version("t", "r", affected_entities=["e1", "e2"]))
        log.append(create_version("t", "r", affected_entities=["e2", "e3"]))
        assert len(log.by_entity("e1")) == 1
        assert len(log.by_entity("e2")) == 2

    def test_by_type(self, tmp_path: Path):
        log = VersionLog(tmp_path / "v.jsonl")
        log.append(create_version("type_a", "r"))
        log.append(create_version("type_b", "r"))
        log.append(create_version("type_a", "r"))
        assert len(log.by_type("type_a")) == 2

    def test_recent(self, tmp_path: Path):
        log = VersionLog(tmp_path / "v.jsonl")
        for i in range(5):
            log.append(create_version("t", f"reason {i}"))
        recent = log.recent(n=3)
        assert len(recent) == 3
        assert recent[-1].change_reason == "reason 4"

    def test_empty_load(self, tmp_path: Path):
        log = VersionLog(tmp_path / "nonexistent.jsonl")
        log.load()
        assert log.count == 0
