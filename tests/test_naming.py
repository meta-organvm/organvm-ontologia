"""Tests for the temporal naming system."""

from ontologia.entity.naming import (
    NameIndex,
    NameRecord,
    _slugify,
    add_name,
    retire_name,
)


class TestSlugify:
    def test_basic(self):
        assert _slugify("Hello World") == "hello-world"

    def test_special_chars(self):
        assert _slugify("organvm-engine (v2)") == "organvm-engine-v2"

    def test_double_hyphen(self):
        assert _slugify("recursive-engine--generative-entity") == "recursive-engine--generative-entity"

    def test_leading_trailing(self):
        assert _slugify("  My Project  ") == "my-project"

    def test_multiple_spaces(self):
        assert _slugify("a   b   c") == "a-b-c"


class TestNameRecord:
    def test_is_active_current(self):
        record = NameRecord(
            entity_id="ent_repo_test",
            display_name="Test",
            slug="test",
            valid_from="2026-01-01T00:00:00+00:00",
        )
        assert record.is_active()

    def test_is_active_retired(self):
        record = NameRecord(
            entity_id="ent_repo_test",
            display_name="Test",
            slug="test",
            valid_from="2026-01-01T00:00:00+00:00",
            valid_to="2026-02-01T00:00:00+00:00",
        )
        assert not record.is_active()

    def test_is_active_future(self):
        record = NameRecord(
            entity_id="ent_repo_test",
            display_name="Test",
            slug="test",
            valid_from="2099-01-01T00:00:00+00:00",
        )
        assert not record.is_active()

    def test_is_active_at_timestamp(self):
        record = NameRecord(
            entity_id="ent_repo_test",
            display_name="Test",
            slug="test",
            valid_from="2026-01-01T00:00:00+00:00",
            valid_to="2026-06-01T00:00:00+00:00",
        )
        assert record.is_active(at="2026-03-15T00:00:00+00:00")
        assert not record.is_active(at="2026-07-01T00:00:00+00:00")

    def test_roundtrip(self):
        record = NameRecord(
            entity_id="ent_repo_test",
            display_name="My Repo",
            slug="my-repo",
            valid_from="2026-01-01T00:00:00+00:00",
            is_primary=True,
            source="bootstrap",
        )
        d = record.to_dict()
        restored = NameRecord.from_dict(d)
        assert restored.entity_id == record.entity_id
        assert restored.display_name == record.display_name
        assert restored.slug == record.slug
        assert restored.is_primary == record.is_primary

    def test_jsonl_format(self):
        record = NameRecord(
            entity_id="ent_repo_test",
            display_name="Test",
            slug="test",
            valid_from="2026-01-01T00:00:00+00:00",
        )
        line = record.to_jsonl()
        assert '"entity_id"' in line
        assert "\n" not in line


class TestNameIndex:
    def test_add_and_current(self):
        index = NameIndex()
        record = NameRecord(
            entity_id="e1",
            display_name="Alpha",
            slug="alpha",
            valid_from="2026-01-01T00:00:00+00:00",
            is_primary=True,
        )
        index.add(record)
        assert index.current_name("e1") == record

    def test_resolve_slug(self):
        index = NameIndex()
        record = NameRecord(
            entity_id="e1",
            display_name="My Project",
            slug="my-project",
            valid_from="2026-01-01T00:00:00+00:00",
        )
        index.add(record)
        assert index.resolve_slug("my-project") == ["e1"]

    def test_resolve_display_name_case_insensitive(self):
        index = NameIndex()
        record = NameRecord(
            entity_id="e1",
            display_name="MyProject",
            slug="myproject",
            valid_from="2026-01-01T00:00:00+00:00",
        )
        index.add(record)
        assert index.resolve_display_name("myproject") == ["e1"]

    def test_entity_count(self):
        index = NameIndex()
        for i in range(3):
            index.add(NameRecord(
                entity_id=f"e{i}",
                display_name=f"Name {i}",
                slug=f"name-{i}",
                valid_from="2026-01-01T00:00:00+00:00",
            ))
        assert index.entity_count() == 3

    def test_all_names(self):
        index = NameIndex()
        for name in ["Alpha", "Beta"]:
            index.add(NameRecord(
                entity_id="e1",
                display_name=name,
                slug=_slugify(name),
                valid_from="2026-01-01T00:00:00+00:00",
            ))
        assert len(index.all_names("e1")) == 2

    def test_active_names_filters_retired(self):
        index = NameIndex()
        index.add(NameRecord(
            entity_id="e1",
            display_name="Old",
            slug="old",
            valid_from="2026-01-01T00:00:00+00:00",
            valid_to="2026-02-01T00:00:00+00:00",
        ))
        index.add(NameRecord(
            entity_id="e1",
            display_name="New",
            slug="new",
            valid_from="2026-02-01T00:00:00+00:00",
        ))
        active = index.active_names("e1")
        assert len(active) == 1
        assert active[0].display_name == "New"


class TestAddName:
    def test_add_primary_retires_old(self):
        index = NameIndex()
        r1 = add_name(index, "e1", "Alpha", is_primary=True, valid_from="2026-01-01T00:00:00+00:00")
        r2 = add_name(index, "e1", "Beta", is_primary=True, valid_from="2026-02-01T00:00:00+00:00")

        # Old primary should be retired
        assert r1.valid_to is not None
        assert not r1.is_primary
        # New primary is active
        assert r2.is_active()
        assert r2.is_primary
        assert index.current_name("e1") == r2

    def test_add_alias_keeps_primary(self):
        index = NameIndex()
        primary = add_name(index, "e1", "Main Name", is_primary=True)
        alias = add_name(index, "e1", "Alias", is_primary=False)
        # Primary unchanged
        assert primary.is_primary
        assert primary.valid_to is None
        assert not alias.is_primary
        assert index.current_name("e1") == primary


class TestRetireName:
    def test_retire_existing(self):
        index = NameIndex()
        add_name(index, "e1", "ToRetire", is_primary=True)
        assert retire_name(index, "e1", "ToRetire")
        assert len(index.active_names("e1")) == 0

    def test_retire_nonexistent(self):
        index = NameIndex()
        assert not retire_name(index, "e1", "DoesNotExist")
