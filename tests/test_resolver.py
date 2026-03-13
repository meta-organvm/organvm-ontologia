"""Tests for entity resolution — lookup by UID, name, slug, alias."""

from ontologia.entity.identity import EntityIdentity, EntityType, LifecycleStatus
from ontologia.entity.naming import NameIndex, NameRecord, _slugify, add_name
from ontologia.entity.resolver import EntityResolver


def _make_resolver(
    entities: list[tuple[str, EntityType, str]],
) -> EntityResolver:
    """Helper: create a resolver from (uid, type, display_name) tuples."""
    ent_dict: dict[str, EntityIdentity] = {}
    name_index = NameIndex()

    for uid, etype, dname in entities:
        ent_dict[uid] = EntityIdentity(
            uid=uid,
            entity_type=etype,
            created_at="2026-01-01T00:00:00+00:00",
        )
        add_name(name_index, uid, dname, is_primary=True, valid_from="2026-01-01T00:00:00+00:00")

    return EntityResolver(ent_dict, name_index)


class TestResolveByUid:
    def test_exact_match(self):
        r = _make_resolver([("ent_repo_A", EntityType.REPO, "engine")])
        result = r.resolve("ent_repo_A")
        assert result is not None
        assert result.identity.uid == "ent_repo_A"
        assert result.matched_by == "uid"

    def test_uid_not_found(self):
        r = _make_resolver([("ent_repo_A", EntityType.REPO, "engine")])
        assert r.resolve("ent_repo_NOPE") is None


class TestResolveByName:
    def test_primary_name(self):
        r = _make_resolver([("ent_repo_A", EntityType.REPO, "organvm-engine")])
        result = r.resolve("organvm-engine")
        assert result is not None
        assert result.matched_by == "primary_name"
        assert result.identity.uid == "ent_repo_A"

    def test_case_insensitive(self):
        r = _make_resolver([("ent_repo_A", EntityType.REPO, "OrganVM-Engine")])
        result = r.resolve("organvm-engine")
        assert result is not None

    def test_name_not_found(self):
        r = _make_resolver([("ent_repo_A", EntityType.REPO, "engine")])
        assert r.resolve("nonexistent") is None


class TestResolveBySlug:
    def test_slug_match(self):
        r = _make_resolver([("ent_repo_A", EntityType.REPO, "My Great Project")])
        result = r.resolve("my-great-project")
        assert result is not None
        assert result.matched_by == "slug"


class TestResolveByAlias:
    def test_historical_alias(self):
        ent_dict = {
            "ent_repo_A": EntityIdentity(
                uid="ent_repo_A",
                entity_type=EntityType.REPO,
                created_at="2026-01-01T00:00:00+00:00",
            ),
        }
        name_index = NameIndex()
        # Old name (retired)
        old = NameRecord(
            entity_id="ent_repo_A",
            display_name="old-name",
            slug="old-name",
            valid_from="2026-01-01T00:00:00+00:00",
            valid_to="2026-02-01T00:00:00+00:00",
            is_primary=False,
        )
        name_index.add(old)
        # Current name
        add_name(name_index, "ent_repo_A", "new-name", is_primary=True, valid_from="2026-02-01T00:00:00+00:00")

        resolver = EntityResolver(ent_dict, name_index)
        # "old-name" matches as a slug (slug index persists after retirement)
        # This is correct: slug match precedes alias search in resolver priority
        result = resolver.resolve("old-name")
        assert result is not None
        assert result.matched_by == "slug"
        assert result.current_name is not None
        assert result.current_name.display_name == "new-name"


class TestResolveWithTypeFilter:
    def test_type_filter_match(self):
        r = _make_resolver([
            ("ent_organ_A", EntityType.ORGAN, "Meta"),
            ("ent_repo_B", EntityType.REPO, "engine"),
        ])
        result = r.resolve("ent_organ_A", entity_type=EntityType.ORGAN)
        assert result is not None

    def test_type_filter_mismatch(self):
        r = _make_resolver([("ent_organ_A", EntityType.ORGAN, "Meta")])
        result = r.resolve("ent_organ_A", entity_type=EntityType.REPO)
        assert result is None


class TestResolveAll:
    def test_multiple_matches(self):
        ent_dict = {}
        name_index = NameIndex()
        for uid in ["ent_repo_A", "ent_repo_B"]:
            ent_dict[uid] = EntityIdentity(
                uid=uid,
                entity_type=EntityType.REPO,
                created_at="2026-01-01T00:00:00+00:00",
            )
            add_name(name_index, uid, "shared-name", is_primary=True, valid_from="2026-01-01T00:00:00+00:00")

        resolver = EntityResolver(ent_dict, name_index)
        results = resolver.resolve_all("shared-name")
        assert len(results) == 2


class TestListOperations:
    def test_list_by_type(self):
        r = _make_resolver([
            ("ent_organ_A", EntityType.ORGAN, "Meta"),
            ("ent_repo_B", EntityType.REPO, "engine"),
            ("ent_repo_C", EntityType.REPO, "dashboard"),
        ])
        repos = r.list_by_type(EntityType.REPO)
        assert len(repos) == 2

    def test_list_all(self):
        r = _make_resolver([
            ("ent_organ_A", EntityType.ORGAN, "Meta"),
            ("ent_repo_B", EntityType.REPO, "engine"),
        ])
        assert len(r.list_all()) == 2

    def test_entity_count(self):
        r = _make_resolver([
            ("ent_repo_A", EntityType.REPO, "a"),
            ("ent_repo_B", EntityType.REPO, "b"),
        ])
        assert r.entity_count == 2
